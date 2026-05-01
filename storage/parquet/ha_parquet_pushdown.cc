#define MYSQL_SERVER 1

#include "ha_parquet_pushdown.h"

#include "parquet_cross_engine_scan.h"
#include "parquet_duckdb.h"
#include "parquet_metadata.h"
#include "parquet_schema.h"
#include "parquet_shared.h"

#include "field.h"
#include "log.h"
#include "sql_select.h"

#include <algorithm>
#include <cstring>
#include <string>
#include <unordered_set>

namespace {

std::string describe_tables(const std::vector<TABLE_LIST *> &tables)
{
  std::string description;
  for (size_t i = 0; i < tables.size(); ++i) {
    if (i != 0)
      description += ", ";

    TABLE_LIST *tbl = tables[i];
    description += tbl && tbl->table_name.str ? tbl->table_name.str : "<unknown>";
    if (tbl && tbl->alias.str && std::strcmp(tbl->alias.str, tbl->table_name.str) != 0) {
      description += " AS ";
      description += tbl->alias.str;
    }
  }
  return description;
}

bool duckdb_configs_compatible(const parquet::ObjectStoreConfig &left,
                               const parquet::ObjectStoreConfig &right)
{
  return left.endpoint == right.endpoint && left.region == right.region &&
         left.url_style == right.url_style &&
         left.auth_mode == right.auth_mode &&
         left.credentials.access_key_id == right.credentials.access_key_id &&
         left.credentials.secret_access_key ==
             right.credentials.secret_access_key &&
         left.credentials.session_token == right.credentials.session_token;
}

bool can_pushdown_to_parquet(SELECT_LEX *sel_lex,
                             std::vector<TABLE_LIST *> &parquet_tables,
                             std::vector<TABLE_LIST *> &external_tables)
{
  std::unordered_set<std::string> seen_parquet_names;
  std::unordered_set<std::string> seen_external_names;
  bool has_parquet_table = false;

  for (TABLE_LIST *tbl = sel_lex->get_table_list(); tbl; tbl = tbl->next_global) {
    if (tbl->derived || !tbl->table || !tbl->table->file)
      return false;

    const std::string table_name(tbl->table_name.str);
    if (tbl->table->file->ht == parquet_hton) {
      has_parquet_table = true;
      if (seen_parquet_names.insert(table_name).second)
        parquet_tables.push_back(tbl);
    } else if (seen_external_names.insert(table_name).second) {
      external_tables.push_back(tbl);
    }
  }

  return has_parquet_table;
}

void register_external_table_names(TABLE_LIST *tbl)
{
  if (!tbl || !tbl->table)
    return;

  std::unordered_set<std::string> registered_names;
  auto register_name = [&](const char *name) {
    if (!name || !name[0])
      return;

    const std::string key(name);
    if (registered_names.insert(key).second)
      myparquet::register_external_table(key, tbl->table);
  };

  register_name(tbl->table_name.str);
  register_name(tbl->alias.str);

  if (tbl->db.str && tbl->db.str[0]) {
    const std::string qualified_name =
        std::string(tbl->db.str) + "." + tbl->table_name.str;
    if (registered_names.insert(qualified_name).second)
      myparquet::register_external_table(qualified_name, tbl->table);

    if (tbl->alias.str && std::strcmp(tbl->alias.str, tbl->table_name.str) != 0) {
      const std::string qualified_alias =
          std::string(tbl->db.str) + "." + tbl->alias.str;
      if (registered_names.insert(qualified_alias).second)
        myparquet::register_external_table(qualified_alias, tbl->table);
    }
  }
}

} // namespace

ha_parquet_select_handler::ha_parquet_select_handler(THD *thd_arg,
                                                     SELECT_LEX *sel_lex,
                                                     SELECT_LEX_UNIT *sel_unit)
    : select_handler(thd_arg, parquet_hton, sel_lex, sel_unit),
      duckdb_con(nullptr),
      current_row_index(0),
      has_cross_engine(false),
      query_string(thd_arg->charset())
{
  query_string.length(0);
  query_string.append(thd_arg->query(), thd_arg->query_length());
}

ha_parquet_select_handler::~ha_parquet_select_handler() = default;

void ha_parquet_select_handler::set_parquet_tables(std::vector<TABLE_LIST *> &&tables)
{
  parquet_tables = std::move(tables);
}

void ha_parquet_select_handler::set_cross_engine(std::vector<TABLE_LIST *> &&tables)
{
  has_cross_engine = !tables.empty();
  external_tables = std::move(tables);
}

int ha_parquet_select_handler::init_scan()
{
  DBUG_ENTER("ha_parquet_select_handler::init_scan");

  {
    std::lock_guard<std::mutex> lock(parquet::parquet_duckdb_mutex());
    std::string error_message;
    duckdb_con = parquet::parquet_pushdown_connection_locked(&error_message);
    if (duckdb_con == nullptr) {
      my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), error_message.c_str());
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }
  }
  parquet_log_info("DuckDB select handler initialized using shared runtime");
  query_result.reset();
  current_chunk.reset();
  current_row_index = 0;
  temp_view_names.clear();

  auto cleanup_external_registry = [&]() {
    if (has_cross_engine)
      myparquet::clear_external_tables();
  };

  auto cleanup_temp_views = [&]() {
    if (duckdb_con == nullptr || temp_view_names.empty())
      return;

    for (const auto &view_name : temp_view_names) {
      const std::string drop_view_sql =
          "DROP VIEW IF EXISTS " + parquet::QuoteIdentifier(view_name);
      parquet_log_info("DuckDB query [select/drop-temp-view] " +
                       parquet_log_preview(drop_view_sql));
      auto drop_result = duckdb_con->Query(drop_view_sql);
      if (!drop_result || drop_result->HasError()) {
        sql_print_warning(
            "Parquet: failed to drop cached temp view '%s': %s",
            view_name.c_str(),
            drop_result ? drop_result->GetError().c_str() : "null result");
      }
    }
    temp_view_names.clear();
  };

  parquet::ObjectStoreConfig scan_object_store_config;
  bool have_scan_object_store_config = false;

  for (TABLE_LIST *tbl : parquet_tables) {
    parquet::TableMetadata metadata;
    std::string error_message;
    if (!parquet::ResolveRuntimeTableMetadata(
            tbl->table->s->normalized_path.str, &metadata, &error_message) ||
        !parquet::ValidateCatalogConfig(metadata, &error_message) ||
        !parquet::ValidateObjectStoreConfig(metadata, true, &error_message)) {
      cleanup_temp_views();
      cleanup_external_registry();
      my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), error_message.c_str());
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }

    if (!have_scan_object_store_config) {
      scan_object_store_config = metadata.object_store_config;
      have_scan_object_store_config = true;
    } else if (!duckdb_configs_compatible(scan_object_store_config,
                                          metadata.object_store_config)) {
      cleanup_temp_views();
      cleanup_external_registry();
      my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0),
               "Parquet pushdown currently requires matching object-store "
               "credentials and endpoint settings across Parquet tables");
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }

    if (!configure_duckdb_s3(duckdb_con, metadata.object_store_config,
                             &error_message)) {
      cleanup_temp_views();
      cleanup_external_registry();
      my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), error_message.c_str());
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }

    std::vector<std::string> scan_paths;
    if (!resolve_parquet_scan_paths(&metadata, &scan_paths, &error_message)) {
      cleanup_temp_views();
      cleanup_external_registry();
      my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), error_message.c_str());
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }

    std::string create_view_sql;
    if (scan_paths.empty()) {
      if (!parquet::BuildDuckDBEmptyViewSql(
              tbl->table_name.str, tbl->table, &create_view_sql,
              &error_message)) {
        cleanup_temp_views();
        cleanup_external_registry();
        my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), error_message.c_str());
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
      }
    } else {
      create_view_sql =
          parquet::BuildDuckDBReadParquetViewSql(tbl->table_name.str,
                                                 scan_paths);
    }
    if (create_view_sql.empty()) {
      cleanup_temp_views();
      cleanup_external_registry();
      my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0),
               "Parquet pushdown could not map the table schema to DuckDB");
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }

    parquet_log_info("DuckDB query [select/create-parquet-view] " +
                     parquet_log_preview(create_view_sql));
    auto create_view_result = duckdb_con->Query(create_view_sql);
    if (!create_view_result || create_view_result->HasError()) {
      cleanup_temp_views();
      cleanup_external_registry();
      const std::string error_message =
          create_view_result ? create_view_result->GetError()
                             : "DuckDB returned a null result while creating a Parquet view";
      my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), error_message.c_str());
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }
    temp_view_names.push_back(tbl->table_name.str);
  }

  if (have_scan_object_store_config) {
    std::string error_message;
    if (!configure_duckdb_s3(duckdb_con, scan_object_store_config,
                             &error_message)) {
      cleanup_temp_views();
      cleanup_external_registry();
      my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), error_message.c_str());
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }
  }

  if (has_cross_engine) {
    for (TABLE_LIST *tbl : external_tables)
      register_external_table_names(tbl);

    const std::string parquet_desc = describe_tables(parquet_tables);
    const std::string external_desc = describe_tables(external_tables);
    sql_print_information(
        "Parquet: cross-engine pushdown init parquet_tables=[%s] external_tables=[%s]",
        parquet_desc.c_str(), external_desc.c_str());
  }

  std::string sql(query_string.ptr(), query_string.length());
  parquet_log_info("DuckDB query [select/execute] " +
                   parquet_log_preview(sql));
  query_result = duckdb_con->Query(sql);

  if (!query_result || query_result->HasError()) {
    cleanup_temp_views();
    cleanup_external_registry();
    const std::string error_message =
        query_result ? query_result->GetError()
                     : "DuckDB returned a null result for Parquet pushdown";
    my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), error_message.c_str());
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  DBUG_RETURN(0);
}

int ha_parquet_select_handler::next_row()
{
  DBUG_ENTER("ha_parquet_select_handler::next_row");

  if (!query_result)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);

  if (!current_chunk || current_row_index >= current_chunk->size()) {
    current_chunk.reset();
    current_chunk = query_result->Fetch();

    if (!current_chunk || current_chunk->size() == 0)
      DBUG_RETURN(HA_ERR_END_OF_FILE);

    current_row_index = 0;
  }

  size_t col_count = current_chunk->ColumnCount();
  size_t field_count = 0;
  for (Field **f = table->field; *f; ++f)
    field_count++;

  size_t ncols = (col_count < field_count) ? col_count : field_count;
  for (size_t col_idx = 0; col_idx < ncols; ++col_idx) {
    duckdb::Value value = current_chunk->GetValue(col_idx, current_row_index);
    Field *field = table->field[col_idx];
    std::string error_message;
    if (!parquet::StoreDuckDBValueInMariaDBField(field, value, thd,
                                                 &error_message)) {
      my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), error_message.c_str());
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }
  }

  current_row_index++;
  DBUG_RETURN(0);
}

int ha_parquet_select_handler::end_scan()
{
  DBUG_ENTER("ha_parquet_select_handler::end_scan");

  if (has_cross_engine)
    myparquet::clear_external_tables();

  current_chunk.reset();
  query_result.reset();
  if (duckdb_con != nullptr && !temp_view_names.empty()) {
    for (const auto &view_name : temp_view_names) {
      const std::string drop_view_sql =
          "DROP VIEW IF EXISTS " + parquet::QuoteIdentifier(view_name);
      parquet_log_info("DuckDB query [select/drop-temp-view] " +
                       parquet_log_preview(drop_view_sql));
      auto drop_result = duckdb_con->Query(drop_view_sql);
      if (!drop_result || drop_result->HasError()) {
        sql_print_warning(
            "Parquet: failed to drop cached temp view '%s': %s",
            view_name.c_str(),
            drop_result ? drop_result->GetError().c_str() : "null result");
      }
    }
  }
  temp_view_names.clear();
  duckdb_con = nullptr;
  parquet_log_info("DuckDB select handler deinitialized");
  current_row_index = 0;
  has_cross_engine = false;
  external_tables.clear();

  if (table) {
    free_tmp_table(thd, table);
    table = 0;
  }

  DBUG_RETURN(0);
}

bool is_duckdb_pushdown_supported(THD *thd, SELECT_LEX *sel_lex,
                                  const std::vector<TABLE_LIST *> &parquet_tables,
                                  const std::vector<TABLE_LIST *> &external_tables)
{
  if (!thd || !thd->query())
    return false;
    
  std::string sql(thd->query(), thd->query_length());

  // Strip a leading EXPLAIN keyword so we don't double-wrap when MariaDB
  // calls create_select during its own EXPLAIN processing.
  {
    size_t pos = 0;
    while (pos < sql.size() && std::isspace((unsigned char)sql[pos]))
      ++pos;
    if (pos + 7 <= sql.size()) {
      std::string prefix = sql.substr(pos, 7);
      std::transform(prefix.begin(), prefix.end(), prefix.begin(), ::toupper);
      if (prefix == "EXPLAIN") {
        pos += 7;
        while (pos < sql.size() && std::isspace((unsigned char)sql[pos]))
          ++pos;
        sql = sql.substr(pos);
      }
    }
  }
  
  std::lock_guard<std::mutex> lock(parquet::parquet_duckdb_mutex());
  std::string error;
  duckdb::Connection *con = parquet::parquet_pushdown_connection_locked(&error);
  if (!con) return false;

  std::vector<std::string> temp_views;
  
  for (auto *tbl : parquet_tables) {
    if (!tbl || !tbl->table) continue;
    std::string create_sql;
    if (parquet::BuildDuckDBEmptyViewSql(tbl->table_name.str, tbl->table, &create_sql, &error)) {
      if (!con->Query(create_sql)->HasError()) {
        temp_views.push_back(tbl->table_name.str);
      }
    }
  }

  bool has_cross_engine = !external_tables.empty();
  if (has_cross_engine) {
    for (TABLE_LIST *tbl : external_tables)
      register_external_table_names(tbl);
  }

  // Passively check for failure within DuckDB by running EXPLAIN.
  // This verifies syntax, column binding, and function translation.
  auto result = con->Query("EXPLAIN " + sql);
  bool success = result && !result->HasError();

  if (has_cross_engine) {
    myparquet::clear_external_tables();
  }

  for (const auto &view_name : temp_views) {
    con->Query("DROP VIEW IF EXISTS " + parquet::QuoteIdentifier(view_name));
  }

  return success;
}

select_handler *create_duckdb_select_handler(THD *thd,
                                             SELECT_LEX *sel_lex,
                                             SELECT_LEX_UNIT *sel_unit)
{
  if (!thd || !sel_lex)
    return nullptr;

  if (thd->lex->sql_command != SQLCOM_SELECT)
    return nullptr;

  if (thd->stmt_arena && thd->stmt_arena->is_stmt_prepare())
    return nullptr;

  if (sel_lex->uncacheable & UNCACHEABLE_SIDEEFFECT)
    return nullptr;

  if (sel_lex->master_unit() && sel_lex->master_unit()->is_unit_op())
    return nullptr;

  std::vector<TABLE_LIST *> parquet_tables;
  std::vector<TABLE_LIST *> external_tables;
  if (!can_pushdown_to_parquet(sel_lex, parquet_tables, external_tables))
    return nullptr;

  if (!is_duckdb_pushdown_supported(thd, sel_lex, parquet_tables, external_tables))
    return nullptr;

  auto *handler = new ha_parquet_select_handler(thd, sel_lex, sel_unit);
  handler->set_parquet_tables(std::move(parquet_tables));
  if (!external_tables.empty()) {
    handler->set_cross_engine(std::move(external_tables));
    sql_print_information("Parquet: selected cross-engine pushdown handler");
  }
  return handler;
}
