#define MYSQL_SERVER 1

#include "my_global.h"

#include "parquet_duckdb.h"

#include "parquet_cross_engine_scan.h"
#include "parquet_schema.h"
#include "parquet_shared.h"

#include "field.h"
#include "table.h"

#include <cstdio>
#include <ctime>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_map>
#include <atomic>

namespace parquet
{

namespace
{

std::mutex g_duckdb_mutex;
duckdb::DuckDB *g_duckdb = nullptr;
std::unordered_map<std::thread::id, std::unique_ptr<duckdb::Connection>>
    g_duckdb_handler_connections;
std::unordered_map<std::thread::id, std::unique_ptr<duckdb::Connection>>
    g_duckdb_pushdown_connections;

bool BootstrapExtensionsLocked(std::string *error)
{
  if (g_duckdb == nullptr) {
    if (error != nullptr) {
      *error = "DuckDB runtime is not initialized";
    }
    return false;
  }

  try {
    duckdb::Connection bootstrap_connection(*g_duckdb);
    return parquet_run_duckdb_query(&bootstrap_connection,
                                    "runtime/install-parquet",
                                    "INSTALL parquet;", error) &&
           parquet_run_duckdb_query(&bootstrap_connection,
                                    "runtime/install-httpfs",
                                    "INSTALL httpfs;", error);
  } catch (const std::exception &ex) {
    if (error != nullptr) {
      *error = ex.what();
    }
    return false;
  }
}

bool PrepareConnectionLocked(duckdb::Connection *connection,
                             std::string *error)
{
  return parquet_run_duckdb_query(connection, "runtime/load-parquet",
                                  "LOAD parquet;", error) &&
         parquet_run_duckdb_query(connection, "runtime/load-httpfs",
                                  "LOAD httpfs;", error);
}

duckdb::Connection *CachedConnectionLocked(
    std::unordered_map<std::thread::id, std::unique_ptr<duckdb::Connection>>
        *connection_cache,
    const char *connection_label, std::string *error)
{
  if (connection_cache == nullptr) {
    if (error != nullptr) {
      *error = "DuckDB connection cache must not be null";
    }
    return nullptr;
  }

  if (g_duckdb == nullptr) {
    if (error != nullptr) {
      *error = "DuckDB runtime is not initialized";
    }
    return nullptr;
  }

  const std::thread::id thread_id = std::this_thread::get_id();
  auto it = connection_cache->find(thread_id);
  if (it != connection_cache->end()) {
    return it->second.get();
  }

  try {
    std::unique_ptr<duckdb::Connection> connection(
        new duckdb::Connection(*g_duckdb));
    if (!PrepareConnectionLocked(connection.get(), error)) {
      return nullptr;
    }

    duckdb::Connection *raw = connection.get();
    connection_cache->emplace(thread_id, std::move(connection));
    parquet_log_info(std::string("DuckDB ") + connection_label +
                     " connection initialized for current worker thread");
    return raw;
  } catch (const std::exception &ex) {
    if (error != nullptr) {
      *error = ex.what();
    }
    return nullptr;
  }
}

bool DropDuckDBTableLocked(const std::string &table_name)
{
  if (table_name.empty()) {
    return true;
  }

  try {
    std::string connection_error;
    duckdb::Connection *connection =
        parquet_handler_connection_locked(&connection_error);
    if (connection == nullptr) {
      std::cerr << "DuckDB connection error: " << connection_error
                << std::endl;
      return false;
    }

    const std::string drop_query =
        "DROP TABLE IF EXISTS " + QuoteIdentifier(table_name);
    parquet_log_info("DuckDB query [txn/drop-buffer] " +
                     parquet_log_preview(drop_query));
    auto result = connection->Query(drop_query);
    const bool ok = result && !result->HasError();
    if (!ok) {
      std::cerr << "DuckDB DROP TABLE error: "
                << (result ? result->GetError() : "null result") << std::endl;
    }
    return ok;
  } catch (const std::exception &ex) {
    std::cerr << "DuckDB DROP TABLE exception: " << ex.what() << std::endl;
    return false;
  }
}

std::string LocalStageListSql(const ParquetTableTxnState &table_state)
{
  std::vector<std::string> paths;
  paths.reserve(table_state.local_stage_files.size());
  for (const auto &stage : table_state.local_stage_files) {
    paths.push_back(stage.local_path);
  }
  return BuildDuckDBStringList(paths);
}

uint64_t ReadLocalFileSize(const std::string &path)
{
  std::ifstream stream(path, std::ios::binary | std::ios::ate);
  if (!stream) {
    return 0;
  }
  return static_cast<uint64_t>(stream.tellg());
}

} // namespace

std::mutex &parquet_duckdb_mutex()
{
  return g_duckdb_mutex;
}

bool parquet_run_duckdb_query(duckdb::Connection *connection,
                              const std::string &query_label,
                              const std::string &query,
                              std::string *error)
{
  if (connection == nullptr) {
    if (error != nullptr) {
      *error = "DuckDB connection must not be null";
    }
    return false;
  }

  parquet_log_info("DuckDB query [" + query_label + "] " +
                   parquet_log_preview(query));
  auto result = connection->Query(query);
  if (!result || result->HasError()) {
    if (error != nullptr) {
      *error = result ? result->GetError() : "DuckDB returned a null result";
    }
    return false;
  }
  return true;
}

bool parquet_init_shared_duckdb_runtime(std::string *error)
{
  std::lock_guard<std::mutex> lock(g_duckdb_mutex);
  if (g_duckdb != nullptr) {
    return true;
  }

  try {
    g_duckdb = new duckdb::DuckDB(nullptr);
  } catch (const std::exception &ex) {
    if (error != nullptr) {
      *error = ex.what();
    }
    return false;
  }

  if (!BootstrapExtensionsLocked(error)) {
    delete g_duckdb;
    g_duckdb = nullptr;
    return false;
  }

  myparquet::register_cross_engine_scan(*g_duckdb->instance);
  return true;
}

void parquet_deinit_shared_duckdb_runtime()
{
  std::lock_guard<std::mutex> lock(g_duckdb_mutex);
  g_duckdb_handler_connections.clear();
  g_duckdb_pushdown_connections.clear();
  delete g_duckdb;
  g_duckdb = nullptr;
}

duckdb::Connection *parquet_handler_connection_locked(std::string *error)
{
  return CachedConnectionLocked(&g_duckdb_handler_connections, "handler",
                                error);
}

duckdb::Connection *parquet_pushdown_connection_locked(std::string *error)
{
  return CachedConnectionLocked(&g_duckdb_pushdown_connections, "pushdown",
                                error);
}

uint64_t NextParquetFlushId()
{
  static std::atomic<uint64_t> flush_counter{0};
  return static_cast<uint64_t>(time(nullptr)) * 1000ULL +
         (flush_counter.fetch_add(1, std::memory_order_relaxed) % 1000ULL);
}

std::string StatementBufferName(unsigned long long thread_id,
                                unsigned long long query_id,
                                const std::string &table_path)
{
  return "buf_stmt_" + std::to_string(thread_id) + "_" +
         std::to_string(query_id) + "_" +
         std::to_string(static_cast<unsigned long long>(
             std::hash<std::string>{}(table_path)));
}

bool EnsureStatementAppenderLocked(ParquetTableTxnState *table_state,
                                   TABLE *table,
                                   duckdb::Connection *connection,
                                   std::string *error)
{
  if (table_state == nullptr || table == nullptr || connection == nullptr) {
    if (error != nullptr) {
      *error = "invalid state while ensuring statement appender";
    }
    return false;
  }

  if (table_state->statement_appender) {
    return true;
  }

  std::string create_sql;
  if (!BuildDuckDBCreateTableSql(table_state->statement_buffer_name, table,
                                 &create_sql, error)) {
    return false;
  }
  if (!parquet_run_duckdb_query(connection, "write/ensure-buffer", create_sql,
                                error)) {
    return false;
  }

  try {
    table_state->statement_appender =
        std::make_unique<duckdb::Appender>(*connection,
                                           table_state->statement_buffer_name);
    parquet_log_info("DuckDB appender ready table='" +
                     table_state->table_name + "' buffer='" +
                     table_state->statement_buffer_name + "'");
    return true;
  } catch (const std::exception &ex) {
    if (error != nullptr) {
      *error = ex.what();
    }
    return false;
  }
}

bool CloseStatementAppenderLocked(ParquetTableTxnState *table_state,
                                  std::string *error)
{
  if (table_state == nullptr || !table_state->statement_appender) {
    return true;
  }

  try {
    table_state->statement_appender->Flush();
    table_state->statement_appender->Close();
    table_state->statement_appender.reset();
    return true;
  } catch (const std::exception &ex) {
    table_state->statement_appender.reset();
    if (error != nullptr) {
      *error = ex.what();
    }
    return false;
  }
}

void ResetStatementBufferLocked(ParquetTableTxnState *table_state)
{
  if (table_state == nullptr) {
    return;
  }

  std::string close_error;
  if (!CloseStatementAppenderLocked(table_state, &close_error)) {
    std::cerr << "DuckDB appender close error: " << close_error << std::endl;
  }

  if (!table_state->statement_buffer_name.empty()) {
    parquet_log_info("Parquet dropping statement buffer table='" +
                     table_state->table_name + "' buffer='" +
                     table_state->statement_buffer_name + "'");
    DropDuckDBTableLocked(table_state->statement_buffer_name);
  }
  table_state->statement_buffer_name.clear();
  table_state->statement_row_count = 0;
}

void ResetStatementBuffer(ParquetTableTxnState *table_state)
{
  std::lock_guard<std::mutex> lock(g_duckdb_mutex);
  ResetStatementBufferLocked(table_state);
}

void RemoveLocalFiles(ParquetTableTxnState *table_state)
{
  if (table_state == nullptr) {
    return;
  }

  for (const auto &stage : table_state->local_stage_files) {
    if (!stage.local_path.empty()) {
      parquet_log_info("Parquet removing local staged file table='" +
                       table_state->table_name + "' path='" + stage.local_path +
                       "'");
      std::remove(stage.local_path.c_str());
    }
  }
  table_state->local_stage_files.clear();

  for (const auto &staged_file : table_state->staged_files) {
    if (!staged_file.local_parquet_path.empty()) {
      parquet_log_info("Parquet removing local data file table='" +
                       table_state->table_name + "' path='" +
                       staged_file.local_parquet_path + "'");
      std::remove(staged_file.local_parquet_path.c_str());
    }
  }
  table_state->staged_files.clear();

  for (const auto &path : table_state->local_cleanup_paths) {
    if (!path.empty()) {
      std::remove(path.c_str());
    }
  }
  table_state->local_cleanup_paths.clear();
}

bool AppendRecordToStatementBufferLocked(ParquetTableTxnState *table_state,
                                         TABLE *table,
                                         const uchar *record,
                                         duckdb::Connection *connection,
                                         std::string *error)
{
  if (!EnsureStatementAppenderLocked(table_state, table, connection, error)) {
    return false;
  }

  try {
    duckdb::Appender *appender = table_state->statement_appender.get();
    appender->BeginRow();
    for (Field **field = table->field; *field; ++field) {
      if (!AppendMariaDBFieldToDuckDBAppender(*field, record, appender, error)) {
        ResetStatementBufferLocked(table_state);
        return false;
      }
    }
    appender->EndRow();
    table_state->statement_row_count++;
    return true;
  } catch (const std::exception &ex) {
    if (error != nullptr) {
      *error = ex.what();
    }
    ResetStatementBufferLocked(table_state);
    return false;
  }
}

bool StageStatementBufferToLocal(ParquetTableTxnState *table_state,
                                 const std::string &canonical_parquet_path,
                                 std::string *error)
{
  if (table_state == nullptr || table_state->statement_row_count == 0 ||
      table_state->statement_buffer_name.empty()) {
    return true;
  }

  const auto flush_id = NextParquetFlushId();
  const std::string local_stage_path =
      BuildLocalStagePath(canonical_parquet_path, flush_id);

  try {
    std::lock_guard<std::mutex> lock(g_duckdb_mutex);
    duckdb::Connection *connection = parquet_handler_connection_locked(error);
    if (connection == nullptr) {
      return false;
    }
    if (!CloseStatementAppenderLocked(table_state, error)) {
      return false;
    }
    std::remove(local_stage_path.c_str());
    const std::string copy_query = BuildDuckDBCopyToParquetSql(
        QuoteIdentifier(table_state->statement_buffer_name), local_stage_path);
    if (!parquet_run_duckdb_query(connection, "txn/stage-local/copy",
                                  copy_query, error)) {
      return false;
    }
  } catch (const std::exception &ex) {
    if (error != nullptr) {
      *error = ex.what();
    }
    return false;
  }

  table_state->local_stage_files.push_back(
      {local_stage_path, table_state->statement_row_count});
  parquet_log_info("Parquet staged local file table='" +
                   table_state->table_name + "' path='" + local_stage_path +
                   "' rows=" + std::to_string(table_state->statement_row_count));
  ResetStatementBuffer(table_state);
  return true;
}

bool MaterializeLocalDataFile(ParquetTableTxnState *table_state,
                              const TableMetadata &metadata,
                              ParquetStagedFile *staged_file,
                              std::string *error)
{
  if (table_state == nullptr || staged_file == nullptr) {
    if (error != nullptr) {
      *error = "invalid state while materializing Parquet data file";
    }
    return false;
  }

  if (table_state->local_stage_files.empty()) {
    return true;
  }

  const auto flush_id = NextParquetFlushId();
  const std::string local_data_path =
      BuildLocalDataPath(metadata.local_paths.parquet_file_path, flush_id);
  ObjectLocation object_location;
  if (!ResolveTableObjectLocation(
          metadata, "data/part_" + std::to_string(flush_id) + ".parquet",
          &object_location, error)) {
    return false;
  }
  const std::string target_object_path =
      BuildS3Uri(object_location.bucket, object_location.key);

  uint64_t total_rows = 0;
  for (const auto &stage : table_state->local_stage_files) {
    total_rows += stage.row_count;
  }

  uint64_t file_size = 0;
  try {
    std::lock_guard<std::mutex> lock(g_duckdb_mutex);
    duckdb::Connection *connection = parquet_handler_connection_locked(error);
    if (connection == nullptr) {
      return false;
    }

    std::remove(local_data_path.c_str());
    const std::string source_query =
        "SELECT * FROM read_parquet(" + LocalStageListSql(*table_state) + ")";
    const std::string copy_query =
        BuildDuckDBCopyToParquetSql("(" + source_query + ")", local_data_path);
    if (!parquet_run_duckdb_query(connection, "commit/materialize-data",
                                  copy_query, error)) {
      return false;
    }

  } catch (const std::exception &ex) {
    if (error != nullptr) {
      *error = ex.what();
    }
    return false;
  }

  file_size = ReadLocalFileSize(local_data_path);

  *staged_file = {table_state->table_path,
                  table_state->table_name,
                  local_data_path,
                  target_object_path,
                  total_rows,
                  file_size,
                  flush_id};
  table_state->staged_files.push_back(*staged_file);
  parquet_log_info("Parquet materialized local data file table='" +
                   table_state->table_name + "' path='" + local_data_path +
                   "' target='" + target_object_path + "' rows=" +
                   std::to_string(total_rows) + " bytes=" +
                   std::to_string(file_size));
  return true;
}

bool SeedEmptyLocalParquet(TABLE *table,
                           const std::string &buffer_table_name,
                           const std::string &parquet_file_path,
                           std::string *error)
{
  try {
    std::lock_guard<std::mutex> lock(g_duckdb_mutex);
    duckdb::Connection *connection = parquet_handler_connection_locked(error);
    if (connection == nullptr) {
      return false;
    }

    parquet_run_duckdb_query(connection, "create/cleanup-old-buffer",
                             "DROP TABLE IF EXISTS " + QuoteIdentifier(buffer_table_name), nullptr);

    std::string create_sql;
    if (!BuildDuckDBCreateTableSql(buffer_table_name, table, &create_sql,
                                   error)) {
      return false;
    }
    if (!parquet_run_duckdb_query(connection, "create/buffer-table", create_sql,
                                  error)) {
      return false;
    }

    const std::string copy_query = BuildDuckDBCopyToParquetSql(
        QuoteIdentifier(buffer_table_name), parquet_file_path);
    bool copy_ok = parquet_run_duckdb_query(connection, "create/seed-parquet",
                                            copy_query, error);

    parquet_run_duckdb_query(connection, "create/cleanup-buffer",
                             "DROP TABLE IF EXISTS " + QuoteIdentifier(buffer_table_name), nullptr);
    return copy_ok;
  } catch (const std::exception &ex) {
    if (error != nullptr) {
      *error = ex.what();
    }
    return false;
  }
}

} // namespace parquet
