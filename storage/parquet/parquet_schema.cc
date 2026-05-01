#define MYSQL_SERVER 1

#include "my_global.h"

#include "parquet_schema.h"

#include "field.h"
#include "my_time.h"
#include "sql_class.h"
#include "sql_time.h"
#include "table.h"
#include "parquet_shared.h"

#include <json.hpp>

#include <stdexcept>

namespace parquet
{

namespace
{

using json = nlohmann::json;

class FieldRecordScope
{
public:
  FieldRecordScope(Field *field_arg, const uchar *record)
      : field_(field_arg), saved_ptr_(field_arg->ptr)
  {
    field_->ptr = const_cast<uchar *>(field_->ptr_in_record(record));
  }

  ~FieldRecordScope() { field_->ptr = saved_ptr_; }

private:
  Field *field_;
  uchar *saved_ptr_;
};

void SetError(std::string *error, const std::string &message)
{
  if (error != nullptr) {
    *error = message;
  }
}

bool MariaDBFieldToIcebergType(Field *field, json *iceberg_type,
                               std::string *error)
{
  if (field == nullptr || iceberg_type == nullptr) {
    SetError(error, "invalid field while mapping Iceberg schema");
    return false;
  }

  switch (field->type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_YEAR:
      *iceberg_type = "int";
      return true;
    case MYSQL_TYPE_LONGLONG:
      *iceberg_type = "long";
      return true;
    case MYSQL_TYPE_FLOAT:
      *iceberg_type = "float";
      return true;
    case MYSQL_TYPE_DOUBLE:
      *iceberg_type = "double";
      return true;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL: {
      uint32 precision = field->field_length;
      const uint32 scale = field->decimals();
      if (scale > 0 && precision > 0) {
        precision--; // Display length includes the decimal point.
      }
      if (!field->is_unsigned() && precision > 0) {
        precision--; // Display length may include the sign.
      }
      if (precision < scale + 1) {
        precision = scale + 1;
      }
      if (precision > 38) {
        precision = 38;
      }
      *iceberg_type = {{"type", "decimal"},
                       {"precision", precision},
                       {"scale", scale}};
      return true;
    }
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_SET:
      *iceberg_type = "string";
      return true;
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
      *iceberg_type = (field->charset() == &my_charset_bin) ? json("binary")
                                                            : json("string");
      return true;
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE:
      *iceberg_type = "date";
      return true;
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
      *iceberg_type = "time";
      return true;
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
      *iceberg_type = "timestamp";
      return true;
    case MYSQL_TYPE_BIT:
      *iceberg_type = "boolean";
      return true;
    default:
      SetError(error, "unsupported MariaDB column type for Parquet/Iceberg");
      return false;
  }
}

void StoreTemporalValue(Field *field, MYSQL_TIME *ltime)
{
  field->store_time(ltime);
}

} // namespace

std::string QuoteIdentifier(const std::string &identifier)
{
  std::string quoted = "\"";
  for (char ch : identifier) {
    if (ch == '"') {
      quoted += "\"\"";
    } else {
      quoted += ch;
    }
  }
  quoted += "\"";
  return quoted;
}

std::string BuildDuckDBStringList(const std::vector<std::string> &values)
{
  std::string list = "[";
  for (size_t i = 0; i < values.size(); ++i) {
    if (i != 0) {
      list += ", ";
    }
    list += quote_string_literal(values[i]);
  }
  list += "]";
  return list;
}

bool MariaDBFieldToDuckDBType(Field *field, std::string *duckdb_type,
                              std::string *error)
{
  if (field == nullptr || duckdb_type == nullptr) {
    SetError(error, "invalid field while mapping DuckDB schema");
    return false;
  }

  switch (field->type()) {
    case MYSQL_TYPE_TINY:
      *duckdb_type = "TINYINT";
      return true;
    case MYSQL_TYPE_SHORT:
      *duckdb_type = "SMALLINT";
      return true;
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
      *duckdb_type = "INTEGER";
      return true;
    case MYSQL_TYPE_LONGLONG:
      *duckdb_type = "BIGINT";
      return true;
    case MYSQL_TYPE_FLOAT:
      *duckdb_type = "FLOAT";
      return true;
    case MYSQL_TYPE_DOUBLE:
      *duckdb_type = "DOUBLE";
      return true;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      *duckdb_type = "DECIMAL";
      return true;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_SET:
      *duckdb_type = "VARCHAR";
      return true;
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_BLOB:
      *duckdb_type = (field->charset() == &my_charset_bin) ? "BLOB" : "VARCHAR";
      return true;
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE:
      *duckdb_type = "DATE";
      return true;
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
      *duckdb_type = "TIME";
      return true;
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
      *duckdb_type = "TIMESTAMP";
      return true;
    case MYSQL_TYPE_YEAR:
      *duckdb_type = "SMALLINT";
      return true;
    case MYSQL_TYPE_BIT:
      *duckdb_type = "BOOLEAN";
      return true;
    default:
      SetError(error, "unsupported MariaDB column type for Parquet/DuckDB");
      return false;
  }
}

bool BuildDuckDBCreateTableSql(const std::string &table_name, TABLE *table,
                               std::string *sql, std::string *error)
{
  if (table == nullptr || table->s == nullptr || sql == nullptr) {
    SetError(error, "invalid table while building DuckDB CREATE TABLE");
    return false;
  }

  std::string query = "CREATE TABLE IF NOT EXISTS " + QuoteIdentifier(table_name) + " (";
  bool first = true;
  for (Field **field = table->s->field; *field; ++field) {
    std::string duck_type;
    if (!MariaDBFieldToDuckDBType(*field, &duck_type, error)) {
      return false;
    }
    if (!first) {
      query += ", ";
    }
    first = false;
    query += QuoteIdentifier((*field)->field_name.str) + " " + duck_type;
  }
  query += ")";
  *sql = std::move(query);
  return true;
}

bool BuildDuckDBEmptyViewSql(const std::string &view_name, TABLE *table,
                             std::string *sql, std::string *error)
{
  if (table == nullptr || sql == nullptr) {
    SetError(error, "invalid table while building DuckDB empty view");
    return false;
  }

  std::string query =
      "CREATE OR REPLACE TEMP VIEW " + QuoteIdentifier(view_name) + " AS SELECT ";
  bool first = true;
  for (Field **field = table->field; *field; ++field) {
    std::string duck_type;
    if (!MariaDBFieldToDuckDBType(*field, &duck_type, error)) {
      return false;
    }
    if (!first) {
      query += ", ";
    }
    first = false;
    query += "CAST(NULL AS " + duck_type + ") AS " +
             QuoteIdentifier((*field)->field_name.str);
  }
  query += " WHERE FALSE";
  *sql = std::move(query);
  return true;
}

std::string BuildDuckDBReadParquetViewSql(
    const std::string &view_name, const std::vector<std::string> &paths)
{
  return "CREATE OR REPLACE TEMP VIEW " + QuoteIdentifier(view_name) +
         " AS " + BuildDuckDBReadParquetSql(paths);
}

std::string BuildDuckDBReadParquetSql(const std::vector<std::string> &paths)
{
  return "SELECT * FROM read_parquet(" + BuildDuckDBStringList(paths) + ")";
}

std::string BuildDuckDBCopyToParquetSql(const std::string &table_or_query,
                                        const std::string &path)
{
  return "COPY " + table_or_query + " TO " + quote_string_literal(path) +
         " (FORMAT PARQUET)";
}

bool BuildIcebergSchemaJson(TABLE *table, int schema_id,
                            std::string *schema_json, std::string *error)
{
  if (table == nullptr || table->s == nullptr || schema_json == nullptr) {
    SetError(error, "invalid table while building Iceberg schema");
    return false;
  }

  json fields = json::array();
  int field_id = 1;
  for (Field **field = table->s->field; *field; ++field) {
    json iceberg_type;
    if (!MariaDBFieldToIcebergType(*field, &iceberg_type, error)) {
      return false;
    }
    fields.push_back({{"id", field_id++},
                      {"name", std::string((*field)->field_name.str)},
                      {"required", false},
                      {"type", iceberg_type}});
  }

  *schema_json =
      json({{"type", "struct"}, {"schema-id", schema_id}, {"fields", fields}})
          .dump();
  return true;
}

duckdb::Value MariaDBFieldToDuckDBValue(Field *field, const uchar *record)
{
  if (field->is_null_in_record(record)) {
    return duckdb::Value();
  }

  FieldRecordScope field_scope(field, record);
  switch (field->real_type()) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_YEAR:
      if (field->is_unsigned()) {
        return duckdb::Value::UBIGINT(field->val_uint());
      }
      return duckdb::Value::BIGINT(field->val_int());
    case MYSQL_TYPE_FLOAT:
      return duckdb::Value::FLOAT(static_cast<float>(field->val_real()));
    case MYSQL_TYPE_DOUBLE:
      return duckdb::Value::DOUBLE(field->val_real());
    case MYSQL_TYPE_BIT:
      return duckdb::Value::BOOLEAN(field->val_int() != 0);
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_SET:
    case MYSQL_TYPE_ENUM:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING: {
      String value;
      String *result = field->val_str(&value);
      if (result == nullptr) {
        return duckdb::Value();
      }
      return duckdb::Value(std::string(result->ptr(), result->length()));
    }
    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB: {
      String value;
      String *result = field->val_str(&value);
      if (result == nullptr) {
        return duckdb::Value();
      }
      if (field->charset() == &my_charset_bin) {
        return duckdb::Value::BLOB(
            reinterpret_cast<duckdb::const_data_ptr_t>(result->ptr()),
            result->length());
      }
      return duckdb::Value(std::string(result->ptr(), result->length()));
    }
    default:
      throw std::runtime_error("unsupported MariaDB column type in Parquet appender");
  }
}

bool AppendMariaDBFieldToDuckDBAppender(Field *field, const uchar *record,
                                        duckdb::Appender *appender,
                                        std::string *error)
{
  if (field == nullptr || appender == nullptr) {
    SetError(error, "invalid state while appending MariaDB row");
    return false;
  }

  try {
    appender->Append(MariaDBFieldToDuckDBValue(field, record));
    return true;
  } catch (const std::exception &ex) {
    SetError(error, ex.what());
    return false;
  }
}

bool StoreDuckDBValueInMariaDBField(Field *field, duckdb::Value &value,
                                    THD *thd, std::string *error)
{
  (void) thd;
  if (field == nullptr) {
    SetError(error, "field must not be null");
    return false;
  }

  try {
    if (value.IsNull()) {
      field->set_default();
      if (field->real_maybe_null()) {
        field->set_null();
      }
      return true;
    }

    field->set_notnull();
    switch (field->type()) {
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_GEOMETRY:
      case MYSQL_TYPE_BIT: {
        auto str = value.GetValueUnsafe<duckdb::string>();
        field->store(str.c_str(), str.size(), &my_charset_bin);
        return true;
      }
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VAR_STRING: {
        auto str = value.GetValue<duckdb::string>();
        field->store(str.c_str(), str.size(),
                     field->has_charset() ? field->charset() : &my_charset_bin);
        return true;
      }
      case MYSQL_TYPE_NULL:
      case MYSQL_TYPE_DECIMAL:
      case MYSQL_TYPE_ENUM:
      case MYSQL_TYPE_SET:
      case MYSQL_TYPE_NEWDECIMAL: {
        auto str = value.GetValue<duckdb::string>();
        field->store(str.c_str(), str.size(), system_charset_info);
        return true;
      }
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_YEAR:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONG:
        field->store(value.GetValue<int64_t>(), field->is_unsigned());
        return true;
      case MYSQL_TYPE_LONGLONG:
        if (field->is_unsigned()) {
          field->store(value.GetValue<uint64_t>(), true);
        } else {
          field->store(value.GetValue<int64_t>(), false);
        }
        return true;
      case MYSQL_TYPE_FLOAT:
        field->store(value.GetValue<float>());
        return true;
      case MYSQL_TYPE_DOUBLE:
        field->store(value.GetValue<double>());
        return true;
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_NEWDATE:
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_DATETIME2:
      case MYSQL_TYPE_TIMESTAMP:
      case MYSQL_TYPE_TIMESTAMP2: {
        auto str = value.GetValue<duckdb::string>();
        MYSQL_TIME tm;
        MYSQL_TIME_STATUS status;
        my_time_status_init(&status);
        str_to_datetime_or_date(str.c_str(), str.size(), &tm, 0, &status);
        StoreTemporalValue(field, &tm);
        return true;
      }
      case MYSQL_TYPE_TIME:
      case MYSQL_TYPE_TIME2: {
        auto str = value.GetValue<duckdb::string>();
        MYSQL_TIME tm;
        MYSQL_TIME_STATUS status;
        my_time_status_init(&status);
        str_to_DDhhmmssff(str.c_str(), str.size(), &tm, TIME_MAX_HOUR, &status);
        StoreTemporalValue(field, &tm);
        return true;
      }
      default: {
        auto str = value.GetValue<duckdb::string>();
        field->store(str.c_str(), str.size(), system_charset_info);
        return true;
      }
    }
  } catch (const std::exception &ex) {
    SetError(error, ex.what());
    return false;
  }
}

} // namespace parquet
