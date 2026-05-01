#ifndef PARQUET_SCHEMA_INCLUDED
#define PARQUET_SCHEMA_INCLUDED

#define MYSQL_SERVER 1

#include "duckdb.hpp"

#include <string>
#include <vector>

class Field;
class THD;
struct TABLE;
typedef unsigned char uchar;

namespace parquet
{

std::string QuoteIdentifier(const std::string &identifier);
std::string BuildDuckDBStringList(const std::vector<std::string> &values);

bool MariaDBFieldToDuckDBType(Field *field, std::string *duckdb_type,
                              std::string *error);
bool BuildDuckDBCreateTableSql(const std::string &table_name, TABLE *table,
                               std::string *sql, std::string *error);
bool BuildDuckDBEmptyViewSql(const std::string &view_name, TABLE *table,
                             std::string *sql, std::string *error);
std::string BuildDuckDBReadParquetViewSql(
    const std::string &view_name, const std::vector<std::string> &paths);
std::string BuildDuckDBReadParquetSql(const std::vector<std::string> &paths);
std::string BuildDuckDBCopyToParquetSql(const std::string &table_or_query,
                                        const std::string &path);

bool BuildIcebergSchemaJson(TABLE *table, int schema_id,
                            std::string *schema_json, std::string *error);

duckdb::Value MariaDBFieldToDuckDBValue(Field *field, const uchar *record);
bool AppendMariaDBFieldToDuckDBAppender(Field *field, const uchar *record,
                                        duckdb::Appender *appender,
                                        std::string *error);
bool StoreDuckDBValueInMariaDBField(Field *field, duckdb::Value &value,
                                    THD *thd, std::string *error);

} // namespace parquet

#endif
