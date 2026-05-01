#ifndef PARQUET_DUCKDB_INCLUDED
#define PARQUET_DUCKDB_INCLUDED

#define MYSQL_SERVER 1

#include "duckdb.hpp"
#include "parquet_metadata.h"
#include "parquet_transaction.h"

#include <cstdint>
#include <mutex>
#include <string>

struct TABLE;
typedef unsigned char uchar;

namespace parquet
{

std::mutex &parquet_duckdb_mutex();
bool parquet_run_duckdb_query(duckdb::Connection *con,
                              const std::string &query_label,
                              const std::string &query,
                              std::string *error);
bool parquet_init_shared_duckdb_runtime(std::string *error);
void parquet_deinit_shared_duckdb_runtime();
duckdb::Connection *parquet_handler_connection_locked(std::string *error);
duckdb::Connection *parquet_pushdown_connection_locked(std::string *error);

uint64_t NextParquetFlushId();
std::string StatementBufferName(unsigned long long thread_id,
                                unsigned long long query_id,
                                const std::string &table_path);

bool EnsureStatementAppenderLocked(ParquetTableTxnState *table_state,
                                   TABLE *table,
                                   duckdb::Connection *connection,
                                   std::string *error);
bool CloseStatementAppenderLocked(ParquetTableTxnState *table_state,
                                  std::string *error);
void ResetStatementBufferLocked(ParquetTableTxnState *table_state);
void ResetStatementBuffer(ParquetTableTxnState *table_state);
void RemoveLocalFiles(ParquetTableTxnState *table_state);

bool AppendRecordToStatementBufferLocked(ParquetTableTxnState *table_state,
                                         TABLE *table,
                                         const uchar *record,
                                         duckdb::Connection *connection,
                                         std::string *error);
bool StageStatementBufferToLocal(ParquetTableTxnState *table_state,
                                 const std::string &canonical_parquet_path,
                                 std::string *error);
bool MaterializeLocalDataFile(ParquetTableTxnState *table_state,
                              const TableMetadata &metadata,
                              ParquetStagedFile *staged_file,
                              std::string *error);
bool SeedEmptyLocalParquet(TABLE *table,
                           const std::string &buffer_table_name,
                           const std::string &parquet_file_path,
                           std::string *error);

} // namespace parquet

#endif
