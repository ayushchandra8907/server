#ifndef PARQUET_TRANSACTION_INCLUDED
#define PARQUET_TRANSACTION_INCLUDED

#include "duckdb.hpp"
#include "parquet_object_store.h"

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

class THD;
struct handlerton;
struct TABLE_SHARE;

namespace parquet
{

struct ParquetStagedFile {
  std::string table_path;
  std::string table_name;
  std::string local_parquet_path;
  std::string target_object_path;
  uint64_t record_count = 0;
  uint64_t file_size_bytes = 0;
  uint64_t flush_id = 0;
};

struct ParquetLocalStageFile {
  std::string local_path;
  uint64_t row_count = 0;
};

struct ParquetTableTxnState {
  std::string table_path;
  std::string table_name;
  std::string statement_buffer_name;
  uint64_t statement_row_count = 0;
  std::unique_ptr<duckdb::Appender> statement_appender;
  std::vector<ParquetLocalStageFile> local_stage_files;
  std::vector<ParquetStagedFile> staged_files;
  std::vector<ObjectLocation> uploaded_objects;
  std::vector<std::string> local_cleanup_paths;
};

struct ParquetTxnState {
  std::map<std::string, ParquetTableTxnState> tables;
  bool registered_with_server = false;
  bool has_error = false;
};

ParquetTxnState *GetOrCreateTxnState(THD *thd, handlerton *hton);
ParquetTxnState *GetTxnState(THD *thd, handlerton *hton);
void ClearTxnState(THD *thd, handlerton *hton);
ParquetTableTxnState *GetOrCreateTableTxnState(ParquetTxnState *txn_state,
                                               TABLE_SHARE *share);

bool IsStagedFileComplete(const ParquetStagedFile &staged_file);
bool ValidateTxnState(const ParquetTxnState &txn_state, std::string *error);
bool TxnHasStagedFiles(const ParquetTxnState &txn_state);

std::string BuildLocalStagePath(const std::string &canonical_parquet_path,
                                uint64_t flush_id);
std::string BuildLocalDataPath(const std::string &canonical_parquet_path,
                               uint64_t flush_id);
std::string BuildLocalManifestPath(const std::string &canonical_parquet_path,
                                   const std::string &label,
                                   uint64_t flush_id);
std::string BuildPrototypeObjectPath(const std::string &table_name,
                                     uint64_t flush_id);

} // namespace parquet

#endif
