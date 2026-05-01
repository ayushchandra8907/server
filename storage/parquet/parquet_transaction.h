#ifndef PARQUET_TRANSACTION_INCLUDED
#define PARQUET_TRANSACTION_INCLUDED

#include <cstdint>
#include <string>

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

std::string BuildLocalStagePath(const std::string &canonical_parquet_path,
                                uint64_t flush_id);

} // namespace parquet

#endif
