#ifndef PARQUET_WRITE_BUFFER_INCLUDED
#define PARQUET_WRITE_BUFFER_INCLUDED

#include <cstdint>
#include <string>
#include <vector>

namespace parquet
{

struct ParquetBufferedFile {
  std::string local_path;
  std::string target_s3_path;
  uint64_t    row_count      = 0;
  uint64_t    file_size_bytes = 0;
};

void ParquetWriteBufferInit();
void ParquetWriteBufferDeinit();

void ParquetWriteBufferAppend(const std::string &table_path,
                              const std::string &table_name,
                              const ParquetBufferedFile &file);

bool ParquetWriteBufferTake(const std::string &table_path,
                            std::string *out_table_name,
                            std::vector<ParquetBufferedFile> *out_files);

bool ParquetWriteBufferShouldFlush(const std::string &table_path,
                                   uint64_t max_rows);

std::vector<std::string> ParquetWriteBufferStaleTables(uint64_t interval_ms);

std::vector<std::string> ParquetWriteBufferGetLocalPaths(
    const std::string &table_path);

} // namespace parquet

#endif
