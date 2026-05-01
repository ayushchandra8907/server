#define MYSQL_SERVER 1

#include "parquet_transaction.h"

#include <string>

namespace parquet
{

std::string BuildLocalStagePath(const std::string &canonical_parquet_path,
                                uint64_t flush_id)
{
  const std::string suffix = ".parquet";

  if (canonical_parquet_path.size() >= suffix.size() &&
      canonical_parquet_path.compare(canonical_parquet_path.size() -
                                         suffix.size(),
                                     suffix.size(), suffix) == 0) {
    return canonical_parquet_path.substr(
               0, canonical_parquet_path.size() - suffix.size()) +
           ".stage_" + std::to_string(flush_id) + suffix;
  }

  return canonical_parquet_path + ".stage_" + std::to_string(flush_id) +
         suffix;
}

} // namespace parquet
