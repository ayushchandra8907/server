#ifndef PARQUET_CREATE_INCLUDED
#define PARQUET_CREATE_INCLUDED

#define MYSQL_SERVER 1

#include "parquet_metadata.h"

#include <cstdio>
#include <string>

struct TABLE;

namespace parquet
{

enum class CreateTableErrorKind {
  kNone,
  kInvalidCreateOption,
  kUnsupported,
  kInternal
};

struct CreateTableResult {
  CreateTableErrorKind error_kind = CreateTableErrorKind::kNone;
  TableMetadata metadata;

  bool ok() const { return error_kind == CreateTableErrorKind::kNone; }
};

inline void RemoveCreateLocalArtifacts(const TableMetadata &metadata)
{
  if (!metadata.local_paths.parquet_file_path.empty()) {
    std::remove(metadata.local_paths.parquet_file_path.c_str());
  }
  const std::string metadata_file_path =
      metadata.metadata_file_path.empty()
          ? ResolveMetadataFilePath(metadata.local_paths.table_path.c_str())
          : metadata.metadata_file_path;
  if (!metadata_file_path.empty()) {
    std::remove(metadata_file_path.c_str());
  }
}

bool CreateParquetTable(const char *table_path, TABLE *table,
                        const char *catalog_options,
                        const char *connection_options,
                        CreateTableResult *result, std::string *error);

} // namespace parquet

#endif
