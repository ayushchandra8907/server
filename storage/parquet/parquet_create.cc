#define MYSQL_SERVER 1

#include "my_global.h"

#include "parquet_create.h"

#include "parquet_catalog.h"
#include "parquet_duckdb.h"
#include "parquet_object_store.h"
#include "parquet_schema.h"
#include "parquet_shared.h"

#include "table.h"

#include <string>

namespace parquet
{

namespace
{

std::string CatalogStatusMessage(const std::string &operation,
                                 const CatalogStatus &status)
{
  std::string message = operation;
  if (!status.message.empty()) {
    message += ": " + status.message;
  }
  if (status.http_status != 0) {
    message += " (HTTP " + std::to_string(status.http_status) + ")";
  }
  return message;
}

bool SetFailure(CreateTableResult *result, CreateTableErrorKind kind,
                const TableMetadata &metadata, const std::string &message,
                std::string *error)
{
  if (result != nullptr) {
    result->error_kind = kind;
    result->metadata = metadata;
  }
  if (error != nullptr) {
    *error = message;
  }
  return false;
}

bool BootstrapCatalog(ParquetCatalogClient *client, std::string *error)
{
  auto status = client->BootstrapConfig();
  if (!status.ok()) {
    if (error != nullptr) {
      *error = CatalogStatusMessage(
          "failed to bootstrap Iceberg REST catalog", status);
    }
    return false;
  }
  return true;
}

class CreateCleanupGuard
{
public:
  CreateCleanupGuard(TableMetadata *metadata, ParquetCatalogClient *catalog)
      : metadata_(metadata), catalog_(catalog)
  {
  }

  void mark_catalog_created() { catalog_created_ = true; }
  void mark_local_artifacts_owned() { local_artifacts_owned_ = true; }
  void dismiss() { dismissed_ = true; }

  ~CreateCleanupGuard()
  {
    if (dismissed_ || metadata_ == nullptr) {
      return;
    }

    if (local_artifacts_owned_) {
      RemoveCreateLocalArtifacts(*metadata_);
    }

    if (catalog_created_ && catalog_ != nullptr) {
      auto status = catalog_->DropTable(metadata_->catalog_table_ident);
      if (!status.ok() && status.code != CatalogStatusCode::kNotFound) {
        parquet_log_warning(CatalogStatusMessage(
            "failed to clean up newly-created Iceberg REST catalog table",
            status));
      }
    }
  }

private:
  TableMetadata *metadata_ = nullptr;
  ParquetCatalogClient *catalog_ = nullptr;
  bool catalog_created_ = false;
  bool local_artifacts_owned_ = false;
  bool dismissed_ = false;
};

} // namespace

bool CreateParquetTable(const char *table_path, TABLE *table,
                        const char *catalog_options,
                        const char *connection_options,
                        CreateTableResult *result, std::string *error)
{
  if (result != nullptr) {
    *result = CreateTableResult();
  }

  TableMetadata metadata;
  if (!ResolveCreateTableMetadata(table_path, catalog_options,
                                  connection_options, &metadata, error)) {
    return SetFailure(result, CreateTableErrorKind::kInvalidCreateOption,
                      metadata,
                      error != nullptr ? *error : std::string(), error);
  }

  std::string validation_error;
  if (!ValidateCatalogConfig(metadata, &validation_error) ||
      !ValidateObjectStoreConfig(metadata, true, &validation_error)) {
    return SetFailure(result, CreateTableErrorKind::kUnsupported, metadata,
                      validation_error, error);
  }

  std::string schema_json;
  if (!BuildIcebergSchemaJson(table, 0, &schema_json, &validation_error)) {
    return SetFailure(result, CreateTableErrorKind::kUnsupported, metadata,
                      validation_error, error);
  }

  ParquetCatalogClient catalog_client(metadata.catalog_config);
  std::string catalog_error;
  if (!BootstrapCatalog(&catalog_client, &catalog_error)) {
    return SetFailure(result, CreateTableErrorKind::kInternal, metadata,
                      catalog_error, error);
  }

  CreateCleanupGuard cleanup(&metadata, &catalog_client);

  auto namespace_status =
      catalog_client.EnsureNamespace(metadata.catalog_table_ident.namespace_ident);
  if (!namespace_status.ok() &&
      namespace_status.code != CatalogStatusCode::kConflict) {
    return SetFailure(result, CreateTableErrorKind::kInternal, metadata,
                      CatalogStatusMessage(
                          "failed to ensure Iceberg namespace",
                          namespace_status),
                      error);
  }

  CatalogCreateTableRequest request;
  request.ident = metadata.catalog_table_ident;
  request.schema_json = schema_json;

  CatalogLoadTableResult create_result;
  auto create_status = catalog_client.CreateTable(request, &create_result);
  if (create_status.ok()) {
    cleanup.mark_catalog_created();
  } else if (create_status.code == CatalogStatusCode::kConflict) {
    create_status = catalog_client.LoadTable(
        metadata.catalog_table_ident, &create_result,
        metadata.access_delegation);
  }
  if (!create_status.ok()) {
    return SetFailure(result, CreateTableErrorKind::kInternal, metadata,
                      CatalogStatusMessage(
                          "failed to create Iceberg REST catalog table",
                          create_status),
                      error);
  }

  if (!ApplyCatalogLoadResult(&metadata, create_result, &validation_error)) {
    return SetFailure(result, CreateTableErrorKind::kInternal, metadata,
                      validation_error, error);
  }

  const std::string buffer_table_name = "buf_" + metadata.local_paths.table_name;
  cleanup.mark_local_artifacts_owned();
  std::string seed_error;
  if (!SeedEmptyLocalParquet(table, buffer_table_name,
                             metadata.local_paths.parquet_file_path,
                             &seed_error)) {
    return SetFailure(result, CreateTableErrorKind::kInternal, metadata,
                      "Failed to create local Parquet seed file: " +
                          seed_error,
                      error);
  }

  std::string save_error;
  if (!SaveTableMetadata(metadata, &save_error)) {
    return SetFailure(result, CreateTableErrorKind::kInternal, metadata,
                      "Failed to persist Parquet table metadata: " +
                          save_error,
                      error);
  }

  cleanup.dismiss();
  if (result != nullptr) {
    result->error_kind = CreateTableErrorKind::kNone;
    result->metadata = metadata;
  }
  if (error != nullptr) {
    error->clear();
  }
  return true;
}

} // namespace parquet
