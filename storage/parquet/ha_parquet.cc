#include "ha_parquet.h"
#include "ha_parquet_pushdown.h"

#include "parquet_catalog.h"
#include "parquet_create.h"
#include "parquet_duckdb.h"
#include "parquet_iceberg.h"
#include "parquet_metadata.h"
#include "parquet_object_store.h"
#include "parquet_schema.h"
#include "parquet_shared.h"
#include "parquet_transaction.h"
#include "parquet_write_buffer.h"

#include "handler.h"
#include "my_sys.h"
#include "sql_class.h"

#include <json.hpp>

#include <atomic>
#include <cctype>
#include <condition_variable>
#include <cstdio>
#include <ctime>
#include <mutex>
#include <thread>

handlerton *parquet_hton = 0;

namespace {

using json = nlohmann::json;

static THR_LOCK parquet_lock;

struct ha_table_option_struct
{
  char *catalog;
  char *connection;
};

static ha_create_table_option parquet_table_option_list[] = {
    HA_TOPTION_STRING("CATALOG", catalog),
    HA_TOPTION_STRING("CONNECTION", connection),
    HA_TOPTION_END};

static char *parquet_tmp_lakekeeper_bearer_token = 0;
static char *parquet_tmp_s3_access_key_id = 0;
static char *parquet_tmp_s3_secret_access_key = 0;
static uint32_t parquet_write_buffer_max_rows_value = 100000;
static uint32_t parquet_write_buffer_flush_interval_ms_value = 30000;

static void update_lakekeeper_bearer_token(MYSQL_THD,
                                           struct st_mysql_sys_var *,
                                           void *, const void *)
{
  my_free(parquet_lakekeeper_bearer_token);
  parquet_lakekeeper_bearer_token = 0;
  if (parquet_tmp_lakekeeper_bearer_token &&
      parquet_tmp_lakekeeper_bearer_token[0]) {
    parquet_lakekeeper_bearer_token = parquet_tmp_lakekeeper_bearer_token;
    parquet_tmp_lakekeeper_bearer_token =
        my_strdup(PSI_NOT_INSTRUMENTED, "*****", MYF(MY_WME));
  }
}

static void update_s3_access_key_id(MYSQL_THD, struct st_mysql_sys_var *,
                                    void *, const void *)
{
  my_free(parquet_s3_access_key_id);
  parquet_s3_access_key_id = 0;
  if (parquet_tmp_s3_access_key_id && parquet_tmp_s3_access_key_id[0]) {
    parquet_s3_access_key_id = parquet_tmp_s3_access_key_id;
    parquet_tmp_s3_access_key_id =
        my_strdup(PSI_NOT_INSTRUMENTED, "*****", MYF(MY_WME));
  }
}

static void update_s3_secret_access_key(MYSQL_THD, struct st_mysql_sys_var *,
                                        void *, const void *)
{
  my_free(parquet_s3_secret_access_key);
  parquet_s3_secret_access_key = 0;
  if (parquet_tmp_s3_secret_access_key &&
      parquet_tmp_s3_secret_access_key[0]) {
    parquet_s3_secret_access_key = parquet_tmp_s3_secret_access_key;
    parquet_tmp_s3_secret_access_key =
        my_strdup(PSI_NOT_INSTRUMENTED, "*****", MYF(MY_WME));
  }
}

static MYSQL_SYSVAR_STR(lakekeeper_base_url, parquet_lakekeeper_base_url,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "Iceberg REST catalog base URL", 0, 0,
                        "http://localhost:8181/catalog/v1/");
static MYSQL_SYSVAR_STR(lakekeeper_warehouse_id,
                        parquet_lakekeeper_warehouse_id,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "Iceberg REST catalog warehouse name or LakeKeeper "
                        "warehouse ID",
                        0, 0, "");
static MYSQL_SYSVAR_STR(lakekeeper_namespace, parquet_lakekeeper_namespace,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "Default Iceberg REST catalog namespace", 0, 0,
                        "default");
static MYSQL_SYSVAR_STR(lakekeeper_bearer_token,
                        parquet_tmp_lakekeeper_bearer_token,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY |
                            PLUGIN_VAR_MEMALLOC,
                        "Iceberg REST catalog bearer token", 0,
                        update_lakekeeper_bearer_token, "");
static MYSQL_SYSVAR_STR(s3_bucket, parquet_s3_bucket,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "Default Parquet object-store bucket", 0, 0, "");
static MYSQL_SYSVAR_STR(s3_data_prefix, parquet_s3_data_prefix,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "Default Parquet object-store key prefix", 0, 0,
                        "data");
static MYSQL_SYSVAR_STR(s3_region, parquet_s3_region,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "Default Parquet object-store region", 0, 0,
                        "us-east-2");
static MYSQL_SYSVAR_STR(s3_access_key_id, parquet_tmp_s3_access_key_id,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY |
                            PLUGIN_VAR_MEMALLOC,
                        "Parquet object-store access key ID", 0,
                        update_s3_access_key_id, "");
static MYSQL_SYSVAR_STR(s3_secret_access_key,
                        parquet_tmp_s3_secret_access_key,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY |
                            PLUGIN_VAR_MEMALLOC,
                        "Parquet object-store secret access key", 0,
                        update_s3_secret_access_key, "");
static MYSQL_SYSVAR_UINT(write_buffer_max_rows,
                         parquet_write_buffer_max_rows_value,
                         PLUGIN_VAR_RQCMDARG,
                         "Flush write buffer to S3 when buffered row count "
                         "reaches this threshold (0 = flush on every commit)",
                         0, 0, 100000, 0, UINT_MAX, 1);
static MYSQL_SYSVAR_UINT(write_buffer_flush_interval_ms,
                         parquet_write_buffer_flush_interval_ms_value,
                         PLUGIN_VAR_RQCMDARG,
                         "Background flush interval for the write buffer in "
                         "milliseconds",
                         0, 0, 30000, 1000, UINT_MAX, 1);

static struct st_mysql_sys_var *parquet_system_variables[] = {
    MYSQL_SYSVAR(lakekeeper_base_url),
    MYSQL_SYSVAR(lakekeeper_warehouse_id),
    MYSQL_SYSVAR(lakekeeper_namespace),
    MYSQL_SYSVAR(lakekeeper_bearer_token),
    MYSQL_SYSVAR(s3_bucket),
    MYSQL_SYSVAR(s3_data_prefix),
    MYSQL_SYSVAR(s3_region),
    MYSQL_SYSVAR(s3_access_key_id),
    MYSQL_SYSVAR(s3_secret_access_key),
    MYSQL_SYSVAR(write_buffer_max_rows),
    MYSQL_SYSVAR(write_buffer_flush_interval_ms),
    NULL};

const ha_table_option_struct *parquet_table_options(HA_CREATE_INFO *create_info)
{
  return create_info != nullptr
             ? (const ha_table_option_struct *) create_info->option_struct
             : nullptr;
}

bool raise_unknown_error(const std::string &message)
{
  my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), message.c_str());
  return false;
}

bool raise_create_option_error(const std::string &message)
{
  my_printf_error(ER_ILLEGAL_HA_CREATE_OPTION, "%s", MYF(0),
                  message.c_str());
  return false;
}

std::string catalog_status_message(const std::string &operation,
                                   const parquet::CatalogStatus &status)
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

std::string object_status_message(const std::string &operation,
                                  const parquet::ObjectStoreStatus &status)
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

bool resolve_runtime_metadata_or_error(const char *table_path,
                                       parquet::TableMetadata *metadata)
{
  std::string error;
  if (!parquet::ResolveRuntimeTableMetadata(table_path, metadata, &error)) {
    raise_unknown_error(error);
    return false;
  }
  return true;
}

bool validate_catalog_or_error(const parquet::TableMetadata &metadata)
{
  std::string error;
  if (!parquet::ValidateCatalogConfig(metadata, &error)) {
    raise_unknown_error(error);
    return false;
  }
  return true;
}

bool validate_object_store_or_error(const parquet::TableMetadata &metadata)
{
  std::string error;
  if (!parquet::ValidateObjectStoreConfig(metadata, true, &error)) {
    raise_unknown_error(error);
    return false;
  }
  return true;
}

bool bootstrap_catalog_or_error(parquet::ParquetCatalogClient *client,
                                std::string *error)
{
  auto status = client->BootstrapConfig();
  if (!status.ok()) {
    if (error != nullptr) {
      *error = catalog_status_message(
          "failed to bootstrap Iceberg REST catalog", status);
    }
    return false;
  }
  return true;
}

json namespace_json_array(const parquet::CatalogNamespaceIdent &ident)
{
  json namespace_parts = json::array();
  for (const auto &part : ident.parts) {
    namespace_parts.push_back(part);
  }
  return namespace_parts;
}

static std::string item_to_sql(const Item *item)
{
  if (!item) return "";
  const Item_func *func = dynamic_cast<const Item_func *>(item);
  if (!func || func->argument_count() != 2) return "";

  const Item *left = func->arguments()[0];
  const Item *right = func->arguments()[1];

  if (left->type() != Item::FIELD_ITEM) return "";

  const Item_field *field = static_cast<const Item_field *>(left);
  std::string col = parquet::QuoteIdentifier(field->field_name.str);
  std::string val;

  if (right->type() == Item::CONST_ITEM) {
    String tmp;
    String *s = const_cast<Item*>(right)->val_str(&tmp);
    if (!s) return "";
    std::string strval(s->ptr(), s->length());
    bool is_number = !strval.empty() &&
                     (isdigit((unsigned char)strval[0]) || strval[0] == '-');
    val = is_number ? strval : quote_string_literal(strval);
  } else {
    return "";
  }

  std::string op = func->func_name();
  if (op == "=" || op == "eq") return col + " = " + val;
  if (op == ">" || op == "gt") return col + " > " + val;
  if (op == "<" || op == "lt") return col + " < " + val;
  return "";
}

bool is_real_commit(THD *thd, bool all)
{
  return ((all || thd->transaction->all.ha_list == 0) &&
          !(thd->variables.option_bits & OPTION_GTID_BEGIN));
}

bool is_real_rollback(THD *thd, bool all)
{
  return all || thd->transaction->all.ha_list == 0;
}

bool has_pending_work(const parquet::ParquetTxnState &txn_state,
                      int *pending_table_count)
{
  bool pending = false;
  int count = 0;
  for (const auto &entry : txn_state.tables) {
    const auto &table_state = entry.second;
    if (!table_state.local_stage_files.empty() ||
        !table_state.staged_files.empty() ||
        table_state.statement_row_count != 0) {
      pending = true;
      count++;
    }
  }
  if (pending_table_count != nullptr) {
    *pending_table_count = count;
  }
  return pending;
}

bool catalog_configs_compatible(const parquet::CatalogClientConfig &left,
                                const parquet::CatalogClientConfig &right)
{
  return left.base_uri == right.base_uri &&
         left.warehouse == right.warehouse &&
         left.prefix == right.prefix &&
         left.bearer_token == right.bearer_token;
}

bool upload_file(parquet::ParquetTableTxnState *table_state,
                 parquet::ParquetObjectStoreClient *object_store,
                 const std::string &local_path,
                 const parquet::ObjectLocation &location,
                 const std::string &content_type,
                 uint64_t expected_length,
                 std::string *error)
{
  parquet::PutObjectRequest request;
  request.local_file_path = local_path;
  request.location = location;
  request.content_type = content_type;
  request.expected_content_length = expected_length;

  auto status = object_store->PutFile(request);
  if (!status.ok()) {
    if (error != nullptr) {
      *error = object_status_message("failed to upload Parquet object", status);
    }
    return false;
  }

  table_state->uploaded_objects.push_back(location);
  return true;
}

void cleanup_uploaded_objects(parquet::ParquetTableTxnState *table_state,
                              const parquet::TableMetadata &metadata)
{
  if (table_state == nullptr || table_state->uploaded_objects.empty()) {
    return;
  }

  parquet::ParquetObjectStoreClient object_store(metadata.object_store_config);
  auto results = object_store.DeleteObjectsBestEffort(table_state->uploaded_objects);
  for (const auto &result : results) {
    if (!result.status.ok()) {
      std::cerr << "Parquet cleanup failed for "
                << parquet::BuildS3Uri(result.location.bucket,
                                       result.location.key)
                << ": " << result.status.message << std::endl;
    }
  }
  table_state->uploaded_objects.clear();
}

struct CommitWork {
  parquet::ParquetTableTxnState *table_state = nullptr;
  parquet::TableMetadata metadata;
  parquet::CatalogLoadTableResult load_result;
  parquet::IcebergCommitArtifacts artifacts;
  std::unique_ptr<parquet::ParquetCatalogClient> catalog_client;
};

bool prepare_commit_metadata(parquet::ParquetTableTxnState *table_state,
                             CommitWork *work,
                             std::string *error)
{
  if (table_state == nullptr || work == nullptr) {
    if (error != nullptr) {
      *error = "invalid transaction state while preparing Parquet commit";
    }
    return false;
  }

  work->table_state = table_state;
  if (!parquet::ResolveRuntimeTableMetadata(table_state->table_path.c_str(),
                                            &work->metadata, error) ||
      !parquet::ValidateCatalogConfig(work->metadata, error) ||
      !parquet::ValidateObjectStoreConfig(work->metadata, true, error)) {
    return false;
  }

  work->catalog_client.reset(
      new parquet::ParquetCatalogClient(work->metadata.catalog_config));
  return bootstrap_catalog_or_error(work->catalog_client.get(), error);
}

bool materialize_upload_and_build_artifacts(CommitWork *work,
                                            std::string *error)
{
  auto *table_state = work->table_state;
  const bool has_pending_files =
      !table_state->local_stage_files.empty() ||
      !table_state->staged_files.empty();
  if (has_pending_files) {
    auto load_status = work->catalog_client->LoadTable(
        work->metadata.catalog_table_ident, &work->load_result,
        work->metadata.access_delegation);
    if (!load_status.ok()) {
      if (error != nullptr) {
        *error = catalog_status_message(
            "failed to load Iceberg table before commit", load_status);
      }
      return false;
    }

    if (!parquet::ApplyCatalogLoadResult(&work->metadata, work->load_result,
                                         error)) {
      return false;
    }
  }

  parquet::ParquetStagedFile staged_file;
  if (!parquet::MaterializeLocalDataFile(table_state, work->metadata,
                                         &staged_file, error)) {
    return false;
  }
  if (table_state->staged_files.empty()) {
    return true;
  }

  parquet::ParquetObjectStoreClient object_store(
      work->metadata.object_store_config);
  for (const auto &file : table_state->staged_files) {
    parquet::ObjectLocation location;
    if (!parquet::ParseS3Uri(file.target_object_path, &location)) {
      if (error != nullptr) {
        *error = "failed to parse staged object path " + file.target_object_path;
      }
      return false;
    }
    location = parquet::ResolveAbsoluteObjectLocation(
        work->metadata.object_store_config, location.bucket, location.key);
    if (!upload_file(table_state, &object_store, file.local_parquet_path,
                     location, "application/vnd.apache.parquet",
                     file.file_size_bytes, error)) {
      return false;
    }
  }

  if (!parquet::BuildIcebergCommitArtifacts(work->metadata, work->load_result,
                                            table_state->staged_files,
                                            &work->artifacts, error)) {
    return false;
  }

  table_state->local_cleanup_paths.push_back(work->artifacts.manifest_local_path);
  table_state->local_cleanup_paths.push_back(
      work->artifacts.manifest_list_local_path);

  if (!upload_file(table_state, &object_store,
                   work->artifacts.manifest_local_path,
                   work->artifacts.manifest_location,
                   "application/octet-stream", 0, error)) {
    return false;
  }
  return upload_file(table_state, &object_store,
                     work->artifacts.manifest_list_local_path,
                     work->artifacts.manifest_list_location,
                     "application/octet-stream", 0, error);
}

bool save_committed_metadata(CommitWork *work, std::string *error)
{
  if (!parquet::ApplyCatalogLoadResult(&work->metadata, work->load_result,
                                       error)) {
    return false;
  }
  work->metadata.current_snapshot_id =
      std::to_string(work->artifacts.snapshot_id);
  work->metadata.active_files = work->artifacts.active_files;
  work->metadata.active_scan_paths =
      parquet::ExtractActiveScanPaths(work->metadata.active_files);
  return parquet::SaveTableMetadata(work->metadata, error);
}

json table_change_json(const CommitWork &work)
{
  json change = json::parse(work.artifacts.commit_request_json);
  change["identifier"] = {
      {"namespace", namespace_json_array(
                        work.metadata.catalog_table_ident.namespace_ident)},
      {"name", work.metadata.catalog_table_ident.table_name}};
  return change;
}

bool commit_prepared_work(std::vector<CommitWork> *works, std::string *error)
{
  if (works == nullptr || works->empty()) {
    return true;
  }

  if (works->size() == 1) {
    CommitWork &work = works->front();
    parquet::CatalogCommitRequest request;
    request.ident = work.metadata.catalog_table_ident;
    request.request_json = work.artifacts.commit_request_json;

    parquet::CatalogLoadTableResult commit_result;
    auto status = work.catalog_client->CommitTable(request, &commit_result);
    if (!status.ok()) {
      if (!status.commit_state_unknown) {
        cleanup_uploaded_objects(work.table_state, work.metadata);
      }
      if (error != nullptr) {
        *error = catalog_status_message("failed to commit Iceberg table", status);
      }
      return false;
    }
    if (commit_result.status.ok() &&
        !commit_result.metadata.raw_metadata_json.empty()) {
      work.load_result = commit_result;
    }
    return save_committed_metadata(&work, error);
  }

  json changes = json::array();
  for (const auto &work : *works) {
    changes.push_back(table_change_json(work));
  }

  parquet::CatalogTransactionCommitRequest request;
  request.request_json = json({{"table-changes", changes}}).dump();

  auto status = works->front().catalog_client->CommitTransactionIfSupported(
      request);
  if (!status.ok()) {
    if (!status.commit_state_unknown) {
      for (auto &work : *works) {
        cleanup_uploaded_objects(work.table_state, work.metadata);
      }
    }
    if (error != nullptr) {
      *error = catalog_status_message(
          "failed to commit Iceberg REST transaction", status);
    }
    return false;
  }

  for (auto &work : *works) {
    if (!save_committed_metadata(&work, error)) {
      return false;
    }
  }
  return true;
}

static int parquet_flush_table_buffer(
    const std::string &table_path,
    const std::string &table_name,
    std::vector<parquet::ParquetBufferedFile> buffered_files)
{
  if (buffered_files.empty()) {
    return 0;
  }

  parquet_log_info("Parquet write buffer flush begin table='" + table_name +
                   "' files=" + std::to_string(buffered_files.size()));

  parquet::ParquetTableTxnState table_state;
  table_state.table_path = table_path;
  table_state.table_name = table_name;
  for (const auto &buf_file : buffered_files) {
    parquet::ParquetStagedFile staged;
    staged.table_path         = table_path;
    staged.table_name         = table_name;
    staged.local_parquet_path = buf_file.local_path;
    staged.target_object_path = buf_file.target_s3_path;
    staged.record_count       = buf_file.row_count;
    staged.file_size_bytes    = buf_file.file_size_bytes;
    table_state.staged_files.push_back(staged);
  }

  CommitWork work;
  std::string error;
  if (!prepare_commit_metadata(&table_state, &work, &error)) {
    parquet_log_warning("write buffer flush: prepare failed for table='" +
                        table_name + "': " + error);
    return 1;
  }

  if (!materialize_upload_and_build_artifacts(&work, &error)) {
    cleanup_uploaded_objects(&table_state, work.metadata);
    parquet_log_warning("write buffer flush: upload failed for table='" +
                        table_name + "': " + error);
    return 1;
  }

  std::vector<CommitWork> works;
  works.push_back(std::move(work));
  if (!commit_prepared_work(&works, &error)) {
    parquet_log_warning("write buffer flush: catalog commit failed for table='" +
                        table_name + "': " + error);
    return 1;
  }

  for (const auto &buf_file : buffered_files) {
    if (!buf_file.local_path.empty()) {
      std::remove(buf_file.local_path.c_str());
    }
  }
  parquet::RemoveLocalFiles(&table_state);

  parquet_log_info("Parquet write buffer flush complete table='" + table_name + "'");
  return 0;
}

static std::atomic<bool> g_buffer_flush_stop{false};
static std::mutex g_buffer_flush_cv_mutex;
static std::condition_variable g_buffer_flush_cv;
static std::thread g_buffer_flush_thread;

static void parquet_buffer_flush_thread_func()
{
  parquet_log_info("Parquet write buffer flush thread started");
  while (true) {
    std::unique_lock<std::mutex> lock(g_buffer_flush_cv_mutex);
    g_buffer_flush_cv.wait_for(
        lock,
        std::chrono::milliseconds(
            static_cast<int64_t>(parquet_write_buffer_flush_interval_ms_value)),
        [] { return g_buffer_flush_stop.load(); });

    if (g_buffer_flush_stop.load()) {
      break;
    }
    lock.unlock();

    const auto stale = parquet::ParquetWriteBufferStaleTables(
        static_cast<uint64_t>(parquet_write_buffer_flush_interval_ms_value));

    for (const auto &table_path : stale) {
      std::string table_name;
      std::vector<parquet::ParquetBufferedFile> files;
      if (!parquet::ParquetWriteBufferTake(table_path, &table_name, &files)) {
        continue;
      }
      if (parquet_flush_table_buffer(table_path, table_name, std::move(files)) != 0) {
        parquet_log_warning(
            "Parquet write buffer background flush failed for table '" +
            table_path + "'");
      }
    }
  }
  parquet_log_info("Parquet write buffer flush thread stopped");
}

} // namespace

ha_parquet::ha_parquet(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg)
{
  thr_lock_data_init(&parquet_lock, &lock, NULL);
}

ulonglong ha_parquet::table_flags() const { return HA_FILE_BASED; }
ulong ha_parquet::index_flags(uint, uint, bool) const { return 0; }

int ha_parquet::open(const char *name, int, uint)
{
  DBUG_ENTER("ha_parquet::open");
  auto paths = parquet::ResolveLocalPaths(name);
  parquet_file_path = paths.parquet_file_path;
  parquet_log_info("handler open table='" + std::string(name) +
                   "' parquet_file='" + parquet_file_path + "'");
  DBUG_RETURN(0);
}

int ha_parquet::close(void) { return 0; }

const Item *ha_parquet::cond_push(const Item *cond)
{
  DBUG_ENTER("ha_parquet::cond_push");
  pushed_cond_sql.clear();
  has_pushed_cond = false;
  if (cond) {
    pushed_cond_sql = item_to_sql(cond);
    if (!pushed_cond_sql.empty()) {
      has_pushed_cond = true;
      DBUG_RETURN(nullptr);
    }
  }
  DBUG_RETURN(cond);
}

void ha_parquet::cond_pop()
{
  pushed_cond_sql.clear();
  has_pushed_cond = false;
}

int ha_parquet::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{
  std::string error;
  const auto *options = parquet_table_options(create_info);
  parquet::CreateTableResult result;
  if (!parquet::CreateParquetTable(
          name, table_arg, options != nullptr ? options->catalog : nullptr,
          options != nullptr ? options->connection : nullptr, &result,
          &error)) {
    if (result.error_kind ==
        parquet::CreateTableErrorKind::kInvalidCreateOption) {
      raise_create_option_error(error);
      return HA_ERR_UNSUPPORTED;
    }
    if (result.error_kind == parquet::CreateTableErrorKind::kUnsupported) {
      raise_unknown_error(error);
      return HA_ERR_UNSUPPORTED;
    }
    raise_unknown_error(error);
    return HA_ERR_INTERNAL_ERROR;
  }

  parquet_file_path = result.metadata.local_paths.parquet_file_path;
  return 0;
}

int ha_parquet::delete_table(const char *name)
{
  DBUG_ENTER("ha_parquet::delete_table");

  const std::string table_path(name);
  if (!parquet::MetadataSidecarExists(name)) {
    parquet_log_info("drop table skipped Iceberg REST catalog delete for '" +
                     table_path +
                     "' because Parquet sidecar metadata is absent");
    std::remove((table_path + ".parquet").c_str());
    std::remove((table_path + ".parquet.meta").c_str());
    DBUG_RETURN(0);
  }

  parquet::TableMetadata metadata;
  if (!resolve_runtime_metadata_or_error(name, &metadata) ||
      !validate_catalog_or_error(metadata)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  std::string error;
  parquet::ParquetCatalogClient catalog_client(metadata.catalog_config);
  if (!bootstrap_catalog_or_error(&catalog_client, &error)) {
    raise_unknown_error(error);
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
  auto status = catalog_client.DropTable(metadata.catalog_table_ident);
  if (!status.ok() && status.code != parquet::CatalogStatusCode::kNotFound) {
    raise_unknown_error(catalog_status_message(
        "failed to drop Iceberg REST catalog table", status));
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  std::remove((table_path + ".parquet").c_str());
  std::remove((table_path + ".parquet.meta").c_str());
  DBUG_RETURN(0);
}

int ha_parquet::write_row(const uchar *buf)
{
  DBUG_ENTER("ha_parquet::write_row");

  THD *thd = table != nullptr ? table->in_use : nullptr;
  if (thd == nullptr) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  parquet::ParquetTxnState *txn =
      parquet::GetOrCreateTxnState(thd, parquet_hton);
  if (txn == nullptr) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
  txn->registered_with_server = true;

  parquet::ParquetTableTxnState *table_state =
      parquet::GetOrCreateTableTxnState(txn, table->s);
  if (table_state == nullptr) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  const std::string desired_buffer_name = parquet::StatementBufferName(
      static_cast<unsigned long long>(thd->thread_id),
      static_cast<unsigned long long>(thd->query_id),
      table->s->normalized_path.str);
  if (!table_state->statement_buffer_name.empty() &&
      table_state->statement_buffer_name != desired_buffer_name) {
    parquet::ResetStatementBuffer(table_state);
  }
  if (table_state->statement_buffer_name != desired_buffer_name) {
    table_state->statement_buffer_name = desired_buffer_name;
    table_state->statement_row_count = 0;
  }

  try {
    std::lock_guard<std::mutex> lock(parquet::parquet_duckdb_mutex());
    std::string connection_error;
    duckdb::Connection *connection =
        parquet::parquet_handler_connection_locked(&connection_error);
    if (connection == nullptr) {
      std::cerr << "DuckDB connection error: " << connection_error
                << std::endl;
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }

    if (!parquet::AppendRecordToStatementBufferLocked(
            table_state, table, buf, connection, &connection_error)) {
      parquet_log_warning("DuckDB appender row error: " + connection_error);
      if (connection_error.find("unsupported MariaDB column type") !=
          std::string::npos) {
        DBUG_RETURN(HA_ERR_UNSUPPORTED);
      }
      DBUG_RETURN(HA_ERR_GENERIC);
    }
  } catch (const std::exception &ex) {
    parquet_log_warning(std::string("write_row exception: ") + ex.what());
    parquet::ResetStatementBuffer(table_state);
    DBUG_RETURN(HA_ERR_GENERIC);
  }

  DBUG_RETURN(0);
}

int ha_parquet::update_row(const uchar *, const uchar *) { return HA_ERR_WRONG_COMMAND; }
int ha_parquet::delete_row(const uchar *) { return HA_ERR_WRONG_COMMAND; }

int ha_parquet::rnd_init(bool scan)
{
  (void) scan;
  DBUG_ENTER("ha_parquet::rnd_init");

  current_row = 0;
  scan_result.reset();
  parquet::TableMetadata metadata;
  if (!resolve_runtime_metadata_or_error(table->s->normalized_path.str,
                                         &metadata) ||
      !validate_catalog_or_error(metadata)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  std::vector<std::string> scan_paths;
  std::string error;
  if (!resolve_parquet_scan_paths(&metadata, &scan_paths, &error)) {
    raise_unknown_error(error);
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  for (const auto &local_path : parquet::ParquetWriteBufferGetLocalPaths(
           table->s->normalized_path.str)) {
    scan_paths.push_back(local_path);
  }

  if (scan_paths.empty()) {
    parquet_log_info("rnd_init: no scan paths found, returning empty scan");
    DBUG_RETURN(0);
  }

  if (!validate_object_store_or_error(metadata)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  std::string query = parquet::BuildDuckDBReadParquetSql(scan_paths);
  if (has_pushed_cond && !pushed_cond_sql.empty()) {
    query += " WHERE " + pushed_cond_sql;
  }

  try {
    std::lock_guard<std::mutex> lock(parquet::parquet_duckdb_mutex());
    std::string connection_error;
    duckdb::Connection *connection =
        parquet::parquet_handler_connection_locked(&connection_error);
    if (connection == nullptr) {
      raise_unknown_error(connection_error);
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }
    if (!configure_duckdb_s3(connection, metadata.object_store_config, &error)) {
      raise_unknown_error(error);
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }

    parquet_log_info("DuckDB query [read/scan] " +
                     parquet_log_preview(query));
    auto result = connection->Query(query);
    if (!result || result->HasError()) {
      parquet_log_warning("rnd_init read error: " +
                          (result ? result->GetError() : "null result"));
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }
    scan_result = std::move(result);

  } catch (const std::exception &e) {
    parquet_log_warning(std::string("rnd_init exception: ") + e.what());
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  DBUG_RETURN(0);
}

int ha_parquet::rnd_next(uchar *buf)
{
  (void) buf;
  DBUG_ENTER("ha_parquet::rnd_next");

  if (!scan_result || current_row >= scan_result->RowCount()) {
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  for (uint i = 0; i < table->s->fields; i++) {
    Field *field = table->field[i];
    auto value = scan_result->GetValue(i, current_row);
    std::string error;
    if (!parquet::StoreDuckDBValueInMariaDBField(field, value, table->in_use,
                                                 &error)) {
      raise_unknown_error(error);
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }
  }

  current_row++;
  DBUG_RETURN(0);
}

int ha_parquet::rnd_pos(uchar *, uchar *) { return HA_ERR_WRONG_COMMAND; }
void ha_parquet::position(const uchar *) {}
int ha_parquet::info(uint) { return 0; }

enum_alter_inplace_result
ha_parquet::check_if_supported_inplace_alter(TABLE *, Alter_inplace_info *)
{ return HA_ALTER_INPLACE_NOT_SUPPORTED; }

int ha_parquet::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_parquet::external_lock");

  if (lock_type == F_RDLCK || lock_type == F_WRLCK) {
    if (lock_type == F_WRLCK) {
      parquet::TableMetadata metadata;
      if (!resolve_runtime_metadata_or_error(table_share->normalized_path.str,
                                             &metadata) ||
          !validate_catalog_or_error(metadata) ||
          !validate_object_store_or_error(metadata)) {
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
      }
    }

    trans_register_ha(thd, false, parquet_hton, 0);
    if (thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
      trans_register_ha(thd, true, parquet_hton, 0);
    }

    if (lock_type == F_WRLCK) {
      parquet::ParquetTxnState *txn =
          parquet::GetOrCreateTxnState(thd, parquet_hton);
      if (txn == nullptr) {
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
      }
      txn->registered_with_server = true;
      parquet::ParquetTableTxnState *table_state =
          parquet::GetOrCreateTableTxnState(txn, table_share);
      if (table_state == nullptr) {
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
      }

      const std::string desired_buffer_name = parquet::StatementBufferName(
          static_cast<unsigned long long>(thd->thread_id),
          static_cast<unsigned long long>(thd->query_id),
          table_share->normalized_path.str);
      if (!table_state->statement_buffer_name.empty() &&
          table_state->statement_buffer_name != desired_buffer_name) {
        parquet::ResetStatementBuffer(table_state);
      }
      if (table_state->statement_buffer_name != desired_buffer_name) {
        table_state->statement_buffer_name = desired_buffer_name;
        table_state->statement_row_count = 0;
        parquet_log_info("Parquet registered write statement table='" +
                         table_state->table_name + "' buffer='" +
                         table_state->statement_buffer_name + "' query_id=" +
                         std::to_string(static_cast<unsigned long long>(
                             thd->query_id)));
      }
    }
  }

  DBUG_RETURN(0);
}

THR_LOCK_DATA **ha_parquet::store_lock(THD *, THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type = lock_type;
  *to++ = &lock;
  return to;
}

static handler *parquet_create_handler(handlerton *p_hton, TABLE_SHARE *table,
                                       MEM_ROOT *mem_root)
{
  return new (mem_root) ha_parquet(p_hton, table);
}

static int ha_parquet_commit(THD *thd, bool all)
{
  parquet::ParquetTxnState *txn =
      parquet::GetTxnState(thd, parquet_hton);
  if (!txn) return 0;

  std::string stage_error;
  for (auto &entry : txn->tables) {
    parquet::ParquetTableTxnState &table_state = entry.second;
    if (table_state.statement_row_count != 0 &&
        !parquet::StageStatementBufferToLocal(
            &table_state, table_state.table_path + ".parquet", &stage_error)) {
      raise_unknown_error("Failed to stage Parquet statement rows locally: " +
                          stage_error);
      txn->has_error = true;
      return 1;
    }
  }

  int pending_table_count = 0;
  const bool pending = has_pending_work(*txn, &pending_table_count);

  if (!is_real_commit(thd, all)) {
    if (pending) {
      parquet_log_info(
          "Parquet statement commit complete; remote flush deferred until "
          "transaction commit tables=" + std::to_string(pending_table_count));
    }
    return 0;
  }

  if (!pending) {
    parquet_log_info("Parquet transaction commit has no staged write work");
    parquet::ClearTxnState(thd, parquet_hton);
    return 0;
  }

  parquet_log_info("Parquet transaction commit begin tables=" +
                   std::to_string(pending_table_count));

  std::string error;
  for (auto &entry : txn->tables) {
    parquet::ParquetTableTxnState &table_state = entry.second;
    if (table_state.local_stage_files.empty() &&
        table_state.staged_files.empty()) {
      continue;
    }

    parquet::TableMetadata metadata;
    if (!parquet::ResolveRuntimeTableMetadata(table_state.table_path.c_str(),
                                              &metadata, &error) ||
        !parquet::ValidateObjectStoreConfig(metadata, true, &error)) {
      raise_unknown_error(error);
      txn->has_error = true;
      return 1;
    }

    parquet::ParquetStagedFile staged_file;
    if (!parquet::MaterializeLocalDataFile(&table_state, metadata,
                                           &staged_file, &error)) {
      raise_unknown_error("Failed to materialize Parquet data file: " + error);
      txn->has_error = true;
      return 1;
    }

    if (table_state.staged_files.empty()) {
      continue;
    }

    parquet::ParquetBufferedFile buf_file;
    buf_file.local_path       = staged_file.local_parquet_path;
    buf_file.target_s3_path   = staged_file.target_object_path;
    buf_file.row_count        = staged_file.record_count;
    buf_file.file_size_bytes  = staged_file.file_size_bytes;
    parquet::ParquetWriteBufferAppend(table_state.table_path,
                                      table_state.table_name, buf_file);

    if (parquet_write_buffer_max_rows_value == 0 ||
        parquet::ParquetWriteBufferShouldFlush(
            table_state.table_path,
            static_cast<uint64_t>(parquet_write_buffer_max_rows_value))) {
      std::string flush_table_name;
      std::vector<parquet::ParquetBufferedFile> flush_files;
      if (parquet::ParquetWriteBufferTake(table_state.table_path,
                                          &flush_table_name, &flush_files)) {
        if (parquet_flush_table_buffer(table_state.table_path, flush_table_name,
                                       std::move(flush_files)) != 0) {
          raise_unknown_error(
              "Parquet write buffer flush failed for table '" +
              table_state.table_name + "'");
          txn->has_error = true;
          return 1;
        }
      }
    }
  }

  parquet_log_info("Parquet transaction commit complete (buffered)");
  parquet::ClearTxnState(thd, parquet_hton);
  return 0;
}

static int ha_parquet_rollback(THD *thd, bool all)
{
  parquet::ParquetTxnState *txn =
      parquet::GetTxnState(thd, parquet_hton);
  if (!txn) return 0;

  const bool real_rollback = is_real_rollback(thd, all);
  parquet_log_info(std::string("Parquet rollback begin scope='") +
                   (real_rollback ? "transaction" : "statement") + "'");
  int error = 0;
  for (auto &entry : txn->tables) {
    parquet::ParquetTableTxnState &table_state = entry.second;
    parquet::ResetStatementBuffer(&table_state);

    if (!real_rollback) {
      continue;
    }

    parquet::TableMetadata metadata;
    if (!table_state.uploaded_objects.empty()) {
      std::string metadata_error;
      if (parquet::ResolveRuntimeTableMetadata(table_state.table_path.c_str(),
                                               &metadata, &metadata_error) &&
          parquet::ValidateObjectStoreConfig(metadata, true, &metadata_error)) {
        cleanup_uploaded_objects(&table_state, metadata);
      } else {
        std::cerr << "rollback: failed to resolve object-store metadata: "
                  << metadata_error << std::endl;
        error = 1;
      }
    }
    parquet::RemoveLocalFiles(&table_state);
  }

  if (real_rollback) {
    parquet::ClearTxnState(thd, parquet_hton);
  }
  parquet_log_info("Parquet rollback complete");
  return error;
}

static int ha_parquet_init(void *p)
{
  parquet_hton = (handlerton *) p;
  parquet_hton->create = parquet_create_handler;
  parquet_hton->create_select = create_duckdb_select_handler;
  parquet_hton->commit = ha_parquet_commit;
  parquet_hton->rollback = ha_parquet_rollback;
  parquet_hton->table_options = parquet_table_option_list;
  thr_lock_init(&parquet_lock);

  update_lakekeeper_bearer_token(0, 0, 0, 0);
  update_s3_access_key_id(0, 0, 0, 0);
  update_s3_secret_access_key(0, 0, 0, 0);

  std::string duckdb_error;
  if (!parquet::parquet_init_shared_duckdb_runtime(&duckdb_error)) {
    parquet_log_warning("DuckDB runtime initialization failed: " +
                        duckdb_error);
    parquet_hton = 0;
    thr_lock_delete(&parquet_lock);
    return 1;
  }
  parquet_log_info("DuckDB global instance initialized");

  parquet::ParquetWriteBufferInit();
  g_buffer_flush_stop.store(false);
  g_buffer_flush_thread = std::thread(parquet_buffer_flush_thread_func);
  return 0;
}

static int ha_parquet_deinit(void *)
{
  {
    std::lock_guard<std::mutex> lock(g_buffer_flush_cv_mutex);
    g_buffer_flush_stop.store(true);
  }
  g_buffer_flush_cv.notify_one();
  if (g_buffer_flush_thread.joinable()) {
    g_buffer_flush_thread.join();
  }

  const auto remaining = parquet::ParquetWriteBufferStaleTables(0);
  for (const auto &table_path : remaining) {
    std::string table_name;
    std::vector<parquet::ParquetBufferedFile> files;
    if (parquet::ParquetWriteBufferTake(table_path, &table_name, &files)) {
      parquet_flush_table_buffer(table_path, table_name, std::move(files));
    }
  }

  parquet::ParquetWriteBufferDeinit();
  parquet::parquet_deinit_shared_duckdb_runtime();
  parquet_log_info("DuckDB global instance deinitialized");
  parquet_hton = 0;
  thr_lock_delete(&parquet_lock);
  return 0;
}

struct st_mysql_storage_engine parquet_storage_engine =
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

maria_declare_plugin(parquet)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &parquet_storage_engine,
  "PARQUET",
  "MariaDB",
  "Parquet storage engine backed by DuckDB and Iceberg REST catalog",
  PLUGIN_LICENSE_GPL,
  ha_parquet_init,
  ha_parquet_deinit,
  0x0100,
  NULL,
  parquet_system_variables,
  "1.0",
  MariaDB_PLUGIN_MATURITY_EXPERIMENTAL
}
maria_declare_plugin_end;
