#define MYSQL_SERVER 1

#include "parquet_shared.h"

#include "parquet_duckdb.h"
#include "parquet_metadata.h"
#include "parquet_object_store.h"

#include "log.h"

#include <algorithm>
#include <cctype>
#include <string>
#include <unordered_set>
#include <utility>

char *parquet_lakekeeper_base_url = nullptr;
char *parquet_lakekeeper_warehouse_id = nullptr;
char *parquet_lakekeeper_namespace = nullptr;
char *parquet_lakekeeper_bearer_token = nullptr;
char *parquet_s3_bucket = nullptr;
char *parquet_s3_data_prefix = nullptr;
char *parquet_s3_region = nullptr;
char *parquet_s3_access_key_id = nullptr;
char *parquet_s3_secret_access_key = nullptr;

namespace {

std::string trim_copy(std::string value)
{
  auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
  value.erase(value.begin(),
              std::find_if(value.begin(), value.end(), not_space));
  value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(),
              value.end());
  return value;
}

std::string ensure_trailing_slash(const std::string &value)
{
  if (value.empty() || value.back() == '/')
    return value;
  return value + "/";
}

std::string trim_slashes_copy(std::string value)
{
  while (!value.empty() && value.front() == '/')
    value.erase(value.begin());
  while (!value.empty() && value.back() == '/')
    value.pop_back();
  return value;
}

std::string single_line_copy(std::string value)
{
  for (char &ch : value) {
    if (ch == '\n' || ch == '\r' || ch == '\t')
      ch = ' ';
  }
  return trim_copy(value);
}

std::string string_from_sysvar(const char *value, const char *fallback = "")
{
  if (value != nullptr)
    return value;
  return fallback;
}

std::string duckdb_s3_endpoint_setting(const std::string &endpoint)
{
  auto trimmed = trim_copy(endpoint);
  const std::string https_prefix = "https://";
  const std::string http_prefix = "http://";

  if (trimmed.rfind(https_prefix, 0) == 0)
    trimmed.erase(0, https_prefix.size());
  else if (trimmed.rfind(http_prefix, 0) == 0)
    trimmed.erase(0, http_prefix.size());

  while (!trimmed.empty() && trimmed.back() == '/')
    trimmed.pop_back();

  return trimmed;
}

bool duckdb_s3_use_ssl(const std::string &endpoint)
{
  return endpoint.rfind("http://", 0) != 0;
}

std::string duckdb_s3_url_style(const std::string &url_style)
{
  std::string lowered = url_style;
  std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                 [](unsigned char ch) {
                   return static_cast<char>(std::tolower(ch));
                 });

  if (lowered == "virtual-host" || lowered == "virtual_host" ||
      lowered == "vhost") {
    return "vhost";
  }

  return "path";
}

} // namespace

ParquetPluginConfigSnapshot parquet_plugin_config_snapshot()
{
  ParquetPluginConfigSnapshot config;
  config.lakekeeper_base_url =
      ensure_trailing_slash(string_from_sysvar(
          parquet_lakekeeper_base_url, "http://localhost:8181/catalog/v1/"));
  config.lakekeeper_warehouse_id =
      string_from_sysvar(parquet_lakekeeper_warehouse_id);
  config.lakekeeper_namespace =
      string_from_sysvar(parquet_lakekeeper_namespace, "default");
  config.lakekeeper_bearer_token =
      string_from_sysvar(parquet_lakekeeper_bearer_token);
  config.s3_bucket = string_from_sysvar(parquet_s3_bucket);
  config.s3_data_prefix =
      trim_slashes_copy(string_from_sysvar(parquet_s3_data_prefix, "data"));
  config.s3_region = string_from_sysvar(parquet_s3_region, "us-east-2");
  config.s3_access_key_id = string_from_sysvar(parquet_s3_access_key_id);
  config.s3_secret_access_key =
      string_from_sysvar(parquet_s3_secret_access_key);
  return config;
}

std::string parquet_default_s3_endpoint_url(const std::string &region)
{
  if (region.empty() || region == "us-east-1")
    return "https://s3.amazonaws.com";

  return "https://s3." + region + ".amazonaws.com";
}

std::string quote_string_literal(const std::string &value)
{
  std::string quoted = "'";
  for (char ch : value) {
    if (ch == '\'')
      quoted += "''";
    else
      quoted += ch;
  }
  quoted += "'";
  return quoted;
}

std::string parquet_log_preview(const std::string &value, size_t max_length)
{
  std::string normalized = single_line_copy(value);
  if (normalized.size() <= max_length)
    return normalized;

  return normalized.substr(0, max_length) + "...<truncated>";
}

void parquet_log_info(const std::string &message)
{
  sql_print_information("Parquet: %s", message.c_str());
}

void parquet_log_warning(const std::string &message)
{
  sql_print_warning("Parquet: %s", message.c_str());
}

std::string parquet_s3_object_path(const parquet::ObjectStoreConfig &config,
                                   const std::string &object_name)
{
  const auto location = parquet::ResolveObjectLocation(config, object_name);
  return parquet::BuildS3Uri(location.bucket, location.key);
}

bool configure_duckdb_s3(duckdb::Connection *con,
                         const parquet::ObjectStoreConfig &config,
                         std::string *error)
{
  if (con == nullptr) {
    if (error != nullptr)
      *error = "DuckDB connection must not be null";
    return false;
  }

  if (config.bucket.empty()) {
    if (error != nullptr)
      *error = "object store bucket is missing";
    return false;
  }

  if (config.auth_mode == parquet::ObjectStoreAuthMode::kRemoteSigning) {
    if (error != nullptr) {
      *error =
          "object store auth_mode=remote_signing is not supported by the "
          "current Parquet handler path";
    }
    return false;
  }

  if (config.credentials.empty()) {
    if (error != nullptr) {
      *error =
          "object store credentials are missing; configure "
          "parquet_s3_access_key_id and parquet_s3_secret_access_key";
    }
    return false;
  }

  if (config.auth_mode == parquet::ObjectStoreAuthMode::kTemporaryCredentials &&
      config.credentials.session_token.empty()) {
    if (error != nullptr) {
      *error =
          "object store auth_mode=temporary requires a session_token, which "
          "is not currently available through Parquet plugin variables";
    }
    return false;
  }

  parquet_log_info("DuckDB configuring S3 endpoint='" + config.endpoint +
                   "' region='" + config.region + "' bucket='" +
                   config.bucket + "' key_prefix='" + config.key_prefix +
                   "' url_style='" + config.url_style + "'");

  if (!config.region.empty()) {
    if (!parquet::parquet_run_duckdb_query(con, "S3 config", "SET s3_region=" + quote_string_literal(config.region), error)) return false;
  }
  if (!parquet::parquet_run_duckdb_query(con, "S3 config", "SET s3_access_key_id=" + quote_string_literal(config.credentials.access_key_id), error)) return false;
  if (!parquet::parquet_run_duckdb_query(con, "S3 config", "SET s3_secret_access_key=" + quote_string_literal(config.credentials.secret_access_key), error)) return false;
  if (!config.credentials.session_token.empty()) {
    if (!parquet::parquet_run_duckdb_query(con, "S3 config", "SET s3_session_token=" + quote_string_literal(config.credentials.session_token), error)) return false;
  }

  if (!config.endpoint.empty()) {
    if (!parquet::parquet_run_duckdb_query(con, "S3 config", "SET s3_endpoint=" + quote_string_literal(duckdb_s3_endpoint_setting(config.endpoint)), error)) return false;
    if (!parquet::parquet_run_duckdb_query(con, "S3 config", std::string("SET s3_use_ssl=") + (duckdb_s3_use_ssl(config.endpoint) ? "true" : "false"), error)) return false;
  }
  if (!parquet::parquet_run_duckdb_query(con, "S3 config", "SET s3_url_style=" + quote_string_literal(duckdb_s3_url_style(config.url_style)), error)) return false;
  return true;
}

std::string table_name_from_path(const std::string &table_path)
{
  size_t pos = table_path.find_last_of("/\\");
  return (pos == std::string::npos) ? table_path : table_path.substr(pos + 1);
}

std::vector<std::string> extract_legacy_fake_manifest_list_scan_paths(
    const std::string &response_body)
{
  std::vector<std::string> s3_files;
  std::unordered_set<std::string> seen_s3_files;
  size_t pos = 0;
  while ((pos = response_body.find("\"manifest-list\"", pos)) !=
         std::string::npos) {
    size_t colon = response_body.find(':', pos);
    if (colon == std::string::npos)
      break;
    size_t value_start = response_body.find('"', colon + 1);
    if (value_start == std::string::npos)
      break;
    size_t value_end = response_body.find('"', value_start + 1);
    if (value_end == std::string::npos)
      break;

    std::string path = response_body.substr(value_start + 1,
                                            value_end - value_start - 1);
    std::string lowered = path;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                   [](unsigned char ch) {
                     return static_cast<char>(std::tolower(ch));
                   });
    const bool looks_like_legacy_fake_data_file =
        path.rfind("s3://", 0) == 0 &&
        lowered.size() >= 8 &&
        lowered.compare(lowered.size() - 8, 8, ".parquet") == 0;
    if (looks_like_legacy_fake_data_file && seen_s3_files.insert(path).second)
      s3_files.push_back(path);

    pos = value_end + 1;
  }
  return s3_files;
}

bool resolve_parquet_scan_paths(parquet::TableMetadata *metadata,
                                std::vector<std::string> *paths,
                                std::string *error)
{
  if (metadata == nullptr || paths == nullptr) {
    if (error != nullptr)
      *error = "metadata and paths outputs must not be null";
    return false;
  }

  paths->clear();
  if (!metadata->active_scan_paths.empty()) {
    *paths = metadata->active_scan_paths;
    return true;
  }

  if (!metadata->active_files.empty()) {
    for (const auto &file : metadata->active_files) {
      if (!file.path.empty())
        paths->push_back(file.path);
    }
    if (!paths->empty()) {
      metadata->active_scan_paths = *paths;
      return true;
    }
  }

  // Legacy migration only: early sidecars stored fake data-file paths in the
  // catalog metadata "manifest-list" field instead of active_files.
  *paths = extract_legacy_fake_manifest_list_scan_paths(
      metadata->raw_catalog_metadata_json);
  if (paths->empty()) {
    // An empty Iceberg table with a snapshot but no data files is valid
    // (e.g. a newly created table where LakeKeeper issues an initial empty
    // snapshot). Return an empty path list so callers produce zero rows.
    if (!metadata->current_snapshot_id.empty()) {
      parquet_log_info(
          "Parquet table has Iceberg snapshot '" +
          metadata->current_snapshot_id +
          "' but no data files yet; returning empty scan path list");
    }
    return true;
  }

  parquet_log_warning(
      "Parquet table used legacy manifest-list scan fallback; persisting "
      "active_scan_paths to the sidecar");
  metadata->active_scan_paths = *paths;
  if (metadata->active_files.empty()) {
    for (const auto &path : *paths) {
      parquet::ActiveDataFile file;
      file.path = path;
      file.snapshot_id = metadata->current_snapshot_id;
      metadata->active_files.push_back(std::move(file));
    }
  }

  std::string save_error;
  if (!parquet::SaveTableMetadata(*metadata, &save_error)) {
    parquet_log_warning("failed to persist legacy scan-path fallback: " +
                        save_error);
  }
  return true;
}
