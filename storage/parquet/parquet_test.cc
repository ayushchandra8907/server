  #define MYSQL_SERVER 1
  
  #include "my_global.h"
  #include "my_config.h"

  #include "table.h"
  #include "field.h"
  #include "sql_type.h"
  #include "parquet_catalog.h"
  #include "parquet_create.h"
  #include "parquet_iceberg.h"
  #include "parquet_metadata.h"
  #include "parquet_object_store.h"
  #include "parquet_schema.h"
  #include "parquet_shared.h"
  #include "parquet_transaction.h"

  #include <cstdio>
  #include <fstream>
  #include <string>
  #include <tap.h>


namespace {

struct PluginConfigGuard {
  char *lakekeeper_base_url = parquet_lakekeeper_base_url;
  char *lakekeeper_warehouse_id = parquet_lakekeeper_warehouse_id;
  char *lakekeeper_namespace = parquet_lakekeeper_namespace;
  char *lakekeeper_bearer_token = parquet_lakekeeper_bearer_token;
  char *s3_bucket = parquet_s3_bucket;
  char *s3_data_prefix = parquet_s3_data_prefix;
  char *s3_region = parquet_s3_region;
  char *s3_access_key_id = parquet_s3_access_key_id;
  char *s3_secret_access_key = parquet_s3_secret_access_key;

  ~PluginConfigGuard()
  {
    parquet_lakekeeper_base_url = lakekeeper_base_url;
    parquet_lakekeeper_warehouse_id = lakekeeper_warehouse_id;
    parquet_lakekeeper_namespace = lakekeeper_namespace;
    parquet_lakekeeper_bearer_token = lakekeeper_bearer_token;
    parquet_s3_bucket = s3_bucket;
    parquet_s3_data_prefix = s3_data_prefix;
    parquet_s3_region = s3_region;
    parquet_s3_access_key_id = s3_access_key_id;
    parquet_s3_secret_access_key = s3_secret_access_key;
  }
};

void set_test_plugin_config()
{
  parquet_lakekeeper_base_url =
      const_cast<char *>("http://127.0.0.1:8181/catalog/v1/");
  parquet_lakekeeper_warehouse_id =
      const_cast<char *>("warehouse-123");
  parquet_lakekeeper_namespace = const_cast<char *>("analytics.stage");
  parquet_lakekeeper_bearer_token = const_cast<char *>("secret-token");
  parquet_s3_bucket = const_cast<char *>("warehouse");
  parquet_s3_data_prefix = const_cast<char *>("iceberg/analytics/users");
  parquet_s3_region = const_cast<char *>("us-east-1");
  parquet_s3_access_key_id = const_cast<char *>("minio");
  parquet_s3_secret_access_key = const_cast<char *>("secret");
}

} // namespace

static void test_build_query_basic_schema()
{
  LEX_CSTRING id_name= {STRING_WITH_LEN("id")};
  LEX_CSTRING name_name= {STRING_WITH_LEN("name")};

  TABLE table{};
  TABLE_SHARE share{};
  table.s= &share;

  Field_long id_field(11, false, &id_name, false);
  Field_varstring name_field(255, false, &name_name, &share,
                             DTCollation(&my_charset_bin));

  Field *fields[]= {&id_field, &name_field, nullptr};
  share.field= fields;

  std::string query;
  std::string error;
  ok(parquet::BuildDuckDBCreateTableSql("users", &table, &query, &error) &&
         query == "CREATE TABLE IF NOT EXISTS \"users\" "
                  "(\"id\" INTEGER, \"name\" VARCHAR)",
     "build_query maps INTEGER and VARCHAR columns");
}

static void test_build_query_blob_mapping()
{
  LEX_CSTRING payload_name= {STRING_WITH_LEN("payload")};

  TABLE table{};
  TABLE_SHARE share{};
  table.s= &share;

  Field_blob payload_field(1024, false, &payload_name,
                           DTCollation(&my_charset_bin));

  Field *fields[]= {&payload_field, nullptr};
  share.field= fields;

  std::string query;
  std::string error;
  ok(parquet::BuildDuckDBCreateTableSql("files", &table, &query, &error) &&
         query == "CREATE TABLE IF NOT EXISTS \"files\" (\"payload\" BLOB)",
     "build_query maps binary blob columns to BLOB");
}

static void test_build_query_bit_mapping()
{
  LEX_CSTRING flag_name= {STRING_WITH_LEN("flag")};
  uchar record[1]= {0};
  uchar bit_storage= 0;

  TABLE table{};
  TABLE_SHARE share{};
  table.s= &share;

  Field_bit flag_field(record, 1, nullptr, 0, &bit_storage, 0, Field::NONE,
                       &flag_name);

  Field *fields[]= {&flag_field, nullptr};
  share.field= fields;

  std::string query;
  std::string schema_json;
  std::string error;
  ok(parquet::BuildDuckDBCreateTableSql("flags", &table, &query, &error) &&
         query == "CREATE TABLE IF NOT EXISTS \"flags\" (\"flag\" BOOLEAN)" &&
         parquet::BuildIcebergSchemaJson(&table, 0, &schema_json, &error) &&
         schema_json.find("\"name\":\"flag\"") != std::string::npos &&
         schema_json.find("\"type\":\"boolean\"") != std::string::npos,
     "BIT columns map to DuckDB BOOLEAN and Iceberg boolean");
}

static void test_store_duckdb_boolean_in_bit_field()
{
  LEX_CSTRING flag_name= {STRING_WITH_LEN("flag")};
  uchar record[1]= {0};
  uchar bit_storage= 0;
  Field_bit flag_field(record, 1, nullptr, 0, &bit_storage, 0, Field::NONE,
                       &flag_name);

  duckdb::Value true_value= duckdb::Value::BOOLEAN(true);
  duckdb::Value false_value= duckdb::Value::BOOLEAN(false);
  std::string error;
  const bool stored_true=
      parquet::StoreDuckDBValueInMariaDBField(&flag_field, true_value, nullptr,
                                              &error) &&
      flag_field.val_int() == 1;
  const bool stored_false=
      parquet::StoreDuckDBValueInMariaDBField(&flag_field, false_value, nullptr,
                                              &error) &&
      flag_field.val_int() == 0;

  ok(stored_true && stored_false,
     "BIT readback stores DuckDB BOOLEAN values numerically");
}

static void test_build_iceberg_schema_json()
{
  LEX_CSTRING id_name= {STRING_WITH_LEN("id")};
  LEX_CSTRING name_name= {STRING_WITH_LEN("name")};

  TABLE table{};
  TABLE_SHARE share{};
  table.s= &share;

  Field_long id_field(11, false, &id_name, false);
  Field_varstring name_field(255, false, &name_name, &share,
                             DTCollation(&my_charset_bin));

  Field *fields[]= {&id_field, &name_field, nullptr};
  share.field= fields;

  std::string schema_json;
  std::string error;
  ok(parquet::BuildIcebergSchemaJson(&table, 0, &schema_json, &error) &&
         schema_json.find("\"type\":\"struct\"") != std::string::npos &&
         schema_json.find("\"name\":\"id\"") != std::string::npos &&
         schema_json.find("\"type\":\"int\"") != std::string::npos &&
         schema_json.find("\"name\":\"name\"") != std::string::npos &&
         schema_json.find("\"type\":\"string\"") != std::string::npos,
     "shared schema builder maps MariaDB fields to Iceberg schema JSON");
}

static void test_local_path_resolution()
{
  auto paths= parquet::ResolveLocalPaths("/tmp/test_db/users");
  ok(paths.table_name == "users" &&
     paths.helper_db_path == "/tmp/test_db/duckdb_helper.duckdb" &&
     paths.parquet_file_path == "/tmp/test_db/users.parquet",
     "local path resolution keeps helper and parquet paths consistent");
}

static void test_staged_file_helpers()
{
  parquet::ParquetStagedFile staged_file;
  ok(!parquet::IsStagedFileComplete(staged_file),
     "default staged file metadata is intentionally incomplete");

  staged_file.table_path= "/tmp/test_db/users";
  staged_file.table_name= "users";
  staged_file.local_parquet_path= "/tmp/test_db/users.stage_7.parquet";
  staged_file.target_object_path= "s3://parquet-stage/users/flush_7.parquet";
  staged_file.record_count= 3;
  staged_file.file_size_bytes= 128;
  staged_file.flush_id= 7;

  ok(parquet::IsStagedFileComplete(staged_file),
     "fully populated staged file metadata validates");
}

static void test_transaction_state_validation()
{
  parquet::ParquetTxnState txn_state;
  std::string error;

  ok(parquet::ValidateTxnState(txn_state, &error),
     "empty transaction state validates");

  txn_state.registered_with_server= true;
  auto &table_state= txn_state.tables["/tmp/test_db/users"];
  table_state.staged_files.push_back({
      "/tmp/test_db/users",
      "users",
      "/tmp/test_db/users.stage_7.parquet",
      "s3://parquet-stage/users/flush_7.parquet",
      3,
      128,
      7});
  ok(parquet::ValidateTxnState(txn_state, &error),
     "registered transaction state with staged files validates");

  txn_state.has_error= true;
  ok(!parquet::ValidateTxnState(txn_state, &error) &&
         error == "transaction state is marked as failed",
     "failed transaction state is rejected");
}

static void test_stage_path_helpers()
{
  ok(parquet::BuildLocalStagePath("/tmp/test_db/users.parquet", 7) ==
         "/tmp/test_db/users.stage_7.parquet" &&
         parquet::BuildPrototypeObjectPath("users", 7) ==
             "s3://parquet-stage/users/flush_7.parquet",
     "stage path helpers stay deterministic for Stage 1");
}

static void test_catalog_base_uri_normalization()
{
  ok(parquet::NormalizeCatalogBaseUri("http://localhost:8181/catalog/") ==
         "http://localhost:8181/catalog",
     "catalog base URI normalization strips trailing slashes");

  ok(parquet::NormalizeCatalogBaseUri("http://localhost:8181/catalog/v1/") ==
         "http://localhost:8181/catalog",
     "catalog base URI normalization accepts versioned REST roots");
}

static void test_lakekeeper_warehouse_name_lookup()
{
  const std::string payload =
      R"json({"warehouses":[{"id":"11111111-1111-1111-1111-111111111111","warehouse-id":"11111111-1111-1111-1111-111111111111","name":"demo"},{"id":"34222c1a-3c39-11f1-8407-8f978f046b38","warehouse-id":"34222c1a-3c39-11f1-8407-8f978f046b38","name":"bucket_change_1"}]})json";
  std::string warehouse_name;
  ok(parquet::FindLakekeeperWarehouseNameById(
         payload, "34222c1a-3c39-11f1-8407-8f978f046b38",
         &warehouse_name) &&
         warehouse_name == "bucket_change_1",
     "LakeKeeper warehouse management response maps UUID to warehouse name");
}

static void test_catalog_capability_defaults()
{
  auto capabilities= parquet::ResolveCatalogCapabilities({});
  ok(capabilities.supports_create_table &&
         capabilities.supports_commit_table &&
         capabilities.supports_commit_transaction &&
         capabilities.supports_register_table &&
         !capabilities.supports_scan_planning,
     "empty advertised endpoint list falls back to core REST defaults");
}

static void test_catalog_capability_scan_planning()
{
  auto capabilities= parquet::ResolveCatalogCapabilities(
      {"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan"});
  ok(capabilities.supports_scan_planning &&
         !capabilities.supports_create_table,
     "scan planning capability is detected from advertised endpoints");
}

static void test_namespace_path_encoding()
{
  parquet::CatalogNamespaceIdent ident{{"sales data", "west"}};
  ok(parquet::EncodeNamespaceForUrlPath(ident, "%1F") ==
         "sales%20data%1Fwest",
     "namespace path encoding preserves the configured separator");
}

static void test_object_location_resolution_path_style()
{
  parquet::ObjectStoreConfig config;
  config.endpoint= "https://s3.us-east-1.amazonaws.com/";
  config.bucket= "warehouse";
  config.key_prefix= "/iceberg/tables/";

  auto location= parquet::ResolveObjectLocation(
      config, "/db/users/part-1.parquet");
  ok(location.bucket == "warehouse" &&
         location.key == "iceberg/tables/db/users/part-1.parquet" &&
         location.url ==
             "https://s3.us-east-1.amazonaws.com/warehouse/"
             "iceberg/tables/db/users/part-1.parquet",
     "path-style object locations normalize bucket, prefix, and URL");
}

static void test_object_location_resolution_virtual_host()
{
  parquet::ObjectStoreConfig config;
  config.endpoint= "https://minio.local:9000/base";
  config.bucket= "lake";
  config.url_style= "virtual-host";

  auto location= parquet::ResolveObjectLocation(
      config, "ns/table/data file.parquet");
  ok(location.bucket == "lake" &&
         location.key == "ns/table/data file.parquet" &&
         location.url ==
             "https://lake.minio.local:9000/base/ns/table/data%20file.parquet",
     "virtual-host object locations put the bucket into the authority");
}

static void test_parse_s3_uri()
{
  parquet::ObjectLocation location;
  ok(parquet::ParseS3Uri("s3://warehouse/db/users/part-1.parquet",
                         &location) &&
         location.bucket == "warehouse" &&
         location.key == "db/users/part-1.parquet" &&
         location.url.empty(),
     "S3 URI parsing extracts bucket and key without inventing a URL");
}

static void test_parse_key_value_options()
{
  std::map<std::string, std::string> options;
  std::string error;
  ok(parquet::ParseKeyValueOptions(
         "endpoint=https://minio.local:9000; bucket=warehouse ; region=us-east-1",
         &options, &error) &&
         options["endpoint"] == "https://minio.local:9000" &&
         options["bucket"] == "warehouse" &&
         options["region"] == "us-east-1",
     "key=value option parsing trims whitespace and lowercases keys");
}

static void test_parse_object_store_connection()
{
  parquet::ObjectStoreConfig config;
  std::string error;
  ok(parquet::ParseObjectStoreConnectionString(
         "endpoint=https://minio.local:9000;bucket=warehouse;region=us-east-1;"
         "key_prefix=iceberg/db/t1;url_style=virtual-host",
         &config, &error) &&
         config.endpoint == "https://minio.local:9000" &&
         config.bucket == "warehouse" &&
         config.key_prefix == "iceberg/db/t1" &&
         config.url_style == "virtual-host" &&
         config.credentials.empty(),
     "object store connection parsing accepts non-secret routing options");
}

static void test_parse_object_store_connection_rejects_secrets()
{
  parquet::ObjectStoreConfig config;
  std::string error;
  ok(!parquet::ParseObjectStoreConnectionString(
         "endpoint=https://minio.local:9000;bucket=warehouse;"
         "access_key_id=minio",
         &config, &error) &&
         error.find("must not include access_key_id") != std::string::npos,
     "object store connection parsing rejects secrets in DDL");
}

static void test_parse_catalog_connection()
{
  parquet::CatalogClientConfig config;
  parquet::CatalogTableIdent ident;
  std::string access_delegation;
  std::string error;
  auto local_paths= parquet::ResolveLocalPaths("/tmp/test_db/users");

  ok(parquet::ParseCatalogConnectionString(
         "uri=http://127.0.0.1:8181/catalog;warehouse=warehouse;"
         "namespace=analytics.stage1;table=users_iceberg;"
         "access_delegation=vended-credentials",
         local_paths, &config, &ident, &access_delegation, &error) &&
         config.base_uri == "http://127.0.0.1:8181/catalog" &&
         config.warehouse == "warehouse" &&
         ident.namespace_ident.parts.size() == 2 &&
         ident.namespace_ident.parts[0] == "analytics" &&
         ident.namespace_ident.parts[1] == "stage1" &&
         ident.table_name == "users_iceberg" &&
         access_delegation == "vended-credentials",
     "catalog connection parsing resolves table identity and delegation");
}

static void test_parse_catalog_connection_rejects_bearer_token()
{
  parquet::CatalogClientConfig config;
  parquet::CatalogTableIdent ident;
  std::string access_delegation;
  std::string error;
  auto local_paths= parquet::ResolveLocalPaths("/tmp/test_db/users");

  ok(!parquet::ParseCatalogConnectionString(
         "uri=http://127.0.0.1:8181/catalog;bearer_token=secret", local_paths,
         &config, &ident, &access_delegation, &error) &&
         error.find("must not include bearer_token") != std::string::npos,
     "catalog connection parsing rejects secrets in DDL");
}

static void test_build_s3_uri_and_absolute_location()
{
  parquet::ObjectStoreConfig config;
  config.endpoint= "https://minio.local:9000";
  config.url_style= "path";

  auto location= parquet::ResolveAbsoluteObjectLocation(
      config, "warehouse", "iceberg/db/users/part-1.parquet");
  ok(parquet::BuildS3Uri("warehouse", "iceberg/db/users/part-1.parquet") ==
         "s3://warehouse/iceberg/db/users/part-1.parquet" &&
         location.url ==
             "https://minio.local:9000/warehouse/"
             "iceberg/db/users/part-1.parquet",
     "absolute object resolution keeps bucket and full object key intact");
}

static void test_configured_table_location_uri()
{
  parquet::ObjectStoreConfig config;
  config.endpoint = "https://minio.local:9000";
  config.bucket = "warehouse";
  config.key_prefix = "/configured/root/";

  std::string location_uri;
  std::string error;
  ok(parquet::BuildConfiguredTableLocationUri(config, &location_uri, &error) &&
         location_uri == "s3://warehouse/configured/root",
     "configured table location URI is derived from object store root");
}

static void test_catalog_load_reconciles_table_location()
{
  parquet::TableMetadata metadata;
  metadata.object_store_config.endpoint = "https://minio.local:9000";
  metadata.object_store_config.bucket = "configured-bucket";
  metadata.object_store_config.key_prefix = "configured/root";

  parquet::CatalogLoadTableResult load_result;
  load_result.metadata.table_uuid = "table-uuid";
  load_result.metadata.table_location = "s3://catalog-bucket/catalog/root/";

  std::string error;
  ok(parquet::ApplyCatalogLoadResult(&metadata, load_result, &error) &&
         metadata.table_uuid == "table-uuid" &&
         metadata.table_location == "s3://catalog-bucket/catalog/root" &&
         metadata.object_store_config.bucket == "catalog-bucket" &&
         metadata.object_store_config.key_prefix == "catalog/root" &&
         metadata.object_store_config.endpoint == "https://minio.local:9000",
     "catalog load reconciles object store root with table location");
}

static void test_runtime_table_location_reconciles_sidecar()
{
  PluginConfigGuard guard;
  set_test_plugin_config();

  parquet::TableMetadata metadata;
  metadata.local_paths = parquet::ResolveLocalPaths("/tmp/runtime_location");
  metadata.metadata_file_path =
      parquet::ResolveMetadataFilePath(metadata.local_paths.table_path.c_str());
  metadata.catalog_enabled = true;
  metadata.object_store_enabled = true;
  metadata.catalog_config.base_uri = "http://custom/catalog/v1/";
  metadata.catalog_table_ident.table_name = "runtime_location";
  metadata.object_store_config.endpoint = "https://minio.local:9000";
  metadata.object_store_config.bucket = "configured-bucket";
  metadata.object_store_config.key_prefix = "configured/root";
  metadata.table_location = "s3://catalog-bucket/catalog/root";

  std::string error;
  ok(parquet::SaveTableMetadata(metadata, &error),
     "stale table-location sidecar saves before reconciliation");

  parquet::TableMetadata resolved;
  ok(parquet::ResolveRuntimeTableMetadata(
         metadata.local_paths.table_path.c_str(), &resolved, &error) &&
         resolved.table_location == "s3://catalog-bucket/catalog/root" &&
         resolved.object_store_config.bucket == "catalog-bucket" &&
         resolved.object_store_config.key_prefix == "catalog/root" &&
         resolved.object_store_config.credentials.access_key_id == "minio",
     "runtime metadata reconciles stale object store root from table location");

  std::remove(metadata.metadata_file_path.c_str());
}

static void test_table_object_location_prefers_table_location()
{
  parquet::TableMetadata metadata;
  metadata.object_store_config.endpoint = "https://minio.local:9000";
  metadata.object_store_config.bucket = "configured-bucket";
  metadata.object_store_config.key_prefix = "configured/root";
  metadata.table_location = "s3://catalog-bucket/catalog/root";

  parquet::ObjectLocation location;
  std::string error;
  ok(parquet::ResolveTableObjectLocation(
         metadata, "data/part-1.parquet", &location, &error) &&
         location.bucket == "catalog-bucket" &&
         location.key == "catalog/root/data/part-1.parquet" &&
         location.url ==
             "https://minio.local:9000/catalog-bucket/"
             "catalog/root/data/part-1.parquet",
     "table object location prefers catalog table location");

  metadata.table_location.clear();
  ok(parquet::ResolveTableObjectLocation(
         metadata, "data/part-1.parquet", &location, &error) &&
         location.bucket == "configured-bucket" &&
         location.key == "configured/root/data/part-1.parquet",
     "table object location falls back to configured object root");
}

static void test_table_location_rejects_non_s3()
{
  parquet::TableMetadata metadata;
  metadata.table_location = "file:///tmp/table";

  std::string error;
  ok(!parquet::ReconcileObjectStoreConfigWithTableLocation(&metadata, &error) &&
         error.find("s3:// URI") != std::string::npos,
     "non-S3 table locations are rejected before object writes");
}

static void test_iceberg_manifest_locations_use_table_location()
{
  parquet::TableMetadata table_metadata;
  table_metadata.local_paths =
      parquet::ResolveLocalPaths("/tmp/iceberg_location_root");
  table_metadata.metadata_file_path = parquet::ResolveMetadataFilePath(
      table_metadata.local_paths.table_path.c_str());
  table_metadata.catalog_enabled = true;
  table_metadata.object_store_enabled = true;
  table_metadata.object_store_config.endpoint = "https://minio.local:9000";
  table_metadata.object_store_config.bucket = "configured-bucket";
  table_metadata.object_store_config.key_prefix = "configured/root";
  table_metadata.table_uuid = "c3163f0d-b617-4d47-bfab-8f5312fdc810";
  table_metadata.table_location = "s3://warehouse/catalog/root";

  parquet::CatalogLoadTableResult load_result;
  load_result.metadata.table_uuid = table_metadata.table_uuid;
  load_result.metadata.format_version = 2;
  load_result.metadata.raw_metadata_json =
      R"json({
        "format-version": 2,
        "table-uuid": "c3163f0d-b617-4d47-bfab-8f5312fdc810",
        "current-schema-id": 0,
        "schemas": [{
          "type": "struct",
          "schema-id": 0,
          "identifier-field-ids": [],
          "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        }],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-sequence-number": 0
      })json";

  std::vector<parquet::ParquetStagedFile> staged_files = {{
      "/tmp/iceberg_location_root",
      "users",
      "/tmp/iceberg_location_root.stage_1.parquet",
      "s3://warehouse/catalog/root/data/flush_1.parquet",
      3,
      128,
      1,
  }};

  parquet::IcebergCommitArtifacts artifacts;
  std::string error;
  ok(parquet::BuildIcebergCommitArtifacts(table_metadata, load_result,
                                          staged_files, &artifacts, &error) &&
         artifacts.manifest_location.bucket == "warehouse" &&
         artifacts.manifest_location.key.rfind("catalog/root/metadata/", 0) ==
             0 &&
         artifacts.manifest_list_location.bucket == "warehouse" &&
         artifacts.manifest_list_location.key.rfind(
             "catalog/root/metadata/", 0) == 0,
     "Iceberg manifest locations use catalog table location");

  std::remove(artifacts.manifest_local_path.c_str());
  std::remove(artifacts.manifest_list_local_path.c_str());
}

static void test_metadata_roundtrip()
{
  parquet::TableMetadata metadata;
  metadata.local_paths= parquet::ResolveLocalPaths("/tmp/users_roundtrip");
  metadata.metadata_file_path=
      parquet::ResolveMetadataFilePath(metadata.local_paths.table_path.c_str());
  metadata.catalog_enabled= true;
  metadata.object_store_enabled= true;
  metadata.catalog_config.base_uri= "http://127.0.0.1:8181/catalog";
  metadata.catalog_config.warehouse= "warehouse";
  metadata.catalog_table_ident.namespace_ident.parts= {"analytics"};
  metadata.catalog_table_ident.table_name= "users";
  metadata.access_delegation= "vended-credentials";
  metadata.object_store_config.endpoint= "https://minio.local:9000";
  metadata.object_store_config.bucket= "warehouse";
  metadata.object_store_config.key_prefix= "iceberg/analytics/users";
  metadata.catalog_config.bearer_token= "secret-token";
  metadata.object_store_config.credentials.access_key_id= "minio";
  metadata.object_store_config.credentials.secret_access_key= "secret";
  metadata.table_uuid= "9d4796f7-3c97-4f4f-b1af-3b87b77b4d53";
  metadata.table_location= "s3://warehouse/iceberg/analytics/users";
  metadata.current_snapshot_id= "12345";
  metadata.raw_catalog_metadata_json= R"json({"format-version":2})json";
  metadata.active_files= {{
      "s3://warehouse/iceberg/analytics/users/data/flush_1.parquet",
      7,
      1024,
      "12345",
      3,
      3,
  }};
  metadata.active_scan_paths= {
      "s3://warehouse/iceberg/analytics/users/data/flush_1.parquet"};

  std::string error;
  ok(parquet::SaveTableMetadata(metadata, &error),
     "table metadata saves to the sidecar file");

  parquet::TableMetadata loaded;
  ok(parquet::LoadTableMetadata(metadata.local_paths.table_path.c_str(),
                                &loaded, &error) &&
         loaded.object_store_enabled &&
         loaded.catalog_enabled &&
         loaded.catalog_table_ident.table_name == "users" &&
         loaded.table_uuid == "9d4796f7-3c97-4f4f-b1af-3b87b77b4d53" &&
         loaded.current_snapshot_id == "12345" &&
         loaded.active_files.size() == 1 &&
         loaded.active_files[0].record_count == 7 &&
         loaded.raw_catalog_metadata_json == R"json({"format-version":2})json" &&
         loaded.catalog_config.bearer_token.empty() &&
         loaded.object_store_config.credentials.empty() &&
         loaded.active_scan_paths.size() == 1 &&
         loaded.active_scan_paths[0] ==
             "s3://warehouse/iceberg/analytics/users/data/flush_1.parquet",
     "table metadata round-trips through the JSON sidecar without secrets");

  std::remove(metadata.metadata_file_path.c_str());
}

static void test_metadata_sidecar_exists()
{
  const char *table_path = "/tmp/parquet_sidecar_exists";
  const std::string metadata_path =
      parquet::ResolveMetadataFilePath(table_path);
  std::remove(metadata_path.c_str());

  const bool absent_before_create = !parquet::MetadataSidecarExists(table_path);
  FILE *sidecar = std::fopen(metadata_path.c_str(), "wb");
  if (sidecar != nullptr) {
    std::fclose(sidecar);
  }
  const bool present_after_create = parquet::MetadataSidecarExists(table_path);
  std::remove(metadata_path.c_str());

  ok(absent_before_create && sidecar != nullptr && present_after_create,
     "metadata sidecar existence helper distinguishes absent and present sidecars");
}

static void test_resolve_create_table_metadata_merges_defaults()
{
  PluginConfigGuard guard;
  set_test_plugin_config();

  parquet::TableMetadata metadata;
  std::string error;
  ok(parquet::ResolveCreateTableMetadata(
         "/tmp/test_db/users",
         "table=users_iceberg;namespace=analytics.custom",
         "endpoint=https://minio.local:9000;url_style=virtual-host",
         &metadata, &error) &&
         metadata.catalog_config.base_uri ==
             "http://127.0.0.1:8181/catalog/v1/" &&
         metadata.catalog_config.warehouse == "warehouse-123" &&
         metadata.catalog_table_ident.namespace_ident.parts.size() == 2 &&
         metadata.catalog_table_ident.namespace_ident.parts[0] == "analytics" &&
         metadata.catalog_table_ident.namespace_ident.parts[1] == "custom" &&
         metadata.catalog_table_ident.table_name == "users_iceberg" &&
         metadata.object_store_config.endpoint == "https://minio.local:9000" &&
         metadata.object_store_config.bucket == "warehouse" &&
         metadata.object_store_config.key_prefix == "iceberg/analytics/users" &&
         metadata.object_store_config.credentials.access_key_id == "minio" &&
         metadata.object_store_config.credentials.secret_access_key ==
             "secret",
     "create metadata resolution merges global defaults and DDL overrides");
}

static void test_runtime_defaults_merge()
{
  PluginConfigGuard guard;
  set_test_plugin_config();

  parquet::TableMetadata metadata;
  metadata.local_paths = parquet::ResolveLocalPaths("/tmp/runtime_merge");
  metadata.metadata_file_path =
      parquet::ResolveMetadataFilePath(metadata.local_paths.table_path.c_str());
  metadata.catalog_enabled = true;
  metadata.object_store_enabled = true;
  metadata.catalog_config.base_uri = "http://custom/catalog/v1/";
  metadata.catalog_table_ident.table_name = "runtime_merge";
  metadata.object_store_config.endpoint = "https://minio.local:9000";

  std::string error;
  ok(parquet::SaveTableMetadata(metadata, &error),
     "partial runtime metadata saves before default injection");

  parquet::TableMetadata resolved;
  ok(parquet::ResolveRuntimeTableMetadata(
         metadata.local_paths.table_path.c_str(), &resolved, &error) &&
         resolved.catalog_config.base_uri == "http://custom/catalog/v1/" &&
         resolved.catalog_config.warehouse == "warehouse-123" &&
         resolved.catalog_table_ident.namespace_ident.parts.size() == 2 &&
         resolved.catalog_table_ident.namespace_ident.parts[0] == "analytics" &&
         resolved.catalog_table_ident.namespace_ident.parts[1] == "stage" &&
         resolved.object_store_config.bucket == "warehouse" &&
         resolved.object_store_config.endpoint == "https://minio.local:9000" &&
         resolved.object_store_config.region == "us-east-1" &&
         resolved.object_store_config.credentials.access_key_id == "minio" &&
         resolved.object_store_config.credentials.secret_access_key ==
             "secret",
     "runtime metadata resolution fills missing non-secrets and injects secrets");

  std::remove(metadata.metadata_file_path.c_str());
}

static void test_extract_active_scan_paths()
{
  std::vector<parquet::ActiveDataFile> active_files = {{
      "s3://warehouse/db/t1/data/part-1.parquet", 3, 111, "7", 4, 4},
      {"s3://warehouse/db/t1/data/part-2.parquet", 5, 222, "8", 5, 5},
  };
  auto paths = parquet::ExtractActiveScanPaths(active_files);
  ok(paths.size() == 2 &&
         paths[0] == "s3://warehouse/db/t1/data/part-1.parquet" &&
         paths[1] == "s3://warehouse/db/t1/data/part-2.parquet",
     "active scan paths are derived from active file lineage");
}

static void test_resolve_scan_paths_from_sidecar()
{
  parquet::TableMetadata metadata;
  metadata.active_scan_paths = {
      "s3://warehouse/db/t1/data/part-1.parquet",
      "s3://warehouse/db/t1/data/part-2.parquet"};

  std::vector<std::string> paths;
  std::string error;
  ok(resolve_parquet_scan_paths(&metadata, &paths, &error) &&
         paths.size() == 2 &&
         paths[0] == "s3://warehouse/db/t1/data/part-1.parquet" &&
         paths[1] == "s3://warehouse/db/t1/data/part-2.parquet",
     "scan path resolver reads active_scan_paths without catalog fallback");
}

static void test_resolve_scan_paths_from_active_files()
{
  parquet::TableMetadata metadata;
  metadata.current_snapshot_id = "8";
  metadata.active_files = {{
      "s3://warehouse/db/t1/data/part-1.parquet", 3, 111, "8", 4, 4},
      {"s3://warehouse/db/t1/data/part-2.parquet", 5, 222, "8", 5, 5},
  };

  std::vector<std::string> paths;
  std::string error;
  ok(resolve_parquet_scan_paths(&metadata, &paths, &error) &&
         paths.size() == 2 &&
         paths[0] == "s3://warehouse/db/t1/data/part-1.parquet" &&
         paths[1] == "s3://warehouse/db/t1/data/part-2.parquet" &&
         metadata.active_scan_paths == paths,
     "scan path resolver derives paths from active file lineage");
}

static void test_resolve_scan_paths_empty_table()
{
  parquet::TableMetadata metadata;

  std::vector<std::string> paths;
  std::string error;
  ok(resolve_parquet_scan_paths(&metadata, &paths, &error) &&
         paths.empty(),
     "scan path resolver treats sidecars without snapshots as empty tables");
}

static void test_legacy_manifest_list_extraction()
{
  auto paths = extract_legacy_fake_manifest_list_scan_paths(
      R"json({"metadata":{"snapshots":[{"manifest-list":"s3://warehouse/db/t1/data/legacy.parquet"}]}})json");
  ok(paths.size() == 1 &&
         paths[0] == "s3://warehouse/db/t1/data/legacy.parquet",
     "legacy fake manifest-list extraction remains isolated outside handler");

  paths = extract_legacy_fake_manifest_list_scan_paths(
      R"json({"metadata":{"snapshots":[{"manifest-list":"s3://warehouse/db/t1/metadata/snap-1.avro"}]}})json");
  ok(paths.empty(),
     "legacy fake manifest-list extraction ignores real Iceberg Avro manifest lists");
}

static void test_resolve_scan_paths_from_legacy_raw_metadata()
{
  parquet::TableMetadata metadata;
  metadata.local_paths = parquet::ResolveLocalPaths("/tmp/parquet_legacy_raw");
  metadata.metadata_file_path =
      parquet::ResolveMetadataFilePath(metadata.local_paths.table_path.c_str());
  metadata.current_snapshot_id = "7";
  metadata.raw_catalog_metadata_json =
      R"json({"metadata":{"snapshots":[{"manifest-list":"s3://warehouse/db/t1/data/legacy.parquet"}]}})json";
  std::remove(metadata.metadata_file_path.c_str());

  std::vector<std::string> paths;
  std::string error;
  ok(resolve_parquet_scan_paths(&metadata, &paths, &error) &&
         paths.size() == 1 &&
         paths[0] == "s3://warehouse/db/t1/data/legacy.parquet" &&
         metadata.active_scan_paths == paths &&
         metadata.active_files.size() == 1 &&
         metadata.active_files[0].snapshot_id == "7",
     "scan path resolver migrates legacy raw metadata from the sidecar");

  parquet::TableMetadata loaded;
  ok(parquet::LoadTableMetadata(metadata.local_paths.table_path.c_str(),
                                &loaded, &error) &&
         loaded.active_scan_paths.size() == 1 &&
         loaded.active_scan_paths[0] ==
             "s3://warehouse/db/t1/data/legacy.parquet",
     "legacy scan path migration persists active_scan_paths");
  std::remove(metadata.metadata_file_path.c_str());
}

static void test_resolve_scan_paths_rejects_missing_lineage()
{
  parquet::TableMetadata metadata;
  metadata.current_snapshot_id = "9";
  metadata.raw_catalog_metadata_json =
      R"json({"current-snapshot-id":9,"snapshots":[{"snapshot-id":9,"summary":{"operation":"append","total-data-files":"1","total-records":"3"},"manifest-list":"s3://warehouse/db/t1/metadata/snap-9.avro"}]})json";

  std::vector<std::string> paths;
  std::string error;
  ok(!resolve_parquet_scan_paths(&metadata, &paths, &error) &&
         error.find("missing active file cache") != std::string::npos,
     "scan path resolver rejects non-empty snapshots without sidecar lineage");
}

static void test_create_cleanup_removes_local_artifacts()
{
  parquet::TableMetadata metadata;
  metadata.local_paths = parquet::ResolveLocalPaths("/tmp/parquet_create_cleanup");
  metadata.metadata_file_path =
      parquet::ResolveMetadataFilePath(metadata.local_paths.table_path.c_str());

  {
    std::ofstream parquet_file(metadata.local_paths.parquet_file_path,
                               std::ios::binary | std::ios::trunc);
    parquet_file << "stale parquet seed";
  }
  {
    std::ofstream sidecar_file(metadata.metadata_file_path,
                              std::ios::binary | std::ios::trunc);
    sidecar_file << "{}";
  }

  parquet::RemoveCreateLocalArtifacts(metadata);

  std::ifstream parquet_file(metadata.local_paths.parquet_file_path,
                             std::ios::binary);
  std::ifstream sidecar_file(metadata.metadata_file_path, std::ios::binary);
  ok(!parquet_file.good() && !sidecar_file.good(),
     "create cleanup removes local seed and sidecar artifacts");
}

static void test_build_iceberg_commit_artifacts()
{
  parquet::TableMetadata table_metadata;
  table_metadata.local_paths = parquet::ResolveLocalPaths("/tmp/iceberg_users");
  table_metadata.metadata_file_path =
      parquet::ResolveMetadataFilePath(table_metadata.local_paths.table_path.c_str());
  table_metadata.catalog_enabled = true;
  table_metadata.object_store_enabled = true;
  table_metadata.catalog_table_ident.namespace_ident.parts = {"analytics"};
  table_metadata.catalog_table_ident.table_name = "users";
  table_metadata.object_store_config.endpoint = "https://minio.local:9000";
  table_metadata.object_store_config.bucket = "warehouse";
  table_metadata.object_store_config.key_prefix = "iceberg/analytics/users";
  table_metadata.table_uuid = "c3163f0d-b617-4d47-bfab-8f5312fdc810";
  table_metadata.table_location = "s3://warehouse/iceberg/analytics/users";

  parquet::CatalogLoadTableResult load_result;
  load_result.metadata.table_uuid = table_metadata.table_uuid;
  load_result.metadata.format_version = 2;
  load_result.metadata.raw_metadata_json =
      R"json({
        "format-version": 2,
        "table-uuid": "c3163f0d-b617-4d47-bfab-8f5312fdc810",
        "current-schema-id": 0,
        "schemas": [{
          "type": "struct",
          "schema-id": 0,
          "identifier-field-ids": [],
          "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
        }],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-sequence-number": 0
      })json";

  std::vector<parquet::ParquetStagedFile> staged_files = {{
      "/tmp/iceberg_users",
      "users",
      "/tmp/iceberg_users.stage_1.parquet",
      "s3://warehouse/iceberg/analytics/users/data/flush_1.parquet",
      3,
      128,
      1,
  }};

  parquet::IcebergCommitArtifacts artifacts;
  std::string error;
  ok(parquet::BuildIcebergCommitArtifacts(table_metadata, load_result,
                                          staged_files, &artifacts, &error) &&
         artifacts.snapshot_id != 0 &&
         artifacts.sequence_number == 1 &&
         artifacts.active_files.size() == 1 &&
         artifacts.active_files[0].snapshot_id ==
             std::to_string(artifacts.snapshot_id) &&
         artifacts.commit_request_json.find("\"action\":\"add-snapshot\"") !=
             std::string::npos &&
         artifacts.commit_request_json.find("\"ref-name\":\"main\"") !=
             std::string::npos,
     "Iceberg commit artifacts include manifests, lineage, and commit updates");

  table_metadata.current_snapshot_id = std::to_string(artifacts.snapshot_id);
  table_metadata.active_files = artifacts.active_files;
  table_metadata.active_scan_paths =
      parquet::ExtractActiveScanPaths(table_metadata.active_files);
  ok(parquet::SaveTableMetadata(table_metadata, &error),
     "committed Iceberg metadata saves active file lineage to the sidecar");

  parquet::TableMetadata committed_metadata;
  ok(parquet::LoadTableMetadata(table_metadata.local_paths.table_path.c_str(),
                                &committed_metadata, &error) &&
         committed_metadata.current_snapshot_id ==
             std::to_string(artifacts.snapshot_id) &&
         committed_metadata.active_files.size() == 1 &&
         committed_metadata.active_scan_paths.size() == 1,
     "committed Iceberg metadata reloads active file lineage from the sidecar");

  ok(std::remove(artifacts.manifest_local_path.c_str()) == 0 &&
         std::remove(artifacts.manifest_list_local_path.c_str()) == 0,
     "temporary Iceberg manifest artifacts can be cleaned up locally");
  std::remove(table_metadata.metadata_file_path.c_str());
}

int main()
{
  plan(55);

  test_build_query_basic_schema();
  test_build_query_blob_mapping();
  test_build_query_bit_mapping();
  test_store_duckdb_boolean_in_bit_field();
  test_build_iceberg_schema_json();
  test_local_path_resolution();
  test_staged_file_helpers();
  test_transaction_state_validation();
  test_stage_path_helpers();
  test_catalog_base_uri_normalization();
  test_lakekeeper_warehouse_name_lookup();
  test_catalog_capability_defaults();
  test_catalog_capability_scan_planning();
  test_namespace_path_encoding();
  test_object_location_resolution_path_style();
  test_object_location_resolution_virtual_host();
  test_parse_s3_uri();
  test_parse_key_value_options();
  test_parse_object_store_connection();
  test_parse_object_store_connection_rejects_secrets();
  test_parse_catalog_connection();
  test_parse_catalog_connection_rejects_bearer_token();
  test_build_s3_uri_and_absolute_location();
  test_configured_table_location_uri();
  test_catalog_load_reconciles_table_location();
  test_runtime_table_location_reconciles_sidecar();
  test_table_object_location_prefers_table_location();
  test_table_location_rejects_non_s3();
  test_iceberg_manifest_locations_use_table_location();
  test_metadata_roundtrip();
  test_metadata_sidecar_exists();
  test_resolve_create_table_metadata_merges_defaults();
  test_runtime_defaults_merge();
  test_extract_active_scan_paths();
  test_resolve_scan_paths_from_sidecar();
  test_resolve_scan_paths_from_active_files();
  test_resolve_scan_paths_empty_table();
  test_legacy_manifest_list_extraction();
  test_resolve_scan_paths_from_legacy_raw_metadata();
  test_resolve_scan_paths_rejects_missing_lineage();
  test_create_cleanup_removes_local_artifacts();
  test_build_iceberg_commit_artifacts();

  return exit_status();
}
