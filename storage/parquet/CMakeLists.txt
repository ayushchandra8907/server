#parquet  build config 

FIND_PACKAGE(CURL REQUIRED)

if(NOT TARGET libduckdb)
  MESSAGE(FATAL_ERROR
    "Parquet storage engine requires DuckDB target 'libduckdb'. "
    "Make sure the DuckDB submodule is initialized and configured "
    "(for example: git submodule update --init --recursive storage/duckdb/duckdb).")
endif()

if(TARGET mytap)
  ADD_EXECUTABLE(parquet_test EXCLUDE_FROM_ALL
    parquet_test.cc
    parquet_catalog.cc
    parquet_iceberg.cc
    parquet_metadata.cc
    parquet_object_store.cc
    parquet_schema.cc
    parquet_shared.cc
    parquet_transaction.cc
    ${CMAKE_SOURCE_DIR}/unittest/sql/dummy_builtins.cc
  )
  target_include_directories(parquet_test PRIVATE
    ${CMAKE_SOURCE_DIR}/storage/columnstore/columnstore/utils/json
    ${CMAKE_SOURCE_DIR}/storage/duckdb/duckdb
    ${CMAKE_SOURCE_DIR}/storage/duckdb/duckdb/third_parties/duckdb/src/include
  )
  target_link_libraries(parquet_test
    sql
    mytap
    libduckdb
    CURL::libcurl
  )
endif()

MYSQL_ADD_PLUGIN(parquet
  ha_parquet.cc
  ha_parquet_pushdown.cc
  parquet_cross_engine_scan.cc
  parquet_duckdb.cc
  parquet_shared.cc
  parquet_schema.cc
  parquet_catalog.cc
  parquet_create.cc
  parquet_iceberg.cc
  parquet_metadata.cc
  parquet_object_store.cc
  parquet_transaction.cc
  parquet_write_buffer.cc
  STORAGE_ENGINE
)

if(TARGET parquet)
  target_include_directories(parquet PRIVATE
    ${CMAKE_SOURCE_DIR}/storage/columnstore/columnstore/utils/json
    ${CMAKE_SOURCE_DIR}/storage/duckdb/duckdb
    ${CMAKE_SOURCE_DIR}/storage/duckdb/duckdb/third_parties/duckdb/src/include
  )
  target_link_libraries(parquet
    libduckdb
    CURL::libcurl
  )
endif()
