/**
 * @file ha_parquet.h
 * @brief MariaDB Parquet Storage Engine Handler
 *
 * This storage engine allows MariaDB to read and write Parquet files stored
 * in S3-compatible object storage. DuckDB is used as the local query execution
 * engine for both reads and write buffering. LakeKeeper serves as the Iceberg
 * REST catalog, tracking table snapshots and data file locations.
 *
 * Write path:
 *   Rows are buffered into a per-statement DuckDB in-memory table, staged as
 *   local Parquet files at statement end, flushed to S3 at transaction commit,
 *   and registered with LakeKeeper as a new Iceberg snapshot.
 *
 * Read path:
 *   On scan init, data file paths are fetched from LakeKeeper. DuckDB reads
 *   those S3 Parquet files directly, optionally applying pushed-down WHERE
 *   conditions.
 */

#ifndef HA_PARQUET_INCLUDED
#define HA_PARQUET_INCLUDED
#define MYSQL_SERVER 1
#include "handler.h"
#include "thr_lock.h"
#include "my_base.h"
#include "duckdb.hpp"
#include <cstdint>
#include <map>
#include <vector>
#include <string>

/**
 * @struct parquet_local_stage_file
 * @brief Tracks a single locally staged Parquet file produced at statement end.
 *
 * When a write statement completes, the DuckDB in-memory buffer is exported
 * to a temporary local Parquet file. Each such file is recorded here until
 * transaction commit, at which point all staged files are merged and uploaded
 * to S3 in a single COPY operation.
 */
struct parquet_local_stage_file {
  /** @brief Absolute path to the temporary local Parquet file */
  std::string local_path;
  /** @brief Number of rows written into this staged file */
  uint64_t    row_count = 0;
};

/**
 * @struct parquet_table_trx_data
 * @brief Per-table write state for the duration of a transaction.
 *
 * Tracks everything needed to buffer, stage, and commit writes to a single
 * Parquet-backed table within one MariaDB transaction. One instance exists
 * per table touched by the transaction, stored in parquet_trx_data::tables.
 */
struct parquet_table_trx_data {
  /** @brief MariaDB table name (unqualified) */
  std::string table_name;
  /** @brief Normalized MariaDB table path, used as the map key */
  std::string table_path;
  /**
   * @brief Name of the DuckDB in-memory staging table for the current statement.
   *
   * Scoped to a single statement (encodes thread ID, query ID, and table path
   * hash). Cleared and recreated when a new statement begins on the same table.
   */
  std::string statement_buffer_name;
  /** @brief Number of rows buffered into the current statement's DuckDB table */
  uint64_t    statement_row_count = 0;
  /**
   * @brief Local Parquet files staged from completed statements.
   *
   * Accumulates across statements within the transaction. All files are
   * merged into a single S3 object at commit time.
   */
  std::vector<parquet_local_stage_file> staged_files;
  /**
   * @brief S3 object paths uploaded during this transaction.
   *
   * Populated during commit. Retained so that a subsequent rollback can
   * delete the objects that were already written to S3.
   */
  std::vector<std::string> uploaded_s3_file_paths;
};

/**
 * @struct parquet_trx_data
 * @brief Top-level transaction state attached to a MariaDB session.
 *
 * Stored via thd_set_ha_data() and retrieved via thd_get_ha_data() using
 * the parquet handlerton. One instance lives for the duration of a transaction
 * and is freed (along with all per-table state) at commit or full rollback.
 */
struct parquet_trx_data {
  /**
   * @brief Per-table write state, keyed by normalized table path.
   *
   * Entries are created lazily the first time a table is written within
   * the transaction.
   */
  std::map<std::string, parquet_table_trx_data> tables;
};

/**
 * @class ha_parquet
 * @brief Handler class for the Parquet storage engine.
 *
 * Implements the MariaDB storage engine handler interface to provide
 * read/write access to Iceberg-format Parquet tables. Each instance
 * corresponds to one open table in one session.
 */
class ha_parquet final : public handler
{
public:
  /**
   * @brief Constructor — initializes the THR_LOCK slot and base handler.
   * @param hton       Handlerton pointer for this storage engine
   * @param table_arg  Table share for the table being opened
   */
  ha_parquet(handlerton *hton, TABLE_SHARE *table_arg);

  /** @brief Destructor */
  ~ha_parquet() override = default;

  /**
   * @brief Returns capability flags for this storage engine.
   *
   * Reports HA_FILE_BASED, indicating that table data is managed as
   * files rather than through a separate server process.
   */
  ulonglong table_flags() const override;

  /**
   * @brief Returns index capability flags.
   *
   * Always returns 0 — this engine does not support indexes.
   */
  ulong index_flags(uint idx, uint part, bool all_parts) const override;

  /**
   * @brief Open a table for use.
   *
   * Resolves parquet_file_path (table path + ".parquet") and helper_db_path
   * from the normalized table name. Does not establish a DuckDB connection;
   * connections are created per-operation inside each method.
   *
   * @param name             Normalized table path (without extension)
   * @param mode             Open mode flags (unused)
   * @param test_if_locked   Lock test flags (unused)
   * @return 0 on success, error code on failure
   */
  int open(const char *name, int mode, uint test_if_locked) override;

  /**
   * @brief Close the table.
   * @return Always returns 0
   */
  int close(void) override;

  /**
   * @brief Create a new Parquet-backed table.
   *
   * Performs three actions in sequence:
   *   1. Validates the CATALOG and CONNECTION table options and resolves
   *      full table metadata (object store config, catalog config).
   *   2. Uses DuckDB to create an in-memory staging table matching the
   *      MariaDB schema, then seeds an empty Parquet file via COPY ... TO.
   *   3. Registers the table in the LakeKeeper Iceberg catalog via HTTP POST,
   *      translating MariaDB column types to Iceberg field types.
   *
   * @param name         Normalized table path
   * @param table_arg    TABLE descriptor with column definitions
   * @param create_info  CREATE TABLE options, including CATALOG and CONNECTION
   * @return 0 on success, HA_ERR_UNSUPPORTED or HA_ERR_INTERNAL_ERROR on failure
   */
  int create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info) override;

  /**
   * @brief Delete a table.
   *
   * Sends an HTTP DELETE to LakeKeeper to deregister the table, then
   * removes the local .parquet and .parquet.meta files. Returns success
   * if LakeKeeper responds with 404 (table already absent).
   *
   * @param name  Normalized table path
   * @return 0 on success, HA_ERR_INTERNAL_ERROR on failure
   */
  int delete_table(const char *name) override;

  /**
   * @brief Buffer a single row for the current write statement.
   *
   * Retrieves or lazily creates the per-statement DuckDB in-memory staging
   * table (keyed by thread ID, query ID, and table path), then INSERTs the
   * supplied row into it. The buffer is flushed to a local Parquet file at
   * statement commit and uploaded to S3 at transaction commit.
   *
   * @param buf  MariaDB row buffer
   * @return 0 on success, HA_ERR_GENERIC or HA_ERR_INTERNAL_ERROR on failure
   */
  int write_row(const uchar *buf) override;

  /**
   * @brief Update is not supported by this engine.
   * @return Always returns HA_ERR_WRONG_COMMAND
   */
  int update_row(const uchar *old_data, const uchar *new_data) override;

  /**
   * @brief Row deletion is not supported by this engine.
   * @return Always returns HA_ERR_WRONG_COMMAND
   */
  int delete_row(const uchar *buf) override;

  /**
   * @brief Initialize a full table scan.
   *
   * Fetches the list of Parquet data file paths from LakeKeeper, then
   * executes a DuckDB SELECT * FROM read_parquet([...]) against those S3
   * files. If a condition was accepted by cond_push(), a WHERE clause is
   * appended before execution. The full result set is materialized into
   * scan_result for row-by-row retrieval via rnd_next().
   *
   * @param scan  True for a full sequential scan (always the case here)
   * @return 0 on success, HA_ERR_INTERNAL_ERROR on failure
   */
  int rnd_init(bool scan) override;

  /**
   * @brief Read the next row from the active scan result.
   *
   * Copies column values from the current DuckDB result row into the
   * MariaDB field buffers, advancing current_row. Type conversion is
   * performed per-column between DuckDB value types and MariaDB field types.
   *
   * @param buf  MariaDB row buffer to populate
   * @return 0 on success, HA_ERR_END_OF_FILE when all rows are consumed
   */
  int rnd_next(uchar *buf) override;

  /**
   * @brief Positional read is not supported by this engine.
   * @return Always returns HA_ERR_WRONG_COMMAND
   */
  int rnd_pos(uchar *buf, uchar *pos) override;

  /**
   * @brief Store the current row position.
   *
   * Not implemented — positional reads are unsupported.
   */
  void position(const uchar *record) override;

  /**
   * @brief Return table statistics to the optimizer.
   *
   * Not implemented — always returns 0.
   */
  int info(uint flag) override;

  /**
   * @brief Declare that in-place ALTER TABLE is never supported.
   * @return Always returns HA_ALTER_INPLACE_NOT_SUPPORTED
   */
  enum_alter_inplace_result check_if_supported_inplace_alter(
      TABLE *altered_table, Alter_inplace_info *ha_alter_info) override;

  /**
   * @brief Acquire or release a table-level lock and register with the
   *        MariaDB transaction system.
   *
   * Called by MariaDB when a statement opens (F_RDLCK / F_WRLCK) or
   * closes (F_UNLCK) a table. On any non-unlock call this method:
   *
   *   1. Calls trans_register_ha() for the statement-level transaction so
   *      that ha_parquet_commit / ha_parquet_rollback are invoked at
   *      statement boundaries.
   *   2. If the session is inside an explicit transaction
   *      (OPTION_NOT_AUTOCOMMIT or OPTION_BEGIN), also registers with the
   *      transaction-level handler so commit/rollback fire at the correct
   *      scope.
   *   3. On F_WRLCK specifically:
   *        - Validates that the table's catalog and object-store metadata
   *          can be resolved, failing fast before any rows are buffered.
   *        - Lazily initializes the per-session parquet_trx_data and the
   *          per-table parquet_table_trx_data tracking structures.
   *        - Allocates the statement buffer name for the DuckDB in-memory
   *          staging table that will accumulate rows for this statement.
   *
   * @param thd        Current thread/session
   * @param lock_type  F_RDLCK (read), F_WRLCK (write), or F_UNLCK (release)
   * @return 0 on success, HA_ERR_INTERNAL_ERROR if metadata validation fails
   */
  int external_lock(THD *thd, int lock_type) override;

  /**
   * @brief Assign a THR_LOCK slot for this handler instance.
   *
   * Called by MariaDB's lock manager. Stores the requested lock type in
   * the handler's THR_LOCK_DATA member and appends it to the lock array.
   *
   * @param thd        Current thread/session (unused)
   * @param to         Output array of lock data pointers
   * @param lock_type  Requested lock type
   * @return Pointer to the next free slot in the lock array
   */
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type) override;

  /**
   * @brief Push a WHERE condition down to the storage engine.
   *
   * Attempts to convert the supplied condition item into a SQL fragment
   * appended to the DuckDB read query. Only simple binary comparisons of
   * the form <column> <op> <constant> are supported (=, >, <). If accepted,
   * nullptr is returned to signal that MariaDB need not re-evaluate the
   * condition. Unsupported conditions are returned unchanged.
   *
   * @param cond  The WHERE condition item tree
   * @return nullptr if fully accepted; cond if the condition cannot be pushed
   */
  const Item *cond_push(const Item *cond) override;

  /**
   * @brief Discard the previously pushed condition.
   *
   * Clears pushed_cond_sql and resets has_pushed_cond to false.
   */
  void cond_pop() override;

private:
  /** @brief THR_LOCK slot used by MariaDB's table-level lock manager */
  THR_LOCK_DATA lock;

  /** @brief Path to the DuckDB helper database file */
  std::string helper_db_path;

  /** @brief Path to the local seed Parquet file (table path + ".parquet") */
  std::string parquet_file_path;

  /** @brief True once open() has successfully resolved file paths */
  bool duckdb_initialized;

  // @note: A single global DuckDB instance (g_duckdb) and per-operation
  // connections are used instead of persistent per-handler members.
  //duckdb::DuckDB *db = nullptr;
  //duckdb::Connection *con = nullptr;

  /** @brief SQL fragment from the most recently accepted cond_push() call */
  std::string pushed_cond_sql;

  /** @brief True when a condition has been accepted by cond_push() */
  bool has_pushed_cond = false;

  /**
   * @brief DuckDB result set held for the duration of a sequential scan.
   *
   * Populated by rnd_init() and consumed row-by-row by rnd_next().
   * Reset at the start of each rnd_init() call.
   */
  std::unique_ptr<duckdb::MaterializedQueryResult> scan_result;

  /** @brief Current row index into scan_result during a sequential scan */
  size_t current_row = 0;
};

#endif