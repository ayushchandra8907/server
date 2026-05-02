#define MYSQL_SERVER 1

#include "parquet_write_buffer.h"
#include "parquet_shared.h"

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace parquet
{

namespace
{

struct ParquetTableWriteBuffer {
  std::string table_path;
  std::string table_name;
  std::vector<ParquetBufferedFile> pending_files;
  uint64_t total_rows  = 0;
  uint64_t total_bytes = 0;
  std::chrono::steady_clock::time_point last_append_time;
};

std::mutex g_write_buffer_mutex;
std::unordered_map<std::string, ParquetTableWriteBuffer> g_write_buffers;

} // namespace

void ParquetWriteBufferInit()
{
  std::lock_guard<std::mutex> lock(g_write_buffer_mutex);
  g_write_buffers.clear();
  parquet_log_info("Parquet write buffer initialized");
}

void ParquetWriteBufferDeinit()
{
  std::lock_guard<std::mutex> lock(g_write_buffer_mutex);
  const size_t remaining = g_write_buffers.size();
  g_write_buffers.clear();
  if (remaining > 0) {
    parquet_log_warning("Parquet write buffer cleared " +
                        std::to_string(remaining) +
                        " unflushed table buffer(s) on shutdown");
  }
  parquet_log_info("Parquet write buffer deinitialized");
}

void ParquetWriteBufferAppend(const std::string &table_path,
                              const std::string &table_name,
                              const ParquetBufferedFile &file)
{
  parquet_log_info("Parquet write buffer append key='" + table_path + "'");
  std::lock_guard<std::mutex> lock(g_write_buffer_mutex);
  auto &buf = g_write_buffers[table_path];
  if (buf.table_path.empty()) {
    buf.table_path = table_path;
    buf.table_name = table_name;
  }
  buf.pending_files.push_back(file);
  buf.total_rows  += file.row_count;
  buf.total_bytes += file.file_size_bytes;
  buf.last_append_time = std::chrono::steady_clock::now();

  parquet_log_info("Parquet write buffer append table='" + table_name +
                   "' buffered_files=" +
                   std::to_string(buf.pending_files.size()) +
                   " buffered_rows=" + std::to_string(buf.total_rows));
}

bool ParquetWriteBufferTake(const std::string &table_path,
                            std::string *out_table_name,
                            std::vector<ParquetBufferedFile> *out_files)
{
  if (out_table_name == nullptr || out_files == nullptr) {
    return false;
  }

  std::lock_guard<std::mutex> lock(g_write_buffer_mutex);
  auto it = g_write_buffers.find(table_path);
  if (it == g_write_buffers.end() || it->second.pending_files.empty()) {
    out_files->clear();
    return false;
  }

  *out_table_name = it->second.table_name;
  *out_files = std::move(it->second.pending_files);
  g_write_buffers.erase(it);

  parquet_log_info("Parquet write buffer take table='" + *out_table_name +
                   "' files=" + std::to_string(out_files->size()));
  return true;
}

bool ParquetWriteBufferShouldFlush(const std::string &table_path,
                                   uint64_t max_rows)
{
  std::lock_guard<std::mutex> lock(g_write_buffer_mutex);
  auto it = g_write_buffers.find(table_path);
  if (it == g_write_buffers.end()) {
    return false;
  }
  return it->second.total_rows >= max_rows;
}

std::vector<std::string> ParquetWriteBufferStaleTables(uint64_t interval_ms)
{
  std::vector<std::string> stale;
  const auto now = std::chrono::steady_clock::now();
  const auto threshold =
      std::chrono::milliseconds(static_cast<int64_t>(interval_ms));

  std::lock_guard<std::mutex> lock(g_write_buffer_mutex);
  for (const auto &entry : g_write_buffers) {
    if (entry.second.pending_files.empty()) {
      continue;
    }
    if (now - entry.second.last_append_time >= threshold) {
      stale.push_back(entry.first);
    }
  }
  return stale;
}

std::vector<std::string> ParquetWriteBufferGetLocalPaths(
    const std::string &table_path)
{
  parquet_log_info("Parquet write buffer lookup key='" + table_path + "'");
  std::vector<std::string> paths;
  std::lock_guard<std::mutex> lock(g_write_buffer_mutex);
  auto it = g_write_buffers.find(table_path);
  if (it == g_write_buffers.end()) {
    parquet_log_info("Parquet write buffer lookup: no entry found");
    return paths;
  }
  paths.reserve(it->second.pending_files.size());
  for (const auto &file : it->second.pending_files) {
    if (!file.local_path.empty()) {
      paths.push_back(file.local_path);
    }
  }
  return paths;
}

} // namespace parquet
