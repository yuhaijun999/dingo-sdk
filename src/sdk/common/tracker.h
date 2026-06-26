// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGODB_SDK_TRACKER_H_
#define DINGODB_SDK_TRACKER_H_

#include <cstdint>
#include <memory>
#include <string>

#include "sdk/common/helper.h" 

namespace dingodb {
namespace sdk {

class Tracker {
 public:
  Tracker() : start_time_(TimestampUs()) {}
  ~Tracker() = default;

  static std::shared_ptr<Tracker> New() { return std::make_shared<Tracker>(); }

  struct Metrics {
    std::atomic<uint64_t> total_transaction_time_us{0};
    std::atomic<uint64_t> prewrite_sdk_time_us{0};
    std::atomic<uint64_t> prewrite_rpc_time_us{0};
    std::atomic<uint64_t> prewrite_retry_count{0};
    std::atomic<uint64_t> prewrite_rpc_retry_count{0};

    std::atomic<uint64_t> commit_sdk_time_us{0};
    std::atomic<uint64_t> commit_rpc_time_us{0};
    std::atomic<uint64_t> commit_retry_count{0};
    std::atomic<uint64_t> commit_rpc_retry_count{0};

    std::atomic<uint64_t> read_sdk_time_us{0};
    std::atomic<uint64_t> read_rpc_time_us{0};
    std::atomic<uint64_t> read_retry_count{0};
    std::atomic<uint64_t> read_rpc_retry_count{0};

    std::atomic<uint64_t> resolve_lock_time_us{0};

    std::atomic<uint64_t> sleep_time_us{0};
    std::atomic<uint64_t> sleep_count{0};
  };

  // 服务端 RocksDB/MVCC 性能指标（来自 response_info.time_info，累加自该事务的所有 RPC）。
  struct ServerMetrics {
    std::atomic<uint64_t> phase_time_us{0};       // Σ elapsed_times.time_us
    std::atomic<uint64_t> mvcc_version{0};        // Σ elapsed_times.skip_version
    std::atomic<uint64_t> internal_skipped{0};    // Σ storage_engine_perf_summary.internal_skipped_count
    std::atomic<uint64_t> tombstone{0};           // Σ storage_engine_perf_summary.internal_tombstone_count
    std::atomic<uint64_t> io_time_us{0};          // Σ storage_engine_perf_summary.io_time_ns / 1000
    std::atomic<uint64_t> miss_block{0};          // Σ storage_engine_perf_summary.miss_block_count
    std::atomic<uint64_t> raft_commit_time_us{0};  // Σ time_info.raft_commit_time_ns / 1000
  };

  void SetTotalTransactionTime() { metrics_.total_transaction_time_us.store(TimestampUs() - start_time_); }
  uint64_t TotalTransactionTime() const { return metrics_.total_transaction_time_us.load(); }

  void IncrementPrewriteSdkTime(uint64_t elapsed_time) { metrics_.prewrite_sdk_time_us.fetch_add(elapsed_time); }
  uint64_t PrewriteSdkTime() const { return metrics_.prewrite_sdk_time_us.load(); }

  void IncrementPrewriteRpcTime(uint64_t elapsed_time) { metrics_.prewrite_rpc_time_us.fetch_add(elapsed_time); }
  uint64_t PrewriteRpcTime() const { return metrics_.prewrite_rpc_time_us.load(); }

  void IncrementPrewriteRetryCount(uint64_t retry_count) { metrics_.prewrite_retry_count.fetch_add(retry_count); }
  uint64_t PrewriteRetryCount() const { return metrics_.prewrite_retry_count.load(); }

  void IncrementPrewriteRpcRetryCount(uint64_t retry_count) {
    metrics_.prewrite_rpc_retry_count.fetch_add(retry_count);
  }
  uint64_t PrewriteRpcRetryCount() const { return metrics_.prewrite_rpc_retry_count.load(); }

  void IncrementCommitSdkTime(uint64_t elapsed_time) { metrics_.commit_sdk_time_us.fetch_add(elapsed_time); }
  uint64_t CommitSdkTime() const { return metrics_.commit_sdk_time_us.load(); }

  void IncrementCommitRpcTime(uint64_t elapsed_time) { metrics_.commit_rpc_time_us.fetch_add(elapsed_time); }
  uint64_t CommitRpcTime() const { return metrics_.commit_rpc_time_us.load(); }

  void IncrementCommitRetryCount(uint64_t retry_count) { metrics_.commit_retry_count.fetch_add(retry_count); }
  uint64_t CommitRetryCount() const { return metrics_.commit_retry_count.load(); }

  void IncrementCommitRpcRetryCount(uint64_t retry_count) { metrics_.commit_rpc_retry_count.fetch_add(retry_count); }
  uint64_t CommitRpcRetryCount() const { return metrics_.commit_rpc_retry_count.load(); }

  void IncrementReadSdkTime(uint64_t elapsed_time) { metrics_.read_sdk_time_us.fetch_add(elapsed_time); }
  uint64_t ReadSdkTime() const { return metrics_.read_sdk_time_us.load(); }

  void IncrementReadRpcTime(uint64_t elapsed_time) { metrics_.read_rpc_time_us.fetch_add(elapsed_time); }
  uint64_t ReadRpcTime() const { return metrics_.read_rpc_time_us.load(); }

  void IncrementReadRetryCount(uint64_t retry_count) { metrics_.read_retry_count.fetch_add(retry_count); }
  uint64_t ReadRetryCount() const { return metrics_.read_retry_count.load(); }

  void IncrementReadRpcRetryCount(uint64_t retry_count) { metrics_.read_rpc_retry_count.fetch_add(retry_count); }
  uint64_t ReadRpcRetryCount() const { return metrics_.read_rpc_retry_count.load(); }

  void IncrementResolveLockTime(uint64_t elapsed_time) { metrics_.resolve_lock_time_us.fetch_add(elapsed_time); }
  uint64_t ResolveLockSdkTime() const { return metrics_.resolve_lock_time_us.load(); }

  void IncrementSleepTime(uint64_t sleep_time_us) { metrics_.sleep_time_us.fetch_add(sleep_time_us); }
  uint64_t SleepTime() const { return metrics_.sleep_time_us.load(); }

  void IncrementSleepCount(uint64_t count) { metrics_.sleep_count.fetch_add(count); }
  uint64_t SleepTimeCount() const { return metrics_.sleep_count.load(); }

  // 累加服务端指标到读路径（scan）桶。
  void AccumulateServerReadMetrics(uint64_t phase_us, uint64_t mvcc_version, uint64_t internal_skipped,
                                   uint64_t tombstone, uint64_t io_us, uint64_t miss_block, uint64_t raft_commit_us) {
    AccumulateServerMetrics(server_read_, phase_us, mvcc_version, internal_skipped, tombstone, io_us, miss_block,
                            raft_commit_us);
  }
  // 累加服务端指标到写路径（prewrite/commit）桶。
  void AccumulateServerWriteMetrics(uint64_t phase_us, uint64_t mvcc_version, uint64_t internal_skipped,
                                    uint64_t tombstone, uint64_t io_us, uint64_t miss_block, uint64_t raft_commit_us) {
    AccumulateServerMetrics(server_write_, phase_us, mvcc_version, internal_skipped, tombstone, io_us, miss_block,
                            raft_commit_us);
  }
  const ServerMetrics& ServerReadMetrics() const { return server_read_; }
  const ServerMetrics& ServerWriteMetrics() const { return server_write_; }

 private:
  static void AccumulateServerMetrics(ServerMetrics& m, uint64_t phase_us, uint64_t mvcc_version,
                                      uint64_t internal_skipped, uint64_t tombstone, uint64_t io_us, uint64_t miss_block,
                                      uint64_t raft_commit_us) {
    m.phase_time_us.fetch_add(phase_us);
    m.mvcc_version.fetch_add(mvcc_version);
    m.internal_skipped.fetch_add(internal_skipped);
    m.tombstone.fetch_add(tombstone);
    m.io_time_us.fetch_add(io_us);
    m.miss_block.fetch_add(miss_block);
    m.raft_commit_time_us.fetch_add(raft_commit_us);
  }

  uint64_t start_time_;

  Metrics metrics_;
  ServerMetrics server_read_;
  ServerMetrics server_write_;
};
using TrackerPtr = std::shared_ptr<Tracker>;
}  // namespace sdk
}  // namespace dingodb
#endif