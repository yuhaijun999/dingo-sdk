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

// 程序 B：向程序 A 创建的 region 反复覆盖写数据，制造大量 MVCC 历史版本
// （GC 后变 tombstone），用于 tombstone 优化前后对比。
//
// 数据语义：第 c 轮循环(1..loop_count) 对 key_i(i=1..key_count) 写入新 value：
//   key   = key_prefix + zero_pad(i)          例: key_01 .. key_10
//   value = value_prefix + i + "_" + c         例: value_1_1, value_1_2, value_10_1
// 每轮 = 1 个乐观快照事务，BatchPut(key_count 个 key) 后 Commit。
//
// 用法示例：
//   ./tombstone_test_load_data --addrs=127.0.0.1:22001 --region_info_file=./region_info.txt \
//       --value_prefix=value_ --loop_count=100000 --csv_file=./load_data.csv --log_file=./load_data.log

#include <gflags/gflags.h>

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "common/logging.h"
#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "glog/logging.h"
#include "tombstone_test_common.h"

using dingodb::sdk::Status;
namespace tt = dingodb::sdk::tombstone_test;

DEFINE_string(addrs, "", "coordinator addrs, e.g. 127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003");
DEFINE_string(region_info_file, "./region_info.txt", "region info file produced by create_region program");
DEFINE_string(key_prefix, "", "override key prefix (default: read from region_info_file)");
DEFINE_string(value_prefix, "value_", "value prefix");
DEFINE_int64(key_count, 0, "override key count (default: read from region_info_file)");
DEFINE_int64(loop_count, 100000, "number of write rounds; each round writes key_count keys in one txn");
DEFINE_bool(use_batch_put, true, "true: BatchPut all keys in one call; false: per-key Put then one Commit");
DEFINE_int64(report_interval, 1000, "print progress to console every N rounds");
DEFINE_string(csv_file, "./load_data.csv", "per-round metrics in CSV for later statistics");
DEFINE_string(log_file, "./load_data.log", "human-readable log file");

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_addrs.empty()) {
    FLAGS_addrs = tt::ResolveCoordinatorAddrs(FLAGS_addrs);
    DINGO_LOG(WARNING) << "coordinator --addrs not set, resolved from ./coor_list or default: " << FLAGS_addrs;
  }

  // 读 region_info，key_prefix / key_count 默认取自该文件，命令行可覆盖。
  tt::RegionInfo info;
  if (!tt::ReadRegionInfo(FLAGS_region_info_file, info)) {
    DINGO_LOG(ERROR) << "read region_info file fail: " << FLAGS_region_info_file
                     << " (run create_region program first)";
    return -1;
  }
  std::string key_prefix = FLAGS_key_prefix.empty() ? info.key_prefix : FLAGS_key_prefix;
  int64_t key_count = FLAGS_key_count > 0 ? FLAGS_key_count : info.key_count;
  CHECK(!key_prefix.empty()) << "key_prefix empty (region_info or --key_prefix)";
  CHECK(key_count > 0) << "key_count must > 0 (region_info or --key_count)";
  CHECK(FLAGS_loop_count > 0) << "loop_count must > 0";
  int key_width = tt::KeyWidth(key_count);
  if (!FLAGS_key_prefix.empty() && FLAGS_key_prefix != info.key_prefix) {
    DINGO_LOG(WARNING) << "key_prefix override (" << key_prefix << ") differs from region_info ("
                       << info.key_prefix << "); keys may fall outside region range";
  }

  dingodb::sdk::Client* tmp_client = nullptr;
  Status built = dingodb::sdk::Client::BuildFromAddrs(FLAGS_addrs, &tmp_client);
  if (!built.ok()) {
    DINGO_LOG(ERROR) << "Fail to build client, check --addrs=" << FLAGS_addrs << " error: " << built.ToString();
    return -1;
  }
  std::shared_ptr<dingodb::sdk::Client> client(tmp_client);

  std::ofstream csv(FLAGS_csv_file, std::ios::trunc);
  std::ofstream log(FLAGS_log_file, std::ios::trunc);
  if (!csv.is_open() || !log.is_open()) {
    DINGO_LOG(ERROR) << "open output file fail, csv=" << FLAGS_csv_file << " log=" << FLAGS_log_file;
    return -1;
  }
  csv << "timestamp,loop,kv_count,commit_status,latency_us,sdk_us,rpc_us,retry,"
         "srv_phase_us,srv_raft_commit_us,srv_io_us,srv_miss_block,srv_mvcc_version,srv_internal_skipped,srv_tombstone"
         "\n";
  log << "# tombstone load_data: region_id=" << info.region_id << " range=[" << info.start_key << ","
      << info.end_key << ") key_prefix=" << key_prefix << " key_count=" << key_count << " key_width=" << key_width
      << " loop_count=" << FLAGS_loop_count << " use_batch_put=" << FLAGS_use_batch_put << "\n";
  log.flush();

  DINGO_LOG(INFO) << "start loading: region_id=" << info.region_id << " key_count=" << key_count
                  << " loop_count=" << FLAGS_loop_count << " keys=[" << tt::MakeKey(key_prefix, 1, key_width) << ".."
                  << tt::MakeKey(key_prefix, key_count, key_width) << "]";

  // 预生成 key 列表（每轮不变）。
  std::vector<std::string> keys;
  keys.reserve(key_count);
  for (int64_t i = 1; i <= key_count; ++i) {
    keys.push_back(tt::MakeKey(key_prefix, i, key_width));
  }

  int64_t ok_rounds = 0;
  int64_t fail_rounds = 0;
  const int64_t run_start_us = tt::NowMicros();

  for (int64_t c = 1; c <= FLAGS_loop_count; ++c) {
    dingodb::sdk::TransactionOptions options;
    options.kind = dingodb::sdk::kOptimistic;
    options.isolation = dingodb::sdk::kSnapshotIsolation;
    options.keep_alive_ms = 0;

    dingodb::sdk::Transaction* tmp_txn = nullptr;
    Status txn_built = client->NewTransaction(options, &tmp_txn);
    if (!txn_built.ok()) {
      DINGO_LOG(ERROR) << "new txn fail at round " << c << ": " << txn_built.ToString();
      ++fail_rounds;
      continue;
    }
    std::shared_ptr<dingodb::sdk::Transaction> txn(tmp_txn);

    const int64_t t0 = tt::NowMicros();

    if (FLAGS_use_batch_put) {
      std::vector<dingodb::sdk::KVPair> kvs;
      kvs.reserve(key_count);
      for (int64_t i = 1; i <= key_count; ++i) {
        kvs.push_back({keys[i - 1], tt::MakeValue(FLAGS_value_prefix, i, c)});
      }
      txn->BatchPut(kvs);
    } else {
      for (int64_t i = 1; i <= key_count; ++i) {
        txn->Put(keys[i - 1], tt::MakeValue(FLAGS_value_prefix, i, c));
      }
    }

    Status commit = txn->Commit();
    const int64_t latency_us = tt::NowMicros() - t0;

    dingodb::sdk::TraceMetrics metrics;
    txn->GetTraceMetrics(metrics);
    uint64_t sdk_us = metrics.prewrite_metric.sdk_time_us.load() + metrics.commit_metric.sdk_time_us.load();
    uint64_t rpc_us = metrics.prewrite_metric.rpc_time_us.load() + metrics.commit_metric.rpc_time_us.load();
    uint64_t retry = metrics.prewrite_metric.retry_count.load() + metrics.commit_metric.retry_count.load();

    // 服务端写路径（prewrite+commit）性能指标。
    const auto& srv = metrics.server_write_metric;
    uint64_t srv_phase_us = srv.phase_time_us.load();
    uint64_t srv_raft_us = srv.raft_commit_time_us.load();
    uint64_t srv_io_us = srv.io_time_us.load();
    uint64_t srv_miss = srv.miss_block.load();
    uint64_t srv_mvcc = srv.mvcc_version.load();
    uint64_t srv_skipped = srv.internal_skipped.load();
    uint64_t srv_tomb = srv.tombstone.load();

    csv << tt::NowString() << "," << c << "," << key_count << "," << (commit.ok() ? "OK" : "FAIL") << ","
        << latency_us << "," << sdk_us << "," << rpc_us << "," << retry << "," << srv_phase_us << "," << srv_raft_us
        << "," << srv_io_us << "," << srv_miss << "," << srv_mvcc << "," << srv_skipped << "," << srv_tomb << "\n";

    if (commit.ok()) {
      ++ok_rounds;
    } else {
      ++fail_rounds;
    }

    // 每轮写一行到日志（索引 loop 从 1 开始 + 全部服务端指标），与 scan.log / load_data.csv 逐行对齐。
    log << tt::NowString() << " loop " << c << "/" << FLAGS_loop_count << " commit=" << (commit.ok() ? "OK" : "FAIL")
        << " latency_us=" << latency_us << " srv_phase_us=" << srv_phase_us << " srv_raft_commit_us=" << srv_raft_us
        << " srv_io_us=" << srv_io_us << " srv_miss_block=" << srv_miss << " srv_mvcc_version=" << srv_mvcc
        << " srv_internal_skipped=" << srv_skipped << " srv_tombstone=" << srv_tomb;
    if (!commit.ok()) {
      log << " FAIL_reason=" << commit.ToString();
    }
    log << "\n";

    if (c % FLAGS_report_interval == 0 || c == FLAGS_loop_count) {
      csv.flush();
      log.flush();
      DINGO_LOG(INFO) << "progress " << c << "/" << FLAGS_loop_count << " ok=" << ok_rounds << " fail=" << fail_rounds
                      << " last_latency_us=" << latency_us;
    }
  }

  const int64_t total_us = tt::NowMicros() - run_start_us;
  log << "# done: ok_rounds=" << ok_rounds << " fail_rounds=" << fail_rounds << " total_written_versions="
      << (ok_rounds * key_count) << " elapsed_us=" << total_us << "\n";
  csv.flush();
  log.flush();

  DINGO_LOG(INFO) << "==== load done ==== ok_rounds=" << ok_rounds << " fail_rounds=" << fail_rounds
                  << " total_versions=" << (ok_rounds * key_count) << " elapsed_ms=" << (total_us / 1000)
                  << " csv=" << FLAGS_csv_file << " log=" << FLAGS_log_file;
  return fail_rounds == 0 ? 0 : 1;
}
