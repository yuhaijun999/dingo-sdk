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

// 程序 C：循环扫描整个 region（[start_key, end_key)，即 key_1..key_N），
// 记录每轮 scan 耗时与返回条数，用于 tombstone 优化前后对比统计。
//
// 命令行参数：循环次数 + 每次循环间隔时间。两种写法等价：
//   ./tombstone_test_scan --scan_rounds=100 --interval_ms=1000
//   ./tombstone_test_scan 100 1000              # 位置参数: 轮数 间隔ms
//
// 读时间戳策略（--isolation 与 --read_ts_mode 配合）：
//   isolation=readcommitted（默认）：每轮取最新 TSO，永远 > GC safepoint，最稳。
//   isolation=snapshot：用固定 start_ts 读一个旧快照，能让 scan 穿越更多 MVCC 版本/tombstone。
//     read_ts 由 --read_ts_mode 决定（仅 snapshot 生效）：
//       latest          —— 用最新 ts（≈ readcommitted）
//       midpoint        —— (GC safepoint + 当前 ts) / 2 取中间值
//       safepoint_delta —— GC safepoint + (--safepoint_delta_ms << 18)，紧贴 safepoint
//     注意：read_ts 必须 > GC safepoint；长跑中后台 GC 可能推进 safepoint 越过固定 read_ts，
//     届时旧版本已回收会读异常，本程序每轮重查 safepoint 并告警。

#include <gflags/gflags.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/logging.h"
#include "dingosdk/client.h"
#include "dingosdk/coordinator.h"
#include "dingosdk/status.h"
#include "glog/logging.h"
#include "tombstone_test_common.h"

using dingodb::sdk::Status;
namespace tt = dingodb::sdk::tombstone_test;

// TSO 时间戳格式：ts = (physical_ms << kPhysicalShiftBits) + logical，与 SDK 内部一致。
static constexpr int kPhysicalShiftBits = 18;

DEFINE_string(addrs, "", "coordinator addrs, e.g. 127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003");
DEFINE_string(region_info_file, "./region_info.txt", "region info file produced by create_region program");
DEFINE_int64(scan_rounds, 10, "number of scan rounds (also accepts positional argv[1])");
DEFINE_int64(interval_ms, 1000, "sleep milliseconds between rounds (also accepts positional argv[2])");
DEFINE_uint64(limit, 0, "scan limit, 0 means scan all keys in range");
DEFINE_string(isolation, "readcommitted", "isolation for scan: readcommitted | snapshot");
DEFINE_string(read_ts_mode, "latest", "snapshot read ts: latest | midpoint | safepoint_delta");
DEFINE_int64(safepoint_delta_ms, 1000, "for read_ts_mode=safepoint_delta: ms added above GC safepoint");
DEFINE_bool(print_kv, false, "dump each returned kv to scan_kv.log");
DEFINE_string(csv_file, "./scan.csv", "per-round metrics in CSV for later statistics");
DEFINE_string(log_file, "./scan.log", "human-readable log file");

// 取当前 ts：开一个临时 readcommitted 事务，其 start_ts 即当前 TSO（Transaction::ID() == start_ts）。
static bool GetCurrentTs(const std::shared_ptr<dingodb::sdk::Client>& client, int64_t& out_ts) {
  dingodb::sdk::TransactionOptions options;
  options.kind = dingodb::sdk::kOptimistic;
  options.isolation = dingodb::sdk::kReadCommitted;
  options.keep_alive_ms = 0;
  options.start_ts = 0;  // auto

  dingodb::sdk::Transaction* tmp = nullptr;
  Status s = client->NewTransaction(options, &tmp);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << "get current ts: new txn fail: " << s.ToString();
    return false;
  }
  std::shared_ptr<dingodb::sdk::Transaction> txn(tmp);
  out_ts = txn->ID();
  return out_ts > 0;
}

// 计算固定 read_ts。返回 0 表示用最新 ts（auto）。
static int64_t ComputeReadTs(const std::shared_ptr<dingodb::sdk::Client>& client) {
  if (FLAGS_read_ts_mode == "latest") {
    return 0;
  }

  dingodb::sdk::Coordinator* tmp_coord = nullptr;
  Status s = client->NewCoordinator(&tmp_coord);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << "new coordinator fail: " << s.ToString() << ", fallback to latest ts";
    return 0;
  }
  std::shared_ptr<dingodb::sdk::Coordinator> coord(tmp_coord);

  int64_t safe_point = 0;
  bool gc_stop = false;
  s = coord->GetGCSafePoint(safe_point, gc_stop);
  if (!s.ok()) {
    DINGO_LOG(ERROR) << "GetGCSafePoint fail: " << s.ToString() << ", fallback to latest ts";
    return 0;
  }

  int64_t current_ts = 0;
  if (!GetCurrentTs(client, current_ts)) {
    DINGO_LOG(ERROR) << "get current ts fail, fallback to latest ts";
    return 0;
  }

  DINGO_LOG(INFO) << "GC safe_point=" << safe_point << " gc_stop=" << gc_stop << " current_ts=" << current_ts;

  if (safe_point <= 0) {
    // GC 尚未推进 safepoint（无回收边界），所有版本都在，用最新 ts 即可扫到全部旧版本。
    DINGO_LOG(WARNING) << "GC safe_point<=0 (GC not advanced), use latest ts; all versions still present";
    return 0;
  }

  int64_t read_ts = 0;
  if (FLAGS_read_ts_mode == "midpoint") {
    read_ts = safe_point + (current_ts - safe_point) / 2;
  } else if (FLAGS_read_ts_mode == "safepoint_delta") {
    read_ts = safe_point + (FLAGS_safepoint_delta_ms << kPhysicalShiftBits);
  } else {
    DINGO_LOG(ERROR) << "invalid --read_ts_mode=" << FLAGS_read_ts_mode << ", fallback to latest ts";
    return 0;
  }

  // 约束：safepoint < read_ts <= current_ts。
  if (read_ts <= safe_point) {
    read_ts = safe_point + 1;
  }
  if (read_ts > current_ts) {
    read_ts = current_ts;
  }
  DINGO_LOG(INFO) << "computed fixed read_ts=" << read_ts << " (mode=" << FLAGS_read_ts_mode << ")";
  return read_ts;
}

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  // 位置参数覆盖（friendly）：argv[1]=轮数 argv[2]=间隔ms。
  if (argc > 1) {
    FLAGS_scan_rounds = std::atoll(argv[1]);
  }
  if (argc > 2) {
    FLAGS_interval_ms = std::atoll(argv[2]);
  }

  if (FLAGS_addrs.empty()) {
    FLAGS_addrs = tt::ResolveCoordinatorAddrs(FLAGS_addrs);
    DINGO_LOG(WARNING) << "coordinator --addrs not set, resolved from ./coor_list or default: " << FLAGS_addrs;
  }
  CHECK(FLAGS_scan_rounds > 0) << "scan_rounds must > 0";
  CHECK(FLAGS_interval_ms >= 0) << "interval_ms must >= 0";

  dingodb::sdk::TransactionIsolation isolation;
  if (FLAGS_isolation == "snapshot") {
    isolation = dingodb::sdk::kSnapshotIsolation;
  } else if (FLAGS_isolation == "readcommitted") {
    isolation = dingodb::sdk::kReadCommitted;
  } else {
    DINGO_LOG(ERROR) << "invalid --isolation=" << FLAGS_isolation << " (use readcommitted | snapshot)";
    return -1;
  }

  tt::RegionInfo info;
  if (!tt::ReadRegionInfo(FLAGS_region_info_file, info)) {
    DINGO_LOG(ERROR) << "read region_info file fail: " << FLAGS_region_info_file
                     << " (run create_region program first)";
    return -1;
  }
  CHECK(!info.start_key.empty() && !info.end_key.empty()) << "region_info missing start_key/end_key";

  dingodb::sdk::Client* tmp_client = nullptr;
  Status built = dingodb::sdk::Client::BuildFromAddrs(FLAGS_addrs, &tmp_client);
  if (!built.ok()) {
    DINGO_LOG(ERROR) << "Fail to build client, check --addrs=" << FLAGS_addrs << " error: " << built.ToString();
    return -1;
  }
  std::shared_ptr<dingodb::sdk::Client> client(tmp_client);

  // 固定 read_ts：仅 snapshot 隔离下生效；0 表示每轮取最新 ts。
  int64_t fixed_read_ts = 0;
  if (isolation == dingodb::sdk::kSnapshotIsolation) {
    fixed_read_ts = ComputeReadTs(client);
    if (fixed_read_ts == 0) {
      DINGO_LOG(WARNING) << "snapshot scan uses latest ts each round (≈ readcommitted)";
    } else {
      DINGO_LOG(WARNING) << "snapshot scan uses FIXED read_ts=" << fixed_read_ts
                         << "; if background GC advances safepoint past it, reads may fail";
    }
  }

  std::ofstream csv(FLAGS_csv_file, std::ios::trunc);
  std::ofstream log(FLAGS_log_file, std::ios::trunc);
  if (!csv.is_open() || !log.is_open()) {
    DINGO_LOG(ERROR) << "open output file fail, csv=" << FLAGS_csv_file << " log=" << FLAGS_log_file;
    return -1;
  }
  std::ofstream kv_log;
  if (FLAGS_print_kv) {
    kv_log.open("./scan_kv.log", std::ios::trunc);
  }

  csv << "timestamp,round,scan_status,latency_us,kv_count,read_ts,total_time_us,read_sdk_us,read_rpc_us,"
         "srv_phase_us,srv_mvcc_version,srv_internal_skipped,srv_tombstone,srv_io_us,srv_miss_block\n";
  log << "# tombstone scan: region_id=" << info.region_id << " range=[" << info.start_key << "," << info.end_key
      << ") isolation=" << FLAGS_isolation << " read_ts_mode=" << FLAGS_read_ts_mode
      << " fixed_read_ts=" << fixed_read_ts << " scan_rounds=" << FLAGS_scan_rounds
      << " interval_ms=" << FLAGS_interval_ms << " limit=" << FLAGS_limit << "\n";
  log.flush();

  DINGO_LOG(INFO) << "start scan: region_id=" << info.region_id << " range=[" << info.start_key << ","
                  << info.end_key << ") rounds=" << FLAGS_scan_rounds << " interval_ms=" << FLAGS_interval_ms
                  << " isolation=" << FLAGS_isolation << " fixed_read_ts=" << fixed_read_ts;

  for (int64_t r = 1; r <= FLAGS_scan_rounds; ++r) {
    // 每轮开始先打印当前进度（第 r 次 / 共 scan_rounds 次），方便控制台实时观察。
    DINGO_LOG(INFO) << "==== scan round " << r << "/" << FLAGS_scan_rounds << " starting ====";

    // 固定 read_ts 时，每轮重查 safepoint，若已越过则告警（数据可能被回收）。
    if (fixed_read_ts > 0) {
      dingodb::sdk::Coordinator* tmp_coord = nullptr;
      if (client->NewCoordinator(&tmp_coord).ok()) {
        std::shared_ptr<dingodb::sdk::Coordinator> coord(tmp_coord);
        int64_t sp = 0;
        bool gc_stop = false;
        if (coord->GetGCSafePoint(sp, gc_stop).ok() && sp >= fixed_read_ts) {
          DINGO_LOG(WARNING) << "round " << r << ": GC safepoint(" << sp << ") >= fixed read_ts(" << fixed_read_ts
                             << "), old versions may be reclaimed, scan may fail";
        }
      }
    }

    dingodb::sdk::TransactionOptions options;
    options.kind = dingodb::sdk::kOptimistic;
    options.isolation = isolation;
    options.keep_alive_ms = 0;
    options.start_ts = fixed_read_ts;  // 0 = 每轮取最新 ts；>0 = 固定旧快照

    dingodb::sdk::Transaction* tmp_txn = nullptr;
    Status txn_built = client->NewTransaction(options, &tmp_txn);
    if (!txn_built.ok()) {
      DINGO_LOG(ERROR) << "new txn fail at round " << r << ": " << txn_built.ToString();
      csv << tt::NowString() << "," << r << ",NEW_TXN_FAIL,0,0," << fixed_read_ts << ",0,0,0,0,0,0,0,0,0\n";
      csv.flush();
      if (FLAGS_interval_ms > 0 && r < FLAGS_scan_rounds) {
        std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_interval_ms));
      }
      continue;
    }
    std::shared_ptr<dingodb::sdk::Transaction> txn(tmp_txn);
    int64_t round_read_ts = txn->ID();

    std::vector<dingodb::sdk::KVPair> kvs;
    const int64_t t0 = tt::NowMicros();
    Status scan = txn->Scan(info.start_key, info.end_key, FLAGS_limit, kvs);
    const int64_t latency_us = tt::NowMicros() - t0;
    txn->Commit();  // read-only txn 收尾

    dingodb::sdk::TraceMetrics metrics;
    txn->GetTraceMetrics(metrics);
    uint64_t total_us = metrics.total_time_us.load();
    uint64_t read_sdk_us = metrics.read_metric.sdk_time_us.load();
    uint64_t read_rpc_us = metrics.read_metric.rpc_time_us.load();

    // 服务端读路径（scan）性能指标 —— tombstone 优化前后对比的核心信号。
    const auto& srv = metrics.server_read_metric;
    uint64_t srv_phase_us = srv.phase_time_us.load();
    uint64_t srv_mvcc = srv.mvcc_version.load();
    uint64_t srv_skipped = srv.internal_skipped.load();
    uint64_t srv_tomb = srv.tombstone.load();
    uint64_t srv_io_us = srv.io_time_us.load();
    uint64_t srv_miss = srv.miss_block.load();

    csv << tt::NowString() << "," << r << "," << (scan.ok() ? "OK" : "FAIL") << "," << latency_us << ","
        << kvs.size() << "," << round_read_ts << "," << total_us << "," << read_sdk_us << "," << read_rpc_us << ","
        << srv_phase_us << "," << srv_mvcc << "," << srv_skipped << "," << srv_tomb << "," << srv_io_us << ","
        << srv_miss << "\n";
    csv.flush();

    log << tt::NowString() << " round " << r << " scan=" << scan.ToString() << " read_ts=" << round_read_ts
        << " latency_us=" << latency_us << " kv_count=" << kvs.size() << " srv_phase_us=" << srv_phase_us
        << " srv_mvcc_version=" << srv_mvcc << " srv_internal_skipped=" << srv_skipped << " srv_tombstone=" << srv_tomb
        << " srv_io_us=" << srv_io_us << " srv_miss_block=" << srv_miss << "\n";
    log.flush();

    if (FLAGS_print_kv && kv_log.is_open()) {
      kv_log << "==== round " << r << " (" << tt::NowString() << ") kv_count=" << kvs.size() << " ====\n";
      for (const auto& kv : kvs) {
        kv_log << kv.key << " => " << kv.value << "\n";
      }
      kv_log.flush();
    }

    DINGO_LOG(INFO) << "round " << r << "/" << FLAGS_scan_rounds << " scan=" << scan.ToString()
                    << " kv_count=" << kvs.size() << " latency_us=" << latency_us << " read_ts=" << round_read_ts;

    if (FLAGS_interval_ms > 0 && r < FLAGS_scan_rounds) {
      std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_interval_ms));
    }
  }

  DINGO_LOG(INFO) << "==== scan done ==== rounds=" << FLAGS_scan_rounds << " csv=" << FLAGS_csv_file
                  << " log=" << FLAGS_log_file;
  return 0;
}
