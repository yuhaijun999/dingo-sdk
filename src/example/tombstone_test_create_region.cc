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

// 程序 A：创建一个普通事务型 region（kLSM 引擎，不使用 index/document），
// 为后续灌数据/扫描做准备。输出 region id 和 region 范围，并落盘 region_info 文件。
//
// 用法示例：
//   ./tombstone_test_create_region --addrs=127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003 \
//       --region_name=tombstone_test --key_prefix=key_ --key_count=10 --replicas=3 \
//       --region_info_file=./region_info.txt

#include <gflags/gflags.h>

#include <cstdint>
#include <memory>
#include <string>

#include "common/logging.h"
#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "glog/logging.h"
#include "tombstone_test_common.h"

using dingodb::sdk::Status;
namespace tt = dingodb::sdk::tombstone_test;

DEFINE_string(addrs, "", "coordinator addrs, e.g. 127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003");
DEFINE_string(region_name, "tombstone_test", "region name");
DEFINE_string(key_prefix, "key_", "key prefix, region range is derived from it");
DEFINE_int64(key_count, 10, "number of distinct keys that will be written into this region");
DEFINE_int64(replicas, 3, "region replica num");
DEFINE_string(region_info_file, "./region_info.txt", "output file recording region id and range");

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
  CHECK(!FLAGS_key_prefix.empty()) << "key_prefix must not be empty";
  CHECK(FLAGS_key_count > 0) << "key_count must > 0";
  CHECK(FLAGS_replicas > 0) << "replicas must > 0";

  // dingo-store 要求事务 region 的 key 首字节是命名空间标识（'x'=client txn）。自动补齐。
  bool prefix_adjusted = false;
  const std::string key_prefix = tt::NormalizeTxnKeyPrefix(FLAGS_key_prefix, prefix_adjusted);
  if (prefix_adjusted) {
    DINGO_LOG(WARNING) << "txn region requires key namespace prefix 'x'(client txn)/'t'(executor txn); "
                       << "adjusted key_prefix from '" << FLAGS_key_prefix << "' to '" << key_prefix << "'";
  }

  dingodb::sdk::Client* tmp_client = nullptr;
  Status built = dingodb::sdk::Client::BuildFromAddrs(FLAGS_addrs, &tmp_client);
  if (!built.ok()) {
    DINGO_LOG(ERROR) << "Fail to build client, check --addrs=" << FLAGS_addrs << " error: " << built.ToString();
    return -1;
  }
  std::shared_ptr<dingodb::sdk::Client> client(tmp_client);

  // region 范围：dingo-store 要求 start/end ≥ 8 字节、同首字节。由前缀+补零派生，bracket 所有数据 key。
  const int key_width = tt::KeyWidth(FLAGS_key_count);
  std::string start_key;
  std::string end_key;
  tt::DeriveRegionRange(key_prefix, key_width, start_key, end_key);
  CHECK(start_key < end_key) << "start_key must < end_key, got [" << start_key << ", " << end_key << ")";

  dingodb::sdk::RegionCreator* tmp_creator = nullptr;
  Status creator_built = client->NewRegionCreator(&tmp_creator);
  CHECK(creator_built.IsOK()) << "create region creator fail: " << creator_built.ToString();
  std::shared_ptr<dingodb::sdk::RegionCreator> creator(tmp_creator);

  int64_t region_id = -1;
  Status create = creator->SetRegionName(FLAGS_region_name)
                      .SetEngineType(dingodb::sdk::EngineType::kLSM)  // 普通事务引擎
                      .SetRange(start_key, end_key)
                      .SetReplicaNum(FLAGS_replicas)
                      .Wait(true)
                      .Create(region_id);
  if (!create.ok() || region_id <= 0) {
    DINGO_LOG(ERROR) << "create region fail: " << create.ToString() << ", region_id=" << region_id;
    return -1;
  }

  bool inprogress = true;
  client->IsCreateRegionInProgress(region_id, inprogress);
  if (inprogress) {
    DINGO_LOG(ERROR) << "region " << region_id << " still in progress after Wait(true)";
    return -1;
  }

  tt::RegionInfo info;
  info.region_id = region_id;
  info.region_name = FLAGS_region_name;
  info.start_key = start_key;
  info.end_key = end_key;
  info.key_prefix = key_prefix;
  info.key_count = FLAGS_key_count;
  info.key_width = key_width;

  if (!tt::WriteRegionInfo(FLAGS_region_info_file, info)) {
    DINGO_LOG(ERROR) << "write region_info file fail: " << FLAGS_region_info_file;
    return -1;
  }

  DINGO_LOG(INFO) << "==== create region success ====";
  DINGO_LOG(INFO) << "region_id   : " << region_id;
  DINGO_LOG(INFO) << "region_name : " << FLAGS_region_name;
  DINGO_LOG(INFO) << "range       : [" << start_key << ", " << end_key << ")";
  DINGO_LOG(INFO) << "engine      : kLSM (normal transaction)";
  DINGO_LOG(INFO) << "key_prefix  : " << key_prefix << ", key_count: " << FLAGS_key_count
                  << ", key_width: " << info.key_width;
  DINGO_LOG(INFO) << "sample keys : " << tt::MakeKey(key_prefix, 1, info.key_width) << " ... "
                  << tt::MakeKey(key_prefix, FLAGS_key_count, info.key_width);
  DINGO_LOG(INFO) << "region_info : " << FLAGS_region_info_file << " (used by load/scan programs)";

  return 0;
}
