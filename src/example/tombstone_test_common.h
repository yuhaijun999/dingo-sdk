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

// 公共工具：tombstone 优化前后对比测试程序 A/B/C 共用。
// 负责 region_info 文件读写、补零造 key、前缀范围上界、时间戳格式化。
#ifndef DINGODB_SDK_EXAMPLE_TOMBSTONE_TEST_COMMON_H_
#define DINGODB_SDK_EXAMPLE_TOMBSTONE_TEST_COMMON_H_

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <fstream>
#include <map>
#include <sstream>
#include <string>

namespace dingodb {
namespace sdk {
namespace tombstone_test {

// region_info 文件内容（A 写，B/C 读）。
struct RegionInfo {
  int64_t region_id = -1;
  std::string region_name;
  std::string start_key;
  std::string end_key;
  std::string key_prefix;
  int64_t key_count = 0;
  int64_t key_width = 0;
};

// key_count 的十进制位数，用于补零对齐（key_count=10 -> 2 -> key_01..key_10）。
inline int KeyWidth(int64_t key_count) {
  int w = 1;
  int64_t v = key_count;
  while (v >= 10) {
    v /= 10;
    ++w;
  }
  return w;
}

// 造第 i 个 key（i 从 1 开始），补零到 width 位：MakeKey("key_", 1, 2) -> "key_01"。
inline std::string MakeKey(const std::string& prefix, int64_t i, int width) {
  std::string num = std::to_string(i);
  std::string pad;
  if (static_cast<int>(num.size()) < width) {
    pad.assign(width - static_cast<int>(num.size()), '0');
  }
  return prefix + pad + num;
}

// 造 value：MakeValue("value_", 1, 1) -> "value_1_1"（value 索引不补零，遵循需求写法）。
inline std::string MakeValue(const std::string& value_prefix, int64_t key_index, int64_t loop) {
  return value_prefix + std::to_string(key_index) + "_" + std::to_string(loop);
}

// 派生满足 dingo-store 约束的 region 范围（CheckRegionPrefix）：
//   start_key / end_key 均需 ≥ 8 字节、首字节相同；范围要 bracket 所有 prefix+补零(i) 的数据 key。
//   start = prefix + 全 '0'（≤ 最小数据 key），end = prefix + 全 '9'（> 最大数据 key，排他上界）。
//   pad 取 max(8, key_width+1) 保证既 ≥8 字节、又严格大于最大数据 key。
inline void DeriveRegionRange(const std::string& prefix, int key_width, std::string& start, std::string& end) {
  int pad = key_width >= 8 ? key_width + 1 : 8;
  start = prefix + std::string(pad, '0');
  end = prefix + std::string(pad, '9');
}

// 给定前缀，返回该前缀所有 key 的排他上界（最后一个字节 +1，处理 0xff 进位）。
// PrefixUpperBound("key_") -> "key`"，覆盖所有以 "key_" 开头的 key（含补零/不补零）。
inline std::string PrefixUpperBound(const std::string& prefix) {
  std::string end = prefix;
  while (!end.empty()) {
    auto& back = reinterpret_cast<unsigned char&>(end.back());
    if (back < 0xFF) {
      ++back;
      return end;
    }
    end.pop_back();
  }
  return std::string();  // 全 0xff 前缀：无上界（实际场景不会出现）
}

// 当前时间（微秒，自 epoch）。
inline int64_t NowMicros() {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// 当前时间的人类可读字符串，形如 2026-06-24 19:30:00.123456。
inline std::string NowString() {
  auto now = std::chrono::system_clock::now();
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
  std::time_t sec = static_cast<std::time_t>(micros / 1000000);
  int us = static_cast<int>(micros % 1000000);
  std::tm tm_buf{};
  localtime_r(&sec, &tm_buf);
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%06d", tm_buf.tm_year + 1900, tm_buf.tm_mon + 1,
                tm_buf.tm_mday, tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec, us);
  return std::string(buf);
}

// dingo-store 要求 client/executor 的 raw/txn region 的 key 首字节是命名空间标识：
//   'w'=client raw, 'x'=client txn, 'r'=executor raw, 't'=executor txn（见 dingo-store Constant）。
// 否则协调器 CreateRegion 报 "This api can only create client or executor raw/txn region"。
// 本测试走事务，用 client txn('x')。若前缀首字节不是 'x'/'t'，自动加 'x' 前缀；adjusted 返回是否调整。
inline std::string NormalizeTxnKeyPrefix(const std::string& prefix, bool& adjusted) {
  if (!prefix.empty() && (prefix[0] == 'x' || prefix[0] == 't')) {
    adjusted = false;
    return prefix;
  }
  adjusted = true;
  return "x" + prefix;
}

// 从 coor_list 文件解析协调器地址：跳过 # 注释行与空行，逐行取 host:port 用逗号拼接。
// 文件不存在/无有效地址时返回空串。文件样例：
//   # dingo-store coordinators
//   172.30.14.11:32001
//   172.30.14.11:32002
inline std::string ReadCoorListFile(const std::string& path) {
  std::ifstream ifs(path);
  if (!ifs.is_open()) {
    return std::string();
  }
  std::string line;
  std::string result;
  while (std::getline(ifs, line)) {
    size_t begin = line.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) {
      continue;  // 空白行
    }
    size_t end = line.find_last_not_of(" \t\r\n");
    std::string token = line.substr(begin, end - begin + 1);
    if (token.empty() || token[0] == '#') {
      continue;  // 注释行
    }
    if (!result.empty()) {
      result += ",";
    }
    result += token;
  }
  return result;
}

// 解析协调器地址：优先用命令行传入；为空则读 ./coor_list；仍为空则回退本地默认。
inline std::string ResolveCoordinatorAddrs(const std::string& flag_addrs,
                                           const std::string& coor_list_path = "./coor_list") {
  if (!flag_addrs.empty()) {
    return flag_addrs;
  }
  std::string from_file = ReadCoorListFile(coor_list_path);
  if (!from_file.empty()) {
    return from_file;
  }
  return "127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003";
}

// 写 region_info 文件（key=value 行）。返回 true 表示成功。
inline bool WriteRegionInfo(const std::string& path, const RegionInfo& info) {
  std::ofstream ofs(path, std::ios::trunc);
  if (!ofs.is_open()) {
    return false;
  }
  ofs << "region_id=" << info.region_id << "\n"
      << "region_name=" << info.region_name << "\n"
      << "start_key=" << info.start_key << "\n"
      << "end_key=" << info.end_key << "\n"
      << "key_prefix=" << info.key_prefix << "\n"
      << "key_count=" << info.key_count << "\n"
      << "key_width=" << info.key_width << "\n";
  return ofs.good();
}

// 读 region_info 文件。返回 true 表示成功且含 region_id。
inline bool ReadRegionInfo(const std::string& path, RegionInfo& info) {
  std::ifstream ifs(path);
  if (!ifs.is_open()) {
    return false;
  }
  std::map<std::string, std::string> kv;
  std::string line;
  while (std::getline(ifs, line)) {
    auto pos = line.find('=');
    if (pos == std::string::npos) {
      continue;
    }
    kv[line.substr(0, pos)] = line.substr(pos + 1);
  }
  if (kv.find("region_id") == kv.end()) {
    return false;
  }
  info.region_id = std::stoll(kv["region_id"]);
  info.region_name = kv.count("region_name") ? kv["region_name"] : "";
  info.start_key = kv.count("start_key") ? kv["start_key"] : "";
  info.end_key = kv.count("end_key") ? kv["end_key"] : "";
  info.key_prefix = kv.count("key_prefix") ? kv["key_prefix"] : "";
  info.key_count = kv.count("key_count") ? std::stoll(kv["key_count"]) : 0;
  info.key_width = kv.count("key_width") ? std::stoll(kv["key_width"]) : 0;
  return true;
}

}  // namespace tombstone_test
}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_EXAMPLE_TOMBSTONE_TEST_COMMON_H_
