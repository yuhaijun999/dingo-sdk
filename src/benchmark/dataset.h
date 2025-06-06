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

#ifndef DINGODB_BENCHMARK_DATASET_H_
#define DINGODB_BENCHMARK_DATASET_H_

#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "H5Cpp.h"
#include "dingosdk/vector.h"
#include "rapidjson/document.h"

namespace dingodb {
namespace benchmark {

// dataset abstraction class
class Dataset {
 public:
  Dataset() { obtain_dimension.store(true); }
  virtual ~Dataset() = default;

  struct TestEntry {
    sdk::VectorWithId vector_with_id;
    std::unordered_map<int64_t, float> neighbors;
    std::string filter_json;
    std::vector<int64_t> filter_vector_ids;
  };
  using TestEntryPtr = std::shared_ptr<TestEntry>;

  static std::shared_ptr<Dataset> New(std::string filepath);

  virtual bool Init() = 0;

  virtual uint32_t GetDimension() const = 0;
  virtual uint32_t GetTrainDataCount() const = 0;
  virtual uint32_t GetTestDataCount() const = 0;

  // Get train data by batch
  virtual void GetBatchTrainData(uint32_t batch_num, std::vector<sdk::VectorWithId>& vector_with_ids, bool& is_eof) = 0;

  // Get all test data
  virtual std::vector<TestEntryPtr> GetTestData() = 0;

  // get type
  virtual std::string GetType() = 0;

  // get dimension
  bool GetObtainDimension() { return obtain_dimension.load(); }

 protected:
  mutable std::atomic<bool> obtain_dimension;
};
using DatasetPtr = std::shared_ptr<Dataset>;

class BaseDataset : public Dataset {
 public:
  BaseDataset(std::string filepath);
  ~BaseDataset() override;

  bool Init() override;

  uint32_t GetDimension() const override;
  uint32_t GetTrainDataCount() const override;
  uint32_t GetTestDataCount() const override;

  // Get train data by batch
  void GetBatchTrainData(uint32_t batch_num, std::vector<sdk::VectorWithId>& vector_with_ids, bool& is_eof) override;

  // Get all test data
  std::vector<TestEntryPtr> GetTestData() override;

  std::string GetType() override { return "BaseDataset"; }

 private:
  std::vector<int> GetNeighbors(uint32_t index);
  std::vector<float> GetDistances(uint32_t index);
  std::unordered_map<int64_t, float> GetTestVectorNeighbors(uint32_t index);

  std::string filepath_;
  std::shared_ptr<H5::H5File> h5file_;

  uint32_t train_row_count_{0};
  uint32_t test_row_count_{0};

  uint32_t dimension_{0};
  std::mutex mutex_;
};

// sift/glove/gist/mnist is same
class SiftDataset : public BaseDataset {
 public:
  SiftDataset(std::string filepath) : BaseDataset(filepath) {}
  ~SiftDataset() override = default;
  std::string GetType() override { return "SiftDataset"; }
};

class GloveDataset : public BaseDataset {
 public:
  GloveDataset(std::string filepath) : BaseDataset(filepath) {}
  ~GloveDataset() override = default;
  std::string GetType() override { return "GloveDataset"; }
};

class GistDataset : public BaseDataset {
 public:
  GistDataset(std::string filepath) : BaseDataset(filepath) {}
  ~GistDataset() override = default;
  std::string GetType() override { return "GistDataset"; }
};

class KosarakDataset : public BaseDataset {
 public:
  KosarakDataset(std::string filepath) : BaseDataset(filepath) {}
  ~KosarakDataset() override = default;
  std::string GetType() override { return "KosarakDataset"; }
};

class LastfmDataset : public BaseDataset {
 public:
  LastfmDataset(std::string filepath) : BaseDataset(filepath) {}
  ~LastfmDataset() override = default;
  std::string GetType() override { return "LastfmDataset"; }
};

class MnistDataset : public BaseDataset {
 public:
  MnistDataset(std::string filepath) : BaseDataset(filepath) {}
  ~MnistDataset() override = default;
  std::string GetType() override { return "MnistDataset"; }
};

class Movielens10mDataset : public BaseDataset {
 public:
  Movielens10mDataset(std::string filepath) : BaseDataset(filepath) {}
  ~Movielens10mDataset() override = default;
  std::string GetType() override { return "Movielens10mDataset"; }
};

class LaionDataset : public BaseDataset {
 public:
  LaionDataset(std::string filepath) : BaseDataset(filepath) {}
  ~LaionDataset() override = default;
  std::string GetType() override { return "LaionDataset"; }
};

class EmbeddingsDataset : public BaseDataset {
 public:
  EmbeddingsDataset(std::string filepath) : BaseDataset(filepath) {}
  ~EmbeddingsDataset() override = default;
  std::string GetType() override { return "EmbeddingsDataset"; }
};

struct BatchVectorEntry {
  std::vector<sdk::VectorWithId> vector_with_ids;
};
using BatchVectorEntryPtr = std::shared_ptr<BatchVectorEntry>;

class JsonDataset : public Dataset, public std::enable_shared_from_this<JsonDataset> {
 public:
  JsonDataset(const std::string& dirpath) : dirpath_(dirpath) { obtain_dimension.store(false); }
  ~JsonDataset() override = default;

  bool Init() override;

  uint32_t GetDimension() const override;
  uint32_t GetTrainDataCount() const override;
  uint32_t GetTestDataCount() const override;

  // Get train data by batch
  void GetBatchTrainData(uint32_t batch_num, std::vector<sdk::VectorWithId>& vector_with_ids, bool& is_eof) override;

  // Get all test data
  std::vector<TestEntryPtr> GetTestData() override;

  std::string GetType() override { return "JsonDataset"; }

  std::shared_ptr<JsonDataset> GetSelf() { return shared_from_this(); }

 protected:
  virtual bool ParseTrainData(const rapidjson::Value& obj, sdk::VectorWithId& vector_with_id) const = 0;
  virtual Dataset::TestEntryPtr ParseTestData(const rapidjson::Value& obj) const = 0;

 private:
  void ParallelLoadTrainData(const std::vector<std::string>& filepaths);
  uint32_t LoadTrainData(std::shared_ptr<rapidjson::Document> doc, uint32_t offset, uint32_t size,
                         std::vector<sdk::VectorWithId>& vector_with_ids) const;

  bool HandleScalarAndNeighborsJson();
  bool HandleNeighborsJson();
  bool ParseScalarLabelsJson(const std::string& json_file);
  bool ParseNeighborsLabelsJson(const std::string& json_file);

  std::string dirpath_;

  // train dataset
  std::vector<std::string> train_filepaths_;
  std::atomic<bool> train_load_finish_{false};
  std::vector<BatchVectorEntryPtr> batch_vector_entry_cache_;
  int head_pos_{0};
  int tail_pos_{0};
  int64_t train_data_count_{0};
  std::mutex mutex_;

  // test dataset
  std::vector<std::string> test_filepaths_;

  uint32_t test_row_count_{0};

  std::thread train_thread_;

 protected:
  // key : id ; value : label string
  std::shared_ptr<std::unordered_map<int64_t, std::string>> scalar_labels_map;

  // key : id ; value : label string  {"id":0,"neighbors_id":[662817,763377,...]}
  std::shared_ptr<std::unordered_map<int64_t, std::vector<int64_t>>> neighbors_id_map;
};

class Wikipedia2212Dataset : public JsonDataset {
 public:
  Wikipedia2212Dataset(const std::string& dirpath) : JsonDataset(dirpath) {}
  ~Wikipedia2212Dataset() override = default;

  bool ParseTrainData(const rapidjson::Value& obj, sdk::VectorWithId& vector_with_id) const override;
  Dataset::TestEntryPtr ParseTestData(const rapidjson::Value& obj) const override;
  std::string GetType() override { return "Wikipedia2212Dataset"; }
};

class BeirBioasqDataset : public JsonDataset {
 public:
  BeirBioasqDataset(const std::string& dirpath) : JsonDataset(dirpath) {}
  ~BeirBioasqDataset() override = default;

  bool ParseTrainData(const rapidjson::Value& obj, sdk::VectorWithId& vector_with_id) const override;
  Dataset::TestEntryPtr ParseTestData(const rapidjson::Value& obj) const override;
  std::string GetType() override { return "BeirBioasqDataset"; }
};

class MiraclDataset : public JsonDataset {
 public:
  MiraclDataset(const std::string& dirpath) : JsonDataset(dirpath) {}
  ~MiraclDataset() override = default;

  bool ParseTrainData(const rapidjson::Value& obj, sdk::VectorWithId& vector_with_id) const override;
  Dataset::TestEntryPtr ParseTestData(const rapidjson::Value& obj) const override;
  std::string GetType() override { return "MiraclDataset"; }
};

class BioasqMediumDataset : public JsonDataset {
 public:
  BioasqMediumDataset(const std::string& dirpath) : JsonDataset(dirpath) {}
  ~BioasqMediumDataset() override = default;

  bool ParseTrainData(const rapidjson::Value& obj, sdk::VectorWithId& vector_with_id) const override;
  Dataset::TestEntryPtr ParseTestData(const rapidjson::Value& obj) const override;
  std::string GetType() override { return "BioasqMediumDataset"; }

 private:
  mutable std::atomic<bool> already_set_label_name_{false};
  mutable std::string label_name_;
};

class OpenaiLargeDataset : public BioasqMediumDataset {
 public:
  OpenaiLargeDataset(const std::string& dirpath) : BioasqMediumDataset(dirpath) {}
  ~OpenaiLargeDataset() override = default;

  bool ParseTrainData(const rapidjson::Value& obj, sdk::VectorWithId& vector_with_id) const override;
  Dataset::TestEntryPtr ParseTestData(const rapidjson::Value& obj) const override;
  std::string GetType() override { return "OpenaiLargeDataset"; }
};

}  // namespace benchmark
}  // namespace dingodb

#endif  // DINGODB_BENCHMARK_DATASET_H_
