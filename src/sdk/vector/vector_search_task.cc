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

#include "sdk/vector/vector_search_task.h"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "dingosdk/vector.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/index.pb.h"
#include "sdk/common/common.h"
#include "sdk/expression/langchain_expr.h"
#include "sdk/expression/langchain_expr_encoder.h"
#include "sdk/expression/langchain_expr_factory.h"
#include "sdk/utils/scoped_cleanup.h"
#include "sdk/vector/vector_common.h"

namespace dingodb {
namespace sdk {

Status VectorSearchTask::Init() {
  if (target_vectors_.empty()) {
    return Status::InvalidArgument("target_vectors is empty");
  }

  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetVectorIndexCache()->GetVectorIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  vector_index_ = std::move(tmp);

  WriteLockGuard guard(rw_lock_);

  auto part_ids = vector_index_->GetPartitionIds();

  for (const auto& part_id : part_ids) {
    next_part_ids_.emplace(part_id);
  }

  {
    // prepare search parameter
    FillInternalSearchParams(&search_parameter_, vector_index_->GetVectorIndexType(), search_param_);
    if (!search_param_.langchain_expr_json.empty()) {
      std::shared_ptr<expression::LangchainExpr> expr;

      std::unique_ptr<expression::LangchainExprFactory> expr_factory;
      if (vector_index_->HasScalarSchema()) {
        expr_factory = std::make_unique<expression::SchemaLangchainExprFactory>(vector_index_->GetScalarSchema());
      } else {
        expr_factory = std::make_unique<expression::LangchainExprFactory>();
      }
      DINGO_RETURN_NOT_OK(expr_factory->CreateExpr(search_param_.langchain_expr_json, expr));

      expression::LangChainExprEncoder encoder;
      *(search_parameter_.mutable_vector_coprocessor()) = encoder.EncodeToCoprocessor(expr.get());
    }
  }

  return Status::OK();
}

void VectorSearchTask::DoAsync() {
  std::set<int64_t> next_part_ids;
  {
    WriteLockGuard guard(rw_lock_);
    next_part_ids = next_part_ids_;
    status_ = Status::OK();
  }

  if (next_part_ids.empty()) {
    DoAsyncDone(Status::OK());
    return;
  }

  sub_tasks_count_.store(next_part_ids.size());

  for (const auto& part_id : next_part_ids) {
    auto* sub_task = new VectorSearchPartTask(stub, index_id_, part_id, search_parameter_, target_vectors_);
    sub_task->AsyncRun([this, sub_task](auto&& s) { SubTaskCallback(std::forward<decltype(s)>(s), sub_task); });
  }
}

void VectorSearchTask::SubTaskCallback(Status status, VectorSearchPartTask* sub_task) {
  SCOPED_CLEANUP({ delete sub_task; });

  if (!status.ok()) {
    DINGO_LOG(WARNING) << "sub_task: " << sub_task->Name() << " fail: " << status.ToString();

    WriteLockGuard guard(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    WriteLockGuard guard(rw_lock_);
    std::unordered_map<int64_t, std::vector<VectorWithDistance>>& sub_results = sub_task->GetSearchResult();
    // merge
    for (auto& result : sub_results) {
      auto iter = tmp_out_result_.find(result.first);
      if (iter != tmp_out_result_.cend()) {
        auto& origin = iter->second;
        auto& to_put = result.second;
        origin.reserve(origin.size() + to_put.size());
        std::move(to_put.begin(), to_put.end(), std::back_inserter(origin));
      } else {
        CHECK(tmp_out_result_.insert({result.first, std::move(result.second)}).second);
      }
    }

    next_part_ids_.erase(sub_task->part_id_);
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      ReadLockGuard guard(rw_lock_);
      ConstructResultUnlocked();
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

void VectorSearchTask::ConstructResultUnlocked() {
  for (const auto& vector_with_id : target_vectors_) {
    VectorWithId tmp;
    {
      //  NOTE: use copy
      const Vector& to_copy = vector_with_id.vector;
      tmp.vector.dimension = to_copy.dimension;
      tmp.vector.value_type = to_copy.value_type;
      tmp.vector.float_values = to_copy.float_values;
      tmp.vector.binary_values = to_copy.binary_values;
    }

    SearchResult search(std::move(tmp));

    out_result_.push_back(std::move(search));
  }

  for (auto& iter : tmp_out_result_) {
    auto& vec = iter.second;
    std::sort(vec.begin(), vec.end(),
              [](const VectorWithDistance& a, const VectorWithDistance& b) { return a.distance < b.distance; });
  }

  for (auto& iter : tmp_out_result_) {
    int64_t idx = iter.first;
    auto& vec_distance = iter.second;
    if (!search_param_.enable_range_search && search_param_.topk > 0 && search_param_.topk < vec_distance.size()) {
      vec_distance.resize(search_param_.topk);
    }

    out_result_[idx].vector_datas = std::move(vec_distance);
  }
}

Status VectorSearchPartTask::Init() {
  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetVectorIndexCache()->GetVectorIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  vector_index_ = std::move(tmp);

  return Status::OK();
}

void VectorSearchPartTask::DoAsync() {
  const auto& range = vector_index_->GetPartitionRange(part_id_);

  Status s = stub.GetMetaCache()->ScanRegionsBetweenContinuousRange(range.start_key(), range.end_key(), regions_);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  {
    WriteLockGuard guard(rw_lock_);
    search_result_.clear();
    status_ = Status::OK();
  }

  controllers_.clear();
  rpcs_.clear();
  nodata_region_ids_.clear();
  nodata_rpcs_.clear();

  for (int i = 0; i < regions_.size(); i++) {
    auto rpc = std::make_unique<VectorSearchRpc>();
    auto region = regions_[i];
    FillVectorSearchRpcRequest(rpc->MutableRequest(), region);
    region_id_to_region_index_[region->RegionId()] = i;
    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(controller);

    rpcs_.push_back(std::move(rpc));
  }

  DCHECK_EQ(rpcs_.size(), regions_.size());
  DCHECK_EQ(rpcs_.size(), controllers_.size());

  sub_tasks_count_.store(regions_.size());

  for (auto i = 0; i < regions_.size(); i++) {
    auto& controller = controllers_[i];

    controller.AsyncCall(
        [this, rpc = rpcs_[i].get()](auto&& s) { VectorSearchRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void VectorSearchPartTask::FillVectorSearchRpcRequest(pb::index::VectorSearchRequest* request,
                                                      const std::shared_ptr<Region>& region) {
  FillRpcContext(*request->mutable_context(), region->RegionId(), region->Epoch());
  *(request->mutable_parameter()) = search_parameter_;
  for (const auto& vector_id : target_vectors_) {
    // NOTE* vector_id is useless
    FillVectorWithIdPB(request->add_vector_with_ids(), vector_id, false);
  }
}

void VectorSearchPartTask::VectorSearchRpcCallback(const Status& status, VectorSearchRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    if (pb::error::Errno::EDISKANN_IS_NO_DATA == status.Errno()) {
      DINGO_LOG(INFO) << " nodata region id : " << rpc->Request()->context().region_id();
      nodata_region_ids_.push_back(rpc->Request()->context().region_id());

    } else {
      if (status_.ok()) {
        WriteLockGuard guard(rw_lock_);
        // only return first fail status
        status_ = status;
      }
    }
  } else {
    if (rpc->Response()->batch_results_size() != rpc->Request()->vector_with_ids_size()) {
      DINGO_LOG(INFO) << Name() << " rpc: " << rpc->Method()
                      << " request vector_with_ids_size: " << rpc->Request()->vector_with_ids_size()
                      << " response batch_results_size: " << rpc->Response()->batch_results_size();
    }

    {
      WriteLockGuard guard(rw_lock_);
      for (auto i = 0; i < rpc->Response()->batch_results_size(); i++) {
        for (const auto& distancepb : rpc->Response()->batch_results(i).vector_with_distances()) {
          VectorWithDistance distance = InternalVectorWithDistance2VectorWithDistance(distancepb);
          search_result_[i].push_back(std::move(distance));
        }
      }
    }
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    CheckNoDataRegion();
  }
}

void VectorSearchPartTask::CheckNoDataRegion() {
  if (!status_.ok() || nodata_region_ids_.empty()) {
    Done();
  } else {
    SearchByBruteForce();
  }
}

void VectorSearchPartTask::Done() {
  Status tmp;
  {
    ReadLockGuard guard(rw_lock_);
    tmp = status_;
  }
  DoAsyncDone(tmp);
}

void VectorSearchPartTask::SearchByBruteForce() {
  pb::common::VectorSearchParameter paramer = search_parameter_;
  paramer.clear_diskann();
  paramer.set_use_brute_force(true);

  for (auto region_id : nodata_region_ids_) {
    auto rpc = std::make_unique<VectorSearchRpc>();
    CHECK(region_id_to_region_index_.find(region_id) != region_id_to_region_index_.end());
    auto region_index = region_id_to_region_index_[region_id];
    auto region = regions_[region_index];
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch());
    *(rpc->MutableRequest()->mutable_parameter()) = paramer;
    for (const auto& vector_id : target_vectors_) {
      FillVectorWithIdPB(rpc->MutableRequest()->add_vector_with_ids(), vector_id, false);
    }
    StoreRpcController controller(stub, *rpc, region);
    nodata_controllers_.push_back(controller);
    nodata_rpcs_.push_back(std::move(rpc));
  }
  DCHECK_EQ(nodata_rpcs_.size(), nodata_region_ids_.size());
  DCHECK_EQ(nodata_rpcs_.size(), nodata_controllers_.size());

  nodata_tasks_count_.store(nodata_region_ids_.size());
  for (auto i = 0; i < nodata_region_ids_.size(); i++) {
    auto& controller = nodata_controllers_[i];
    controller.AsyncCall([this, search_rpc = nodata_rpcs_[i].get()](auto&& s) {
      NodataRegionRpcCallback(std::forward<decltype(s)>(s), search_rpc);
    });
  }
}

void VectorSearchPartTask::NodataRegionRpcCallback(const Status& status, VectorSearchRpc* rpc) {
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    if (status_.ok()) {
      WriteLockGuard guard(rw_lock_);
      // only return first fail status
      status_ = status;
    }

  } else {
    if (rpc->Response()->batch_results_size() != rpc->Request()->vector_with_ids_size()) {
      DINGO_LOG(INFO) << Name() << " rpc: " << rpc->Method()
                      << " request vector_with_ids_size: " << rpc->Request()->vector_with_ids_size()
                      << " response batch_results_size: " << rpc->Response()->batch_results_size();
    }

    {
      WriteLockGuard guard(rw_lock_);
      for (auto i = 0; i < rpc->Response()->batch_results_size(); i++) {
        for (const auto& distancepb : rpc->Response()->batch_results(i).vector_with_distances()) {
          VectorWithDistance distance = InternalVectorWithDistance2VectorWithDistance(distancepb);
          search_result_[i].push_back(std::move(distance));
        }
      }
    }
  }

  if (nodata_tasks_count_.fetch_sub(1) == 1) {
    Done();
  }
}

}  // namespace sdk
}  // namespace dingodb