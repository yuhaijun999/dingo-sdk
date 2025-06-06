
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

#include "sdk/document/document_batch_query_task.h"

#include "sdk/common/common.h"
#include "sdk/document/document_helper.h"
#include "sdk/document/document_translater.h"

namespace dingodb {
namespace sdk {

Status DocumentBatchQueryTask::Init() {
  WriteLockGuard guard(rw_lock_);

  for (long id : query_param_.doc_ids) {
    if (id <= 0) {
      return Status::InvalidArgument("invalid document id: " + std::to_string(id));
    }

    if (!doc_ids_.insert(id).second) {
      return Status::InvalidArgument("duplicate document id: " + std::to_string(id));
    }
  }

  std::shared_ptr<DocumentIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetDocumentIndexCache()->GetDocumentIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  doc_index_ = std::move(tmp);

  return Status::OK();
}

void DocumentBatchQueryTask::DoAsync() {
  std::set<int64_t> next_batch;
  {
    WriteLockGuard guard(rw_lock_);
    next_batch = doc_ids_;
    status_ = Status::OK();
  }

  if (next_batch.empty()) {
    DoAsyncDone(Status::OK());
    return;
  }

  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<int64_t>> region_id_to_doc_ids;

  auto meta_cache = stub.GetMetaCache();

  for (const auto& id : next_batch) {
    std::shared_ptr<Region> tmp;
    Status s = meta_cache->LookupRegionByKey(document_helper::DocumentIdToRangeKey(*doc_index_, id), tmp);
    if (!s.ok()) {
      // TODO: continue
      DoAsyncDone(s);
      return;
    };

    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    region_id_to_doc_ids[tmp->RegionId()].push_back(id);
  }

  controllers_.clear();
  rpcs_.clear();

  for (const auto& entry : region_id_to_doc_ids) {
    auto region_id = entry.first;

    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<DocumentBatchQueryRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region_id, region->Epoch());
    rpc->MutableRequest()->set_without_scalar_data(!query_param_.with_scalar_data);

    if (query_param_.with_scalar_data) {
      for (const auto& select : query_param_.selected_keys) {
        rpc->MutableRequest()->add_selected_keys(select);
      }
    }

    for (const auto& id : entry.second) {
      rpc->MutableRequest()->add_document_ids(id);
    }

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(controller);

    rpcs_.push_back(std::move(rpc));
  }

  DCHECK_EQ(rpcs_.size(), region_id_to_doc_ids.size());
  DCHECK_EQ(rpcs_.size(), controllers_.size());

  sub_tasks_count_.store(region_id_to_doc_ids.size());

  for (auto i = 0; i < region_id_to_doc_ids.size(); i++) {
    auto& controller = controllers_[i];

    controller.AsyncCall(
        [this, rpc = rpcs_[i].get()](auto&& s) { DocumentBatchQueryRpcCallback(std::forward<decltype(s)>(s), rpc); });
  }
}

void DocumentBatchQueryTask::DocumentBatchQueryRpcCallback(const Status& status, DocumentBatchQueryRpc* rpc) {
  VLOG(kSdkVlogLevel) << "rpc: " << rpc->Method() << " request: " << rpc->Request()->DebugString()
                      << " response: " << rpc->Response()->DebugString();

  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    WriteLockGuard guard(rw_lock_);
    if (status_.ok()) {
      // only return first fail status
      status_ = status;
    }
  } else {
    CHECK_EQ(rpc->Response()->doucments_size(), rpc->Request()->document_ids_size())
        << Name() << ", rpc: " << rpc->Method() << " request doc_ids_size: " << rpc->Request()->document_ids_size()
        << " response vectors_size: " << rpc->Response()->doucments_size()
        << " request: " << rpc->Request()->DebugString() << " response: " << rpc->Response()->DebugString();

    WriteLockGuard guard(rw_lock_);
    for (const auto& doc_pb : rpc->Response()->doucments()) {
      if (doc_pb.id() > 0) {
        out_result_.docs.emplace_back(DocumentTranslater::InternalDocumentWithIdPB2DocWithId(doc_pb));
      }
    }

    for (const auto& doc_id : rpc->Request()->document_ids()) {
      doc_ids_.erase(doc_id);
    }
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      ReadLockGuard guard(rw_lock_);
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

}  // namespace sdk
}  // namespace dingodb