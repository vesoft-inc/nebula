/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainAddEdgesGroupProcessor.h"

#include "storage/StorageFlags.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/transaction/ChainAddEdgesLocalProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

void ChainAddEdgesGroupProcessor::process(const cpp2::AddEdgesRequest& req) {
  auto space = req.get_space_id();
  ShuffledReq shuffledReq;
  shuffleRequest(req, shuffledReq);

  callingNum_ = shuffledReq.size();

  auto delegateProcess = [&](auto& item) {
    auto localPartId = item.first.first;
    auto* proc = ChainAddEdgesLocalProcessor::instance(env_);
    proc->setRemotePartId(item.first.second);
    proc->getFuture().thenValue([=](auto&& resp) {
      auto code = resp.get_result().get_failed_parts().empty()
                      ? nebula::cpp2::ErrorCode::SUCCEEDED
                      : resp.get_result().get_failed_parts().begin()->get_code();
      handleAsync(space, localPartId, code);
    });
    proc->process(item.second);
  };

  std::for_each(shuffledReq.begin(), shuffledReq.end(), delegateProcess);
}

void ChainAddEdgesGroupProcessor::shuffleRequest(const cpp2::AddEdgesRequest& req,
                                                 ShuffledReq& shuffledReq) {
  auto numOfPart = env_->metaClient_->partsNum(req.get_space_id());
  if (!numOfPart.ok()) {
    return;
  }
  auto getPart = [&](auto& vid) { return env_->metaClient_->partId(numOfPart.value(), vid); };

  auto genNewReq = [&](auto& reqIn) {
    cpp2::AddEdgesRequest ret;
    ret.space_id_ref() = reqIn.get_space_id();
    ret.prop_names_ref() = reqIn.get_prop_names();
    ret.if_not_exists_ref() = reqIn.get_if_not_exists();
    return ret;
  };

  auto shufflePart = [&](auto&& part) {
    auto& localPart = part.first;

    auto shuffleEdge = [&](auto&& edge) {
      auto remotePart = getPart(edge.get_key().get_dst().getStr());
      auto key = std::make_pair(localPart, remotePart);
      auto it = shuffledReq.find(key);
      if (it == shuffledReq.end()) {
        shuffledReq[key] = genNewReq(req);
      }
      (*shuffledReq[key].parts_ref())[localPart].emplace_back(edge);
    };

    std::for_each(part.second.begin(), part.second.end(), shuffleEdge);
  };

  auto& parts = req.get_parts();
  std::for_each(parts.begin(), parts.end(), shufflePart);
}

}  // namespace storage
}  // namespace nebula
