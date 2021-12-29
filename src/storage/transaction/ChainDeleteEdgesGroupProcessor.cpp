/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainDeleteEdgesGroupProcessor.h"

#include "storage/StorageFlags.h"
#include "storage/transaction/ChainDeleteEdgesLocalProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {
using ChainID = std::pair<PartitionID, PartitionID>;
using SplitedRequest = std::unordered_map<ChainID, cpp2::DeleteEdgesRequest>;

void ChainDeleteEdgesGroupProcessor::process(const cpp2::DeleteEdgesRequest& req) {
  auto spaceId = req.get_space_id();
  auto localPartId = req.get_parts().begin()->first;
  auto stSplitRequest = splitRequest(req);
  if (!stSplitRequest.ok()) {
    // TODO(liuyu): change this when error code done
    pushResultCode(Code::E_PART_NOT_FOUND, localPartId);
    onFinished();
  }

  SplitedRequest splitedRequest = stSplitRequest.value();

  callingNum_ = splitedRequest.size();

  auto fnSplit = [&](auto& request) {
    auto* proc = ChainDeleteEdgesLocalProcessor::instance(env_);
    proc->getFuture().thenValue([=](auto&& resp) {
      auto code = resp.get_result().get_failed_parts().empty()
                      ? nebula::cpp2::ErrorCode::SUCCEEDED
                      : resp.get_result().get_failed_parts().begin()->get_code();
      handleAsync(spaceId, localPartId, code);
    });
    proc->process(request.second);
  };

  std::for_each(splitedRequest.begin(), splitedRequest.end(), fnSplit);
}

StatusOr<SplitedRequest> ChainDeleteEdgesGroupProcessor::splitRequest(
    const cpp2::DeleteEdgesRequest& req) {
  SplitedRequest ret;
  auto numOfPart = env_->metaClient_->partsNum(req.get_space_id());
  if (!numOfPart.ok()) {
    return numOfPart.status();
  }
  auto partNum = numOfPart.value();

  for (auto& onePart : req.get_parts()) {
    auto localPartId = onePart.first;
    for (auto& edgeKey : onePart.second) {
      auto& remoteVid = edgeKey.get_dst().getStr();
      auto remotePartId = env_->metaClient_->partId(partNum, remoteVid);
      auto key = std::make_pair(localPartId, remotePartId);
      if (ret.count(key) == 0) {
        ret[key].space_id_ref() = req.get_space_id();
        if (req.common_ref()) {
          ret[key].common_ref() = req.common_ref().value();
        }
      }
      ret[key].parts_ref().value()[localPartId].emplace_back(edgeKey);
    }
  }

  return ret;
}

}  // namespace storage
}  // namespace nebula
