/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainAddEdgesRemoteProcessor.h"

#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

void ChainAddEdgesRemoteProcessor::process(const cpp2::ChainAddEdgesRequest& req) {
  uuid_ = ConsistUtil::strUUID();
  auto spaceId = req.get_space_id();
  auto edgeKey = req.get_parts().begin()->second.back().key();
  auto localPartId = NebulaKeyUtils::getPart(edgeKey->dst_ref()->getStr());
  auto localTerm = req.get_term();
  auto remotePartId = req.get_parts().begin()->first;
  auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
  do {
    if (!env_->txnMan_->checkTermFromCache(spaceId, localPartId, localTerm)) {
      LOG(WARNING) << uuid_ << " invalid term, incoming part " << remotePartId
                   << ", term = " << req.get_term();
      code = nebula::cpp2::ErrorCode::E_OUTDATED_TERM;
      break;
    }

    auto vIdLen = env_->metaClient_->getSpaceVidLen(spaceId);
    if (!vIdLen.ok()) {
      code = Code::E_INVALID_SPACEVIDLEN;
      break;
    } else {
      spaceVidLen_ = vIdLen.value();
    }
  } while (0);

  if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (FLAGS_trace_toss) {
      // need to do this after set spaceVidLen_
      auto keys = getStrEdgeKeys(req);
      for (auto& key : keys) {
        VLOG(2) << uuid_ << ", key = " << folly::hexlify(key);
      }
    }
    commit(req);
  } else {
    pushResultCode(code, remotePartId);
    onFinished();
  }
}

void ChainAddEdgesRemoteProcessor::commit(const cpp2::ChainAddEdgesRequest& req) {
  auto spaceId = req.get_space_id();
  auto* proc = AddEdgesProcessor::instance(env_);
  proc->getFuture().thenValue([=](auto&& resp) {
    Code rc = Code::SUCCEEDED;
    for (auto& part : resp.get_result().get_failed_parts()) {
      rc = part.code;
      handleErrorCode(part.code, spaceId, part.get_part_id());
    }
    VLOG(2) << uuid_ << " " << apache::thrift::util::enumNameSafe(rc);
    this->result_ = resp.get_result();
    this->onFinished();
  });
  proc->process(ConsistUtil::toAddEdgesRequest(req));
}

std::vector<std::string> ChainAddEdgesRemoteProcessor::getStrEdgeKeys(
    const cpp2::ChainAddEdgesRequest& req) {
  std::vector<std::string> ret;
  for (auto& edgesOfPart : req.get_parts()) {
    auto partId = edgesOfPart.first;
    for (auto& edge : edgesOfPart.second) {
      ret.emplace_back(ConsistUtil::edgeKey(spaceVidLen_, partId, edge.get_key()));
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace nebula
