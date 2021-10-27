/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/ChainAddEdgesProcessorRemote.h"

#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

void ChainAddEdgesProcessorRemote::process(const cpp2::ChainAddEdgesRequest& req) {
  if (FLAGS_trace_toss) {
    uuid_ = ConsistUtil::strUUID();
  }
  VLOG(1) << uuid_ << ConsistUtil::dumpParts(req.get_parts());
  auto partId = req.get_parts().begin()->first;
  auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
  do {
    if (!checkTerm(req)) {
      LOG(WARNING) << uuid_ << " invalid term, incoming part " << partId
                   << ", term = " << req.get_term();
      code = nebula::cpp2::ErrorCode::E_OUTDATED_TERM;
      break;
    }

    auto spaceId = req.get_space_id();
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
        LOG(INFO) << uuid_ << ", key = " << folly::hexlify(key);
      }
    }
    forwardRequest(req);
  } else {
    pushResultCode(code, partId);
    onFinished();
  }
}

bool ChainAddEdgesProcessorRemote::checkTerm(const cpp2::ChainAddEdgesRequest& req) {
  auto partId = req.get_parts().begin()->first;
  return env_->txnMan_->checkTerm(req.get_space_id(), partId, req.get_term());
}

void ChainAddEdgesProcessorRemote::forwardRequest(const cpp2::ChainAddEdgesRequest& req) {
  auto spaceId = req.get_space_id();
  auto* proc = AddEdgesProcessor::instance(env_);
  proc->getFuture().thenValue([=](auto&& resp) {
    Code rc = Code::SUCCEEDED;
    for (auto& part : resp.get_result().get_failed_parts()) {
      rc = part.code;
      handleErrorCode(part.code, spaceId, part.get_part_id());
    }
    VLOG(1) << uuid_ << " " << apache::thrift::util::enumNameSafe(rc);
    this->result_ = resp.get_result();
    this->onFinished();
  });
  proc->process(ConsistUtil::toAddEdgesRequest(req));
}

bool ChainAddEdgesProcessorRemote::checkVersion(const cpp2::ChainAddEdgesRequest& req) {
  if (!req.edge_version_ref()) {
    return true;
  }
  auto spaceId = req.get_space_id();
  auto partId = req.get_parts().begin()->first;
  auto strEdgeKeys = getStrEdgeKeys(req);
  auto currVer = ConsistUtil::getMultiEdgeVers(env_->kvstore_, spaceId, partId, strEdgeKeys);
  auto edgeVer = *req.edge_version_ref();
  for (auto i = 0U; i != currVer.size(); ++i) {
    if (currVer[i] > edgeVer) {
      LOG(WARNING) << "currVer[i]=" << currVer[i] << ", edgeVer=" << edgeVer;
      return false;
    }
  }
  return true;
}

std::vector<std::string> ChainAddEdgesProcessorRemote::getStrEdgeKeys(
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
