/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainDeleteEdgesRemoteProcessor.h"

#include "storage/StorageFlags.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

void ChainDeleteEdgesRemoteProcessor::process(const cpp2::ChainDeleteEdgesRequest& chianReq) {
  txnId_ = chianReq.get_txn_id();
  cpp2::DeleteEdgesRequest req = DeleteEdgesRequestHelper::toDeleteEdgesRequest(chianReq);
  auto term = chianReq.get_term();
  txnId_ = chianReq.get_txn_id();
  auto partId = req.get_parts().begin()->first;
  auto code = nebula::cpp2::ErrorCode::SUCCEEDED;
  do {
    auto spaceId = req.get_space_id();
    if (!env_->txnMan_->checkTermFromCache(spaceId, partId, term)) {
      LOG(WARNING) << txnId_ << "outdate term, incoming part " << partId << ", term = " << term;
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
      auto keys = ConsistUtil::toStrKeys(req, spaceVidLen_);
      for (auto& key : keys) {
        VLOG(1) << txnId_ << ", key = " << folly::hexlify(key);
      }
    }
    commit(req);
  } else {
    pushResultCode(code, partId);
    onFinished();
  }
}

void ChainDeleteEdgesRemoteProcessor::commit(const cpp2::DeleteEdgesRequest& req) {
  auto spaceId = req.get_space_id();
  auto* proc = DeleteEdgesProcessor::instance(env_);
  proc->getFuture().thenValue([=](auto&& resp) {
    Code rc = Code::SUCCEEDED;
    for (auto& part : resp.get_result().get_failed_parts()) {
      rc = part.code;
      handleErrorCode(part.code, spaceId, part.get_part_id());
    }
    VLOG(1) << txnId_ << " " << apache::thrift::util::enumNameSafe(rc);
    this->result_ = resp.get_result();
    this->onFinished();
  });
  proc->process(req);
}

}  // namespace storage
}  // namespace nebula
