/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainUpdateEdgeRemoteProcessor.h"

#include "storage/mutate/UpdateEdgeProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

using Code = ::nebula::cpp2::ErrorCode;

void ChainUpdateEdgeRemoteProcessor::process(const cpp2::ChainUpdateEdgeRequest& req) {
  auto rc = Code::SUCCEEDED;
  if (!checkTerm(req)) {
    LOG(WARNING) << "invalid term";
    rc = Code::E_OUTDATED_TERM;
  }

  auto& updateRequest = req.get_update_edge_request();
  if (rc != Code::SUCCEEDED) {
    pushResultCode(rc, updateRequest.get_part_id());
  } else {
    updateEdge(req);
  }
  onFinished();
}

bool ChainUpdateEdgeRemoteProcessor::checkTerm(const cpp2::ChainUpdateEdgeRequest& req) {
  auto partId = req.get_update_edge_request().get_part_id();
  return env_->txnMan_->checkTerm(req.get_space_id(), partId, req.get_term());
}

// forward to UpdateEdgeProcessor
void ChainUpdateEdgeRemoteProcessor::updateEdge(const cpp2::ChainUpdateEdgeRequest& req) {
  auto* proc = UpdateEdgeProcessor::instance(env_, counters_);
  auto f = proc->getFuture();
  proc->process(req.get_update_edge_request());
  auto resp = std::move(f).get();
  std::swap(resp_, resp);
}

}  // namespace storage
}  // namespace nebula
