/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainUpdateEdgeRemoteProcessor.h"

#include <folly/futures/Future.h>                 // for Future::get
#include <proxygen/httpserver/ResponseBuilder.h>  // for HTTPMethod, HTTPMet...
#include <thrift/lib/cpp2/FieldRef.h>             // for field_ref

#include <ostream>      // for operator<<
#include <type_traits>  // for remove_reference...

#include "common/base/Logging.h"                     // for LOG, LogMessage
#include "common/utils/NebulaKeyUtils.h"             // for NebulaKeyUtils
#include "interface/gen-cpp2/common_types.h"         // for ErrorCode
#include "storage/BaseProcessor.h"                   // for BaseProcessor::p...
#include "storage/CommonUtils.h"                     // for StorageEnv
#include "storage/mutate/UpdateEdgeProcessor.h"      // for UpdateEdgeProcessor
#include "storage/transaction/TransactionManager.h"  // for TransactionManager

namespace nebula {
namespace storage {

using Code = ::nebula::cpp2::ErrorCode;

void ChainUpdateEdgeRemoteProcessor::process(const cpp2::ChainUpdateEdgeRequest& req) {
  auto rc = Code::SUCCEEDED;
  auto spaceId = req.get_space_id();
  auto localPartId = getLocalPart(req);
  auto localTerm = req.get_term();
  if (!env_->txnMan_->checkTermFromCache(spaceId, localPartId, localTerm)) {
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

PartitionID ChainUpdateEdgeRemoteProcessor::getLocalPart(const cpp2::ChainUpdateEdgeRequest& req) {
  auto& edgeKey = req.get_update_edge_request().get_edge_key();
  return NebulaKeyUtils::getPart(edgeKey.dst()->getStr());
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
