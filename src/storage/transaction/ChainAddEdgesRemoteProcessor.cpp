/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainAddEdgesRemoteProcessor.h"

#include <folly/String.h>                   // for hexlify
#include <folly/futures/Future.h>           // for SemiFuture::rele...
#include <folly/futures/Future.h>           // for Future
#include <folly/futures/Future.h>           // for SemiFuture::rele...
#include <folly/futures/Future.h>           // for Future
#include <folly/futures/Promise.h>          // for Promise::Promise<T>
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <memory>         // for allocator_traits...
#include <ostream>        // for operator<<, basi...
#include <type_traits>    // for remove_reference_t
#include <unordered_map>  // for _Node_const_iter...

#include "clients/meta/MetaClient.h"                 // for MetaClient
#include "common/base/Logging.h"                     // for LogMessage, COMP...
#include "common/base/StatusOr.h"                    // for StatusOr
#include "common/utils/NebulaKeyUtils.h"             // for NebulaKeyUtils
#include "interface/gen-cpp2/common_types.h"         // for ErrorCode, Error...
#include "storage/BaseProcessor.h"                   // for BaseProcessor::h...
#include "storage/CommonUtils.h"                     // for StorageEnv
#include "storage/StorageFlags.h"                    // for FLAGS_trace_toss
#include "storage/mutate/AddEdgesProcessor.h"        // for AddEdgesProcessor
#include "storage/transaction/ChainBaseProcessor.h"  // for Code
#include "storage/transaction/ConsistUtil.h"         // for ConsistUtil
#include "storage/transaction/TransactionManager.h"  // for TransactionManager

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
