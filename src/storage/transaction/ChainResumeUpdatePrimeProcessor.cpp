/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainResumeUpdatePrimeProcessor.h"

#include <folly/futures/Future.h>           // for SemiFuture::rele...
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe

#include <algorithm>  // for max
#include <tuple>      // for tie, tuple
#include <utility>    // for move, pair
#include <vector>     // for vector

#include "common/base/Logging.h"                     // for COMPACT_GOOGLE_L...
#include "interface/gen-cpp2/storage_types.h"        // for UpdateEdgeRequest
#include "storage/CommonUtils.h"                     // for StorageEnv
#include "storage/transaction/ChainBaseProcessor.h"  // for Code
#include "storage/transaction/ConsistUtil.h"         // for ConsistUtil
#include "storage/transaction/TransactionManager.h"  // for TransactionManager

namespace nebula {
namespace storage {

ChainResumeUpdatePrimeProcessor::ChainResumeUpdatePrimeProcessor(StorageEnv* env,
                                                                 const std::string& val)
    : ChainUpdateEdgeLocalProcessor(env) {
  req_ = ConsistUtil::parseUpdateRequest(val);
  ChainUpdateEdgeLocalProcessor::prepareRequest(req_);
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ChainResumeUpdatePrimeProcessor::prepareLocal() {
  VLOG(1) << " prepareLocal()";
  std::tie(term_, rcPrepare_) = env_->txnMan_->getTermFromKVStore(spaceId_, localPartId_);
  return rcPrepare_;
}

folly::SemiFuture<Code> ChainResumeUpdatePrimeProcessor::processRemote(Code code) {
  VLOG(1) << "prepareLocal()=" << apache::thrift::util::enumNameSafe(code);
  return ChainUpdateEdgeLocalProcessor::processRemote(code);
}

folly::SemiFuture<Code> ChainResumeUpdatePrimeProcessor::processLocal(Code code) {
  VLOG(1) << "processRemote()=" << apache::thrift::util::enumNameSafe(code);
  if (rcPrepare_ != Code::SUCCEEDED) {
    return rcPrepare_;
  }

  auto currTerm = env_->txnMan_->getTermFromKVStore(spaceId_, localPartId_);
  if (currTerm.first != term_) {
    rcCommit_ = Code::E_LEADER_CHANGED;
    return rcCommit_;
  }

  if (code == Code::E_RPC_FAILURE) {
    appendDoublePrime();
  }

  if (code == Code::E_RPC_FAILURE || code == Code::SUCCEEDED) {
    // if there are something wrong other than rpc failure
    // we need to keep the resume retry(by not remove those prime key)
    auto key = ConsistUtil::primeKey(spaceVidLen_, localPartId_, req_.get_edge_key());
    kvErased_.emplace_back(std::move(key));
    return commit();
  }

  return code;
}

}  // namespace storage
}  // namespace nebula
