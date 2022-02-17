/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainDeleteEdgesResumeRemoteProcessor.h"

#include <folly/Try.h>                      // for Try::~Try<T>
#include <folly/futures/Future.h>           // for SemiFuture::rele...
#include <folly/futures/Promise.h>          // for Promise::Promise<T>
#include <folly/futures/Promise.h>          // for PromiseException...
#include <folly/futures/Promise.h>          // for Promise::Promise<T>
#include <folly/futures/Promise.h>          // for PromiseException...
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe

#include <ext/alloc_traits.h>  // for __alloc_traits<>...
#include <tuple>               // for tie, _Swallow_as...
#include <unordered_map>       // for _Node_const_iter...
#include <utility>             // for move, pair
#include <vector>              // for vector

#include "common/base/Logging.h"                     // for COMPACT_GOOGLE_L...
#include "common/thrift/ThriftTypes.h"               // for TermID
#include "interface/gen-cpp2/storage_types.h"        // for DeleteEdgesRequest
#include "kvstore/KVStore.h"                         // for KVStore
#include "storage/BaseProcessor.h"                   // for BaseProcessor::p...
#include "storage/CommonUtils.h"                     // for StorageEnv
#include "storage/transaction/ChainBaseProcessor.h"  // for Code
#include "storage/transaction/ConsistTypes.h"        // for ResumeType, Resu...
#include "storage/transaction/ConsistUtil.h"         // for ConsistUtil, Del...
#include "storage/transaction/TransactionManager.h"  // for TransactionManager

namespace nebula {
namespace storage {

ChainDeleteEdgesResumeRemoteProcessor::ChainDeleteEdgesResumeRemoteProcessor(StorageEnv* env,
                                                                             const std::string& val)
    : ChainDeleteEdgesLocalProcessor(env) {
  req_ = DeleteEdgesRequestHelper::parseDeleteEdgesRequest(val);
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ChainDeleteEdgesResumeRemoteProcessor::prepareLocal() {
  rcPrepare_ = checkRequest(req_);
  return rcPrepare_;
}

folly::SemiFuture<Code> ChainDeleteEdgesResumeRemoteProcessor::processRemote(Code code) {
  VLOG(1) << txnId_ << " prepareLocal() " << apache::thrift::util::enumNameSafe(code);
  return ChainDeleteEdgesLocalProcessor::processRemote(code);
}

folly::SemiFuture<Code> ChainDeleteEdgesResumeRemoteProcessor::processLocal(Code code) {
  VLOG(1) << txnId_ << " processRemote() " << apache::thrift::util::enumNameSafe(code);
  if (code != Code::SUCCEEDED) {
    return code;
  }

  // if there are something wrong other than rpc failure
  // we need to keep the resume retry(by not remove double prime key)
  std::vector<std::string> doublePrimeKeys;
  for (auto& partOfKeys : req_.get_parts()) {
    std::string key;
    for (auto& edgeKey : partOfKeys.second) {
      doublePrimeKeys.emplace_back();
      doublePrimeKeys.back() =
          ConsistUtil::doublePrimeTable(localPartId_)
              .append(ConsistUtil::edgeKey(spaceVidLen_, localPartId_, edgeKey));
    }
  }

  auto [pro, fut] = folly::makePromiseContract<Code>();
  env_->kvstore_->asyncMultiRemove(spaceId_,
                                   localPartId_,
                                   std::move(doublePrimeKeys),
                                   [this, p = std::move(pro)](auto&& rc) mutable {
                                     rcCommit_ = rc;
                                     p.setValue(rc);
                                   });
  return std::move(fut);
}

void ChainDeleteEdgesResumeRemoteProcessor::finish() {
  VLOG(1) << " commitLocal() = " << apache::thrift::util::enumNameSafe(rcCommit_);
  TermID currTerm = 0;
  std::tie(currTerm, std::ignore) = env_->txnMan_->getTermFromKVStore(spaceId_, localPartId_);
  if (term_ == currTerm) {
    if (rcCommit_ != Code::SUCCEEDED || rcRemote_ != Code::SUCCEEDED) {
      reportFailed(ResumeType::RESUME_REMOTE);
    }
  } else {
    // transaction manager will do the clean.
  }
  pushResultCode(rcCommit_, localPartId_);
  finished_.setValue(rcCommit_);
  onFinished();
}

}  // namespace storage
}  // namespace nebula
