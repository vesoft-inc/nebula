/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainDeleteEdgesResumeRemoteProcessor.h"

#include "storage/StorageFlags.h"

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
