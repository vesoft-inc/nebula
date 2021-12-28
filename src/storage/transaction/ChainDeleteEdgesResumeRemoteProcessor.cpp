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
  code_ = checkRequest(req_);
  return code_;
}

folly::SemiFuture<Code> ChainDeleteEdgesResumeRemoteProcessor::processRemote(Code code) {
  VLOG(1) << txnId_ << " prepareLocal() " << apache::thrift::util::enumNameSafe(code);

  return ChainDeleteEdgesLocalProcessor::processRemote(code);
}

folly::SemiFuture<Code> ChainDeleteEdgesResumeRemoteProcessor::processLocal(Code code) {
  VLOG(1) << txnId_ << " processRemote() " << apache::thrift::util::enumNameSafe(code);

  setErrorCode(code);

  if (code == Code::E_RPC_FAILURE) {
    return code_;
  }

  if (code == Code::SUCCEEDED) {
    // if there are something wrong other than rpc failure
    // we need to keep the resume retry(by not remove double prime key)
    std::vector<std::string> doublePrimeKeys;
    for (auto& partOfKeys : req_.get_parts()) {
      std::string key;
      for (auto& edgeKey : partOfKeys.second) {
        doublePrimeKeys.emplace_back();
        doublePrimeKeys.back() = ConsistUtil::doublePrimeTable().append(
            ConsistUtil::edgeKey(spaceVidLen_, localPartId_, edgeKey));
      }
    }

    folly::Baton<true, std::atomic> baton;
    env_->kvstore_->asyncMultiRemove(
        spaceId_, localPartId_, std::move(doublePrimeKeys), [this, &baton](auto&& rc) {
          this->code_ = rc;
          baton.post();
        });
    baton.wait();
  }

  return code_;
}

}  // namespace storage
}  // namespace nebula
