/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainDeleteEdgesResumeProcessor.h"

#include <thrift/lib/cpp2/protocol/JSONProtocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

ChainDeleteEdgesResumeProcessor::ChainDeleteEdgesResumeProcessor(StorageEnv* env,
                                                                 const std::string& val)
    : ChainDeleteEdgesLocalProcessor(env) {
  req_ = DeleteEdgesRequestHelper::parseDeleteEdgesRequest(val);

  VLOG(1) << "explain req_: " << DeleteEdgesRequestHelper::explain(req_);
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ChainDeleteEdgesResumeProcessor::prepareLocal() {
  code_ = checkRequest(req_);
  primes_ = makePrime(req_);
  setPrime_ = true;
  return code_;
}

folly::SemiFuture<Code> ChainDeleteEdgesResumeProcessor::processRemote(Code code) {
  VLOG(1) << txnId_ << " prepareLocal() " << apache::thrift::util::enumNameSafe(code);
  return ChainDeleteEdgesLocalProcessor::processRemote(code);
}

folly::SemiFuture<Code> ChainDeleteEdgesResumeProcessor::processLocal(Code code) {
  VLOG(1) << txnId_ << " processRemote() " << apache::thrift::util::enumNameSafe(code);
  setErrorCode(code);

  if (code == Code::E_RPC_FAILURE) {
    for (auto& kv : primes_) {
      auto key =
          ConsistUtil::doublePrimeTable().append(kv.first.substr(ConsistUtil::primeTable().size()));
      doublePrimes_.emplace_back(key, kv.second);
    }
  }

  if (code == Code::E_RPC_FAILURE || code == Code::SUCCEEDED) {
    // if there are something wrong other than rpc failure
    // we need to keep the resume retry(by not remove those prime key)
    code_ = commitLocal().get();
    return code_;
  }

  return code_;
}

}  // namespace storage
}  // namespace nebula
