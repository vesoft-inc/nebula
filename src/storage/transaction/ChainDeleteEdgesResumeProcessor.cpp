/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/transaction/ChainDeleteEdgesResumeProcessor.h"

#include <folly/futures/Future.h>           // for SemiFuture::rele...
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe

#include <ostream>  // for operator<<, basi...
#include <utility>  // for pair
#include <vector>   // for vector

#include "common/base/Logging.h"                     // for COMPACT_GOOGLE_L...
#include "interface/gen-cpp2/storage_types.h"        // for DeleteEdgesRequest
#include "kvstore/Common.h"                          // for KV
#include "storage/transaction/ChainBaseProcessor.h"  // for Code
#include "storage/transaction/ConsistUtil.h"         // for ConsistUtil, Del...

namespace nebula {
namespace storage {
class StorageEnv;

class StorageEnv;

ChainDeleteEdgesResumeProcessor::ChainDeleteEdgesResumeProcessor(StorageEnv* env,
                                                                 const std::string& val)
    : ChainDeleteEdgesLocalProcessor(env) {
  req_ = DeleteEdgesRequestHelper::parseDeleteEdgesRequest(val);
}

folly::SemiFuture<nebula::cpp2::ErrorCode> ChainDeleteEdgesResumeProcessor::prepareLocal() {
  rcPrepare_ = checkRequest(req_);
  primes_ = makePrime(req_);
  setPrime_ = true;
  rcPrepare_ = Code::SUCCEEDED;
  return rcPrepare_;
}

folly::SemiFuture<Code> ChainDeleteEdgesResumeProcessor::processRemote(Code code) {
  VLOG(1) << txnId_ << " prepareLocal() " << apache::thrift::util::enumNameSafe(code);
  return ChainDeleteEdgesLocalProcessor::processRemote(code);
}

folly::SemiFuture<Code> ChainDeleteEdgesResumeProcessor::processLocal(Code code) {
  VLOG(1) << txnId_ << " processRemote() " << apache::thrift::util::enumNameSafe(code);
  if (rcPrepare_ != Code::SUCCEEDED) {
    return rcPrepare_;
  }

  if (code == Code::E_RPC_FAILURE) {
    for (auto& kv : primes_) {
      auto key = ConsistUtil::doublePrimeTable(localPartId_)
                     .append(kv.first.substr(ConsistUtil::primeTable(localPartId_).size()));
      doublePrimes_.emplace_back(key, kv.second);
    }
  }

  if (rcRemote_ == Code::E_RPC_FAILURE || rcRemote_ == Code::SUCCEEDED) {
    // if there are something wrong other than rpc failure
    // we need to keep the resume retry(by not remove those prime key)
    return commitLocal();
  }

  return rcRemote_;
}

}  // namespace storage
}  // namespace nebula
