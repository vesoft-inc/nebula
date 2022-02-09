/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINBASEPROCESSOR_H
#define STORAGE_TRANSACTION_CHAINBASEPROCESSOR_H

#include "common/utils/MemoryLockWrapper.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

constexpr int32_t chainRetryLimit = 10;

using Code = ::nebula::cpp2::ErrorCode;

/**
 * @brief interface for all chain processor
 *
 */
class ChainBaseProcessor {
  friend class ChainProcessorFactory;

 public:
  virtual ~ChainBaseProcessor() = default;

  virtual folly::SemiFuture<Code> prepareLocal() {
    return Code::SUCCEEDED;
  }

  virtual folly::SemiFuture<Code> processRemote(Code code) {
    return code;
  }

  virtual folly::SemiFuture<Code> processLocal(Code code) {
    return code;
  }

  virtual folly::Future<Code> getFinished() {
    return finished_.getFuture();
  }

  virtual void finish() = 0;

 protected:
  Code rcPrepare_ = Code::SUCCEEDED;
  Code rcRemote_ = Code::E_UNKNOWN;
  Code rcCommit_ = Code::E_UNKNOWN;
  TermID term_;
  folly::Promise<Code> finished_;
};

}  // namespace storage
}  // namespace nebula
#endif
