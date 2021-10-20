/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

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
 public:
  virtual ~ChainBaseProcessor() = default;

  virtual folly::SemiFuture<Code> prepareLocal() { return Code::SUCCEEDED; }

  virtual folly::SemiFuture<Code> processRemote(Code code) { return code; }

  virtual folly::SemiFuture<Code> processLocal(Code code) { return code; }

  virtual folly::Future<Code> getFinished() { return finished_.getFuture(); }

  virtual void finish() = 0;

 protected:
  void setErrorCode(Code code) {
    if (code_ == Code::SUCCEEDED) {
      code_ = code;
    }
  }

 protected:
  Code code_ = Code::SUCCEEDED;
  folly::Promise<Code> finished_;
};

}  // namespace storage
}  // namespace nebula
