/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "storage/transaction/ChainAddEdgesLocalProcessor.h"

namespace nebula {
namespace storage {

class ResumeAddEdgeProcessor : public ChainAddEdgesLocalProcessor {
 public:
  static ResumeAddEdgeProcessor* instance(StorageEnv* env, const std::string& val) {
    return new ResumeAddEdgeProcessor(env, val);
  }

  folly::SemiFuture<nebula::cpp2::ErrorCode> prepareLocal() override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processRemote(nebula::cpp2::ErrorCode code) override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processLocal(nebula::cpp2::ErrorCode code) override;

  virtual ~ResumeAddEdgeProcessor() = default;

 protected:
  ResumeAddEdgeProcessor(StorageEnv* env, const std::string& val);
};

}  // namespace storage
}  // namespace nebula
