/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/transaction/ChainUpdateEdgeProcessorLocal.h"

namespace nebula {
namespace storage {

/**
 * @brief
 *  if the TxnManager backgroud resume thread found a prime key
 *  it will create this processor to resume the complete update process
 */
class ResumeUpdateRemoteProcessor : public ChainUpdateEdgeProcessorLocal {
 public:
  static ResumeUpdateRemoteProcessor* instance(StorageEnv* env, const std::string& val) {
    return new ResumeUpdateRemoteProcessor(env, val);
  }

  folly::SemiFuture<nebula::cpp2::ErrorCode> prepareLocal() override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processRemote(nebula::cpp2::ErrorCode code) override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processLocal(nebula::cpp2::ErrorCode code) override;

  void finish() override;

  virtual ~ResumeUpdateRemoteProcessor() = default;

 protected:
  ResumeUpdateRemoteProcessor(StorageEnv* env, const std::string& val);

  bool lockEdge(const cpp2::UpdateEdgeRequest& req);
};

}  // namespace storage
}  // namespace nebula
