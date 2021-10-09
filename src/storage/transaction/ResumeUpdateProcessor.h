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
class ResumeUpdateProcessor : public ChainUpdateEdgeProcessorLocal {
 public:
  static ResumeUpdateProcessor* instance(StorageEnv* env, const std::string& val) {
    return new ResumeUpdateProcessor(env, val);
  }

  folly::SemiFuture<nebula::cpp2::ErrorCode> prepareLocal() override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processRemote(nebula::cpp2::ErrorCode code) override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processLocal(nebula::cpp2::ErrorCode code) override;

  void finish() override;

  virtual ~ResumeUpdateProcessor() = default;

 protected:
  ResumeUpdateProcessor(StorageEnv* env, const std::string& val);

  bool lockEdge();
};

}  // namespace storage
}  // namespace nebula
