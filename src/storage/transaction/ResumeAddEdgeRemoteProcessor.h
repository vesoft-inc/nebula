/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_RESUMEADDEDGEREMOTEPROCESSOR_H
#define STORAGE_TRANSACTION_RESUMEADDEDGEREMOTEPROCESSOR_H

#include "storage/transaction/ChainAddEdgesLocalProcessor.h"

namespace nebula {
namespace storage {

class ResumeAddEdgeRemoteProcessor : public ChainAddEdgesLocalProcessor {
 public:
  static ResumeAddEdgeRemoteProcessor* instance(StorageEnv* env, const std::string& val) {
    return new ResumeAddEdgeRemoteProcessor(env, val);
  }

  folly::SemiFuture<nebula::cpp2::ErrorCode> prepareLocal() override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processRemote(nebula::cpp2::ErrorCode code) override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processLocal(nebula::cpp2::ErrorCode code) override;

  virtual ~ResumeAddEdgeRemoteProcessor() = default;

 protected:
  ResumeAddEdgeRemoteProcessor(StorageEnv* env, const std::string& val);
};

}  // namespace storage
}  // namespace nebula
#endif
