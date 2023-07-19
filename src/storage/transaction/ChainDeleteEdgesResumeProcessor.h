/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINDELETEEDGESRESUMEPROCESSOR_H
#define STORAGE_TRANSACTION_CHAINDELETEEDGESRESUMEPROCESSOR_H

#include "storage/transaction/ChainDeleteEdgesLocalProcessor.h"

namespace nebula {
namespace storage {

class ChainDeleteEdgesResumeProcessor : public ChainDeleteEdgesLocalProcessor {
 public:
  static ChainDeleteEdgesResumeProcessor* instance(StorageEnv* env, const std::string& val) {
    return new ChainDeleteEdgesResumeProcessor(env, val);
  }

  folly::SemiFuture<nebula::cpp2::ErrorCode> prepareLocal() override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processRemote(nebula::cpp2::ErrorCode code) override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processLocal(nebula::cpp2::ErrorCode code) override;

  virtual ~ChainDeleteEdgesResumeProcessor() = default;

 protected:
  ChainDeleteEdgesResumeProcessor(StorageEnv* env, const std::string& val);
};

}  // namespace storage
}  // namespace nebula
#endif
