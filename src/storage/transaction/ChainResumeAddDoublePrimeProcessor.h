/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINRESUMEADDDOUBLEPRIMEPROCESSOR_H
#define STORAGE_TRANSACTION_CHAINRESUMEADDDOUBLEPRIMEPROCESSOR_H

#include "storage/transaction/ChainAddEdgesLocalProcessor.h"

namespace nebula {
namespace storage {

class ChainResumeAddDoublePrimeProcessor : public ChainAddEdgesLocalProcessor {
 public:
  static ChainResumeAddDoublePrimeProcessor* instance(StorageEnv* env, const std::string& val) {
    return new ChainResumeAddDoublePrimeProcessor(env, val);
  }

  virtual ~ChainResumeAddDoublePrimeProcessor() = default;

  folly::SemiFuture<nebula::cpp2::ErrorCode> prepareLocal() override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processRemote(nebula::cpp2::ErrorCode code) override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processLocal(nebula::cpp2::ErrorCode code) override;

  void finish() override;

 protected:
  ChainResumeAddDoublePrimeProcessor(StorageEnv* env, const std::string& val);
};

}  // namespace storage
}  // namespace nebula
#endif
