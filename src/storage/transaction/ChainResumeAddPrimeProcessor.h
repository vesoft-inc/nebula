/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINRESUMEADDPRIMEPROCESSOR_H
#define STORAGE_TRANSACTION_CHAINRESUMEADDPRIMEPROCESSOR_H

#include <folly/futures/Future.h>  // for SemiFuture

#include <string>  // for string

#include "interface/gen-cpp2/common_types.h"                  // for ErrorCode
#include "storage/transaction/ChainAddEdgesLocalProcessor.h"  // for ChainAd...

namespace nebula {
namespace storage {
class StorageEnv;

class StorageEnv;

class ChainResumeAddPrimeProcessor : public ChainAddEdgesLocalProcessor {
 public:
  static ChainResumeAddPrimeProcessor* instance(StorageEnv* env, const std::string& val) {
    return new ChainResumeAddPrimeProcessor(env, val);
  }

  virtual ~ChainResumeAddPrimeProcessor() = default;

  folly::SemiFuture<nebula::cpp2::ErrorCode> prepareLocal() override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processRemote(nebula::cpp2::ErrorCode code) override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processLocal(nebula::cpp2::ErrorCode code) override;

 protected:
  ChainResumeAddPrimeProcessor(StorageEnv* env, const std::string& val);
};

}  // namespace storage
}  // namespace nebula
#endif
