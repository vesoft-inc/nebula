/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINDELETEEDGESRESUMEREMOTEPROCESSOR_H
#define STORAGE_TRANSACTION_CHAINDELETEEDGESRESUMEREMOTEPROCESSOR_H

#include <folly/futures/Future.h>  // for Semi...

#include <string>  // for string

#include "interface/gen-cpp2/common_types.h"                     // for Erro...
#include "storage/transaction/ChainDeleteEdgesLocalProcessor.h"  // for Chai...

namespace nebula {
namespace storage {
class StorageEnv;

class StorageEnv;

class ChainDeleteEdgesResumeRemoteProcessor : public ChainDeleteEdgesLocalProcessor {
 public:
  static ChainDeleteEdgesResumeRemoteProcessor* instance(StorageEnv* env, const std::string& val) {
    return new ChainDeleteEdgesResumeRemoteProcessor(env, val);
  }

  virtual ~ChainDeleteEdgesResumeRemoteProcessor() = default;

  folly::SemiFuture<nebula::cpp2::ErrorCode> prepareLocal() override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processRemote(nebula::cpp2::ErrorCode code) override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processLocal(nebula::cpp2::ErrorCode code) override;

  void finish() override;

 protected:
  ChainDeleteEdgesResumeRemoteProcessor(StorageEnv* env, const std::string& val);
};

}  // namespace storage
}  // namespace nebula
#endif
