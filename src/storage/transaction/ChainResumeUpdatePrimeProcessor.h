/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINRESUMEUPDATEPRIMEPROCESSOR_H
#define STORAGE_TRANSACTION_CHAINRESUMEUPDATEPRIMEPROCESSOR_H

#include "storage/transaction/ChainUpdateEdgeLocalProcessor.h"

namespace nebula {
namespace storage {

/**
 * @brief
 *  if the TxnManager background resume thread found a prime key
 *  it will create this processor to resume the complete update process
 */
class ChainResumeUpdatePrimeProcessor : public ChainUpdateEdgeLocalProcessor {
 public:
  static ChainResumeUpdatePrimeProcessor* instance(StorageEnv* env, const std::string& val) {
    return new ChainResumeUpdatePrimeProcessor(env, val);
  }

  folly::SemiFuture<nebula::cpp2::ErrorCode> prepareLocal() override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processRemote(nebula::cpp2::ErrorCode code) override;

  folly::SemiFuture<nebula::cpp2::ErrorCode> processLocal(nebula::cpp2::ErrorCode code) override;

  virtual ~ChainResumeUpdatePrimeProcessor() = default;

 protected:
  ChainResumeUpdatePrimeProcessor(StorageEnv* env, const std::string& val);

  bool lockEdge();
};

}  // namespace storage
}  // namespace nebula
#endif
