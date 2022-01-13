/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_RESUMEUPDATEPROCESSOR_H
#define STORAGE_TRANSACTION_RESUMEUPDATEPROCESSOR_H

#include "storage/transaction/ChainUpdateEdgeLocalProcessor.h"

namespace nebula {
namespace storage {

/**
 * @brief
 *  if the TxnManager background resume thread found a prime key
 *  it will create this processor to resume the complete update process
 */
class ResumeUpdateProcessor : public ChainUpdateEdgeLocalProcessor {
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
#endif
