/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/utils/MemoryLockWrapper.h"
#include "storage/BaseProcessor.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

class ChainUpdateEdgeProcessorRemote : public BaseProcessor<cpp2::UpdateResponse> {
 public:
  static ChainUpdateEdgeProcessorRemote* instance(StorageEnv* env) {
    return new ChainUpdateEdgeProcessorRemote(env);
  }

  void process(const cpp2::ChainUpdateEdgeRequest& req);

 private:
  explicit ChainUpdateEdgeProcessorRemote(StorageEnv* env)
      : BaseProcessor<cpp2::UpdateResponse>(env) {}

  bool checkTerm(const cpp2::ChainUpdateEdgeRequest& req);

  bool checkVersion(const cpp2::ChainUpdateEdgeRequest& req);

  void updateEdge(const cpp2::ChainUpdateEdgeRequest& req);

 private:
  std::unique_ptr<TransactionManager::LockGuard> lk_;
};

}  // namespace storage
}  // namespace nebula
