/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINUPDATEEDGEPROCESSORREMOTE_H
#define STORAGE_TRANSACTION_CHAINUPDATEEDGEPROCESSORREMOTE_H

#include "common/utils/MemoryLockWrapper.h"
#include "storage/BaseProcessor.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

class ChainUpdateEdgeRemoteProcessor : public BaseProcessor<cpp2::UpdateResponse> {
 public:
  static ChainUpdateEdgeRemoteProcessor* instance(StorageEnv* env) {
    return new ChainUpdateEdgeRemoteProcessor(env);
  }

  void process(const cpp2::ChainUpdateEdgeRequest& req);

 private:
  explicit ChainUpdateEdgeRemoteProcessor(StorageEnv* env)
      : BaseProcessor<cpp2::UpdateResponse>(env) {}

  void updateEdge(const cpp2::ChainUpdateEdgeRequest& req);

  PartitionID getLocalPart(const cpp2::ChainUpdateEdgeRequest& req);

 private:
  std::unique_ptr<TransactionManager::LockGuard> lk_;
};

}  // namespace storage
}  // namespace nebula
#endif
