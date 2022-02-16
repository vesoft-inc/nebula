/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINUPDATEEDGEPROCESSORREMOTE_H
#define STORAGE_TRANSACTION_CHAINUPDATEEDGEPROCESSORREMOTE_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException...

#include <memory>   // for unique_ptr
#include <utility>  // for move

#include "common/thrift/ThriftTypes.h"  // for PartitionID
#include "common/utils/MemoryLockWrapper.h"
#include "interface/gen-cpp2/storage_types.h"        // for UpdateResponse
#include "storage/BaseProcessor.h"                   // for BaseProcessor
#include "storage/transaction/TransactionManager.h"  // for TransactionManag...

namespace nebula {
namespace storage {
class StorageEnv;

class StorageEnv;

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
