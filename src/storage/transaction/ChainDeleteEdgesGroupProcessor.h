/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINDELETEEDGESGROUPPROCESSOR_H
#define STORAGE_TRANSACTION_CHAINDELETEEDGESGROUPPROCESSOR_H

#include "storage/BaseProcessor.h"
#include "storage/transaction/ChainBaseProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

class ChainDeleteEdgesGroupProcessor : public BaseProcessor<cpp2::ExecResponse> {
 public:
  static ChainDeleteEdgesGroupProcessor* instance(StorageEnv* env) {
    return new ChainDeleteEdgesGroupProcessor(env);
  }

  void process(const cpp2::DeleteEdgesRequest& req);

 protected:
  explicit ChainDeleteEdgesGroupProcessor(StorageEnv* env)
      : BaseProcessor<cpp2::ExecResponse>(env) {}

  using ChainID = std::pair<PartitionID, PartitionID>;
  using SplitedRequest = std::unordered_map<ChainID, cpp2::DeleteEdgesRequest>;
  StatusOr<SplitedRequest> splitRequest(const cpp2::DeleteEdgesRequest& src);
};

}  // namespace storage
}  // namespace nebula
#endif
