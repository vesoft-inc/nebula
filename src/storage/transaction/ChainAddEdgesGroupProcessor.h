/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "storage/BaseProcessor.h"
#include "storage/transaction/ChainBaseProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {

class ChainAddEdgesGroupProcessor : public BaseProcessor<cpp2::ExecResponse> {
 public:
  static ChainAddEdgesGroupProcessor* instance(StorageEnv* env) {
    return new ChainAddEdgesGroupProcessor(env);
  }

  void process(const cpp2::AddEdgesRequest& req);

 protected:
  explicit ChainAddEdgesGroupProcessor(StorageEnv* env) : BaseProcessor<cpp2::ExecResponse>(env) {}

  using Chain = std::pair<PartitionID, PartitionID>;
  using ShuffledReq = std::unordered_map<Chain, cpp2::AddEdgesRequest>;
  void shuffleRequest(const cpp2::AddEdgesRequest& src, ShuffledReq& dst);
};

}  // namespace storage
}  // namespace nebula
