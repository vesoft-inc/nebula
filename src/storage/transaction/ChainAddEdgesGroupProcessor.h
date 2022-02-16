/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINADDEDGESGROUPPROCESSOR_H
#define STORAGE_TRANSACTION_CHAINADDEDGESGROUPPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Prom...
#include <folly/hash/Hash.h>        // for hash

#include <unordered_map>  // for unordered_map
#include <utility>        // for move, pair

#include "common/thrift/ThriftTypes.h"         // for PartitionID
#include "interface/gen-cpp2/storage_types.h"  // for AddEdgesRequest, ExecR...
#include "storage/BaseProcessor.h"             // for BaseProcessor
#include "storage/transaction/ChainBaseProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {
class StorageEnv;

class StorageEnv;

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
#endif
