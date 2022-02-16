/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CHAINDELETEEDGESGROUPPROCESSOR_H
#define STORAGE_TRANSACTION_CHAINDELETEEDGESGROUPPROCESSOR_H

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Prom...
#include <folly/hash/Hash.h>        // for hash

#include <unordered_map>  // for unordered_map
#include <utility>        // for move, pair

#include "common/base/StatusOr.h"              // for StatusOr
#include "common/thrift/ThriftTypes.h"         // for PartitionID
#include "interface/gen-cpp2/storage_types.h"  // for DeleteEdgesRequest
#include "storage/BaseProcessor.h"             // for BaseProcessor
#include "storage/transaction/ChainBaseProcessor.h"
#include "storage/transaction/ConsistUtil.h"
#include "storage/transaction/TransactionManager.h"

namespace nebula {
namespace storage {
class StorageEnv;

class StorageEnv;

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
