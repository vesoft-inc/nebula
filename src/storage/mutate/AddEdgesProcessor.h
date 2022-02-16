/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_MUTATE_ADDEDGESPROCESSOR_H_
#define STORAGE_MUTATE_ADDEDGESPROCESSOR_H_

#include <folly/Optional.h>         // for Optional
#include <folly/Range.h>            // for StringPiece
#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Prom...

#include <functional>  // for function
#include <memory>      // for shared_ptr
#include <string>      // for string
#include <utility>     // for move
#include <vector>      // for vector

#include "common/base/Base.h"
#include "common/base/ErrorOr.h"  // for ErrorOr
#include "common/stats/StatsManager.h"
#include "common/thrift/ThriftTypes.h"         // for PartitionID, GraphSpaceID
#include "interface/gen-cpp2/common_types.h"   // for ErrorCode
#include "interface/gen-cpp2/storage_types.h"  // for ExecResponse, AddEdges...
#include "kvstore/Common.h"                    // for KV
#include "kvstore/LogEncoder.h"
#include "storage/BaseProcessor.h"  // for BaseProcessor
#include "storage/CommonUtils.h"    // for StorageEnv (ptr only)
#include "storage/StorageFlags.h"

namespace nebula {
class RowReader;
namespace kvstore {
class BatchHolder;
}  // namespace kvstore
namespace meta {
class SchemaProviderIf;
namespace cpp2 {
class IndexItem;
}  // namespace cpp2
}  // namespace meta

class RowReader;
namespace kvstore {
class BatchHolder;
}  // namespace kvstore
namespace meta {
class SchemaProviderIf;
namespace cpp2 {
class IndexItem;
}  // namespace cpp2
}  // namespace meta

namespace storage {

extern ProcessorCounters kAddEdgesCounters;

class AddEdgesProcessor : public BaseProcessor<cpp2::ExecResponse> {
  friend class TransactionManager;
  friend class ChainAddEdgesLocalProcessor;

 public:
  static AddEdgesProcessor* instance(StorageEnv* env,
                                     const ProcessorCounters* counters = &kAddEdgesCounters) {
    return new AddEdgesProcessor(env, counters);
  }

  void process(const cpp2::AddEdgesRequest& req);

  void doProcess(const cpp2::AddEdgesRequest& req);

  void doProcessWithIndex(const cpp2::AddEdgesRequest& req);

 private:
  AddEdgesProcessor(StorageEnv* env, const ProcessorCounters* counters)
      : BaseProcessor<cpp2::ExecResponse>(env, counters) {}

  ErrorOr<nebula::cpp2::ErrorCode, std::string> addEdges(PartitionID partId,
                                                         const std::vector<kvstore::KV>& edges);

  ErrorOr<nebula::cpp2::ErrorCode, std::string> findOldValue(PartitionID partId,
                                                             const folly::StringPiece& rawKey);

  std::vector<std::string> indexKeys(PartitionID partId,
                                     RowReader* reader,
                                     const folly::StringPiece& rawKey,
                                     std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
                                     const meta::SchemaProviderIf* latestSchema);

  void deleteDupEdge(std::vector<cpp2::NewEdge>& edges);

 private:
  GraphSpaceID spaceId_;
  std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;
  bool ifNotExists_{false};
  bool ignoreExistedIndex_{false};

  /// this is a hook function to keep out-edge and in-edge consist
  using ConsistOper = std::function<void(kvstore::BatchHolder&, std::vector<kvstore::KV>*)>;
  folly::Optional<ConsistOper> consistOp_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_ADDEDGESPROCESSOR_H_
