/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_MUTATE_ADDEDGESPROCESSOR_H_
#define STORAGE_MUTATE_ADDEDGESPROCESSOR_H_

#include "common/base/Base.h"
#include "common/stats/StatsManager.h"
#include "kvstore/LogEncoder.h"
#include "kvstore/raftex/RaftPart.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
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

  kvstore::MergeableAtomicOpResult addEdgesWithIndex(PartitionID partId,
                                                     std::vector<kvstore::KV>&& data);

  ErrorOr<nebula::cpp2::ErrorCode, std::string> findOldValue(PartitionID partId,
                                                             const folly::StringPiece& rawKey);

  std::vector<std::string> indexKeys(PartitionID partId,
                                     RowReader* reader,
                                     const folly::StringPiece& rawKey,
                                     std::shared_ptr<nebula::meta::cpp2::IndexItem> index,
                                     const meta::SchemaProviderIf* latestSchema);

  nebula::cpp2::ErrorCode deleteDupEdge(std::vector<cpp2::NewEdge>& edges);

 private:
  GraphSpaceID spaceId_;
  std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>> indexes_;
  bool ifNotExists_{false};
  bool ignoreExistedIndex_{false};

  /// this is a hook function to keep out-edge and in-edge consist
  using ConsistOper = std::function<void(kvstore::BatchHolder&, std::vector<kvstore::KV>*)>;
  std::optional<ConsistOper> consistOp_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_ADDEDGESPROCESSOR_H_
