/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_INDEX_LOOKUPPROCESSOR_H
#define STORAGE_INDEX_LOOKUPPROCESSOR_H
#include <folly/Try.h>                 // for Try::~Try<T>
#include <folly/futures/Promise.h>     // for PromiseException::Prom...
#include <stddef.h>                    // for size_t
#include <thrift/lib/cpp2/FieldRef.h>  // for optional_field_ref

#include <memory>   // for unique_ptr
#include <string>   // for string
#include <utility>  // for move, pair
#include <vector>   // for vector

#include "common/base/Base.h"
#include "common/base/ErrorOr.h"               // for ErrorOr
#include "common/datatypes/DataSet.h"          // for DataSet, Row
#include "common/thrift/ThriftTypes.h"         // for PartitionID
#include "interface/gen-cpp2/common_types.h"   // for ErrorCode
#include "interface/gen-cpp2/storage_types.h"  // for LookupIndexResp, StatType
#include "storage/BaseProcessor.h"             // for BaseProcessor
#include "storage/CommonUtils.h"               // for PlanContext, RuntimeCo...
#include "storage/exec/IndexNode.h"

namespace nebula {
namespace storage {
class IndexNode;
}  // namespace storage
}  // namespace nebula

namespace folly {
class Executor;

class Executor;
}  // namespace folly

namespace nebula {
namespace storage {
class IndexNode;

extern ProcessorCounters kLookupCounters;

class LookupProcessor : public BaseProcessor<cpp2::LookupIndexResp> {
 public:
  static LookupProcessor* instance(StorageEnv* env,
                                   const ProcessorCounters* counters = &kLookupCounters,
                                   folly::Executor* executor = nullptr) {
    return new LookupProcessor(env, counters, executor);
  }
  void process(const cpp2::LookupIndexRequest& req);

 private:
  LookupProcessor(StorageEnv* env, const ProcessorCounters* counters, folly::Executor* executor)
      : BaseProcessor<cpp2::LookupIndexResp>(env, counters), executor_(executor) {}
  void doProcess(const cpp2::LookupIndexRequest& req);
  void onProcessFinished() {
    BaseProcessor<cpp2::LookupIndexResp>::resp_.data_ref() = std::move(resultDataSet_);
    BaseProcessor<cpp2::LookupIndexResp>::resp_.stat_data_ref() = std::move(statsDataSet_);
  }
  void profilePlan(IndexNode* plan);
  void runInSingleThread(const std::vector<PartitionID>& parts, std::unique_ptr<IndexNode> plan);
  void runInMultipleThread(const std::vector<PartitionID>& parts, std::unique_ptr<IndexNode> plan);
  ::nebula::cpp2::ErrorCode prepare(const cpp2::LookupIndexRequest& req);
  ErrorOr<nebula::cpp2::ErrorCode, std::unique_ptr<IndexNode>> buildPlan(
      const cpp2::LookupIndexRequest& req);
  std::unique_ptr<IndexNode> buildOneContext(const cpp2::IndexQueryContext& ctx);
  std::vector<std::unique_ptr<IndexNode>> reproducePlan(IndexNode* root, size_t count);
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::pair<std::string, cpp2::StatType>>>
  handleStatProps(const std::vector<cpp2::StatProp>& statProps);
  void mergeStatsResult(const std::vector<Row>& statsResult);
  folly::Executor* executor_{nullptr};
  std::unique_ptr<PlanContext> planContext_;
  std::unique_ptr<RuntimeContext> context_;
  /**
   * @brief the final output
   */
  nebula::DataSet resultDataSet_;
  nebula::DataSet statsDataSet_;
  std::vector<nebula::DataSet> partResults_;
  std::vector<cpp2::StatType> statTypes_;
};
}  // namespace storage
}  // namespace nebula
#endif
