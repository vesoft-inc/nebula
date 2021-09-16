/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_LOOKUPBASEPROCESSOR_H_
#define STORAGE_QUERY_LOOKUPBASEPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"
#include "storage/exec/AggregateNode.h"
#include "storage/exec/DeDupNode.h"
#include "storage/exec/IndexEdgeNode.h"
#include "storage/exec/IndexFilterNode.h"
#include "storage/exec/IndexOutputNode.h"
#include "storage/exec/IndexScanNode.h"
#include "storage/exec/IndexVertexNode.h"
#include "storage/exec/StoragePlan.h"

namespace nebula {
namespace storage {
using IndexFilterItem =
    std::unordered_map<int32_t, std::pair<std::unique_ptr<StorageExpressionContext>, Expression*>>;

template <typename REQ, typename RESP>
class LookupBaseProcessor : public BaseProcessor<RESP> {
 public:
  virtual ~LookupBaseProcessor() = default;

  virtual void process(const REQ& req) = 0;

 protected:
  LookupBaseProcessor(StorageEnv* env,
                      const ProcessorCounters* counters,
                      folly::Executor* executor = nullptr)
      : BaseProcessor<RESP>(env, counters), executor_(executor) {}

  virtual void onProcessFinished() = 0;

  nebula::cpp2::ErrorCode requestCheck(const cpp2::LookupIndexRequest& req);

  nebula::cpp2::ErrorCode pagingScanReqCheck(const cpp2::IndexSpec& indexSpec);

  nebula::cpp2::ErrorCode generalScanReqCheck(const cpp2::IndexSpec& indexSpec);

  nebula::cpp2::ErrorCode edgeSchemaCheck(const nebula::cpp2::SchemaID& schemaId);

  nebula::cpp2::ErrorCode tagSchemaCheck(const nebula::cpp2::SchemaID& schemaId);

  bool isOutsideIndex(Expression* filter, const meta::cpp2::IndexItem* index);

  StatusOr<StoragePlan<IndexID>> buildPlan(IndexFilterItem* filterItem, nebula::DataSet* result);

  StatusOr<StoragePlan<IndexID>> buildPagingScanPlan(IndexFilterItem* filterItem,
                                                     nebula::DataSet* result,
                                                     const cpp2::PagingScanContext& ctx);

  std::unique_ptr<IndexOutputNode<IndexID>> buildPlanBasic(
      nebula::DataSet* result,
      const cpp2::IndexQueryContext& ctx,
      StoragePlan<IndexID>& plan,
      bool hasNullableCol,
      const std::vector<meta::cpp2::ColumnDef>& fields);

  std::unique_ptr<IndexOutputNode<IndexID>> buildPlanBasic(
      nebula::DataSet* result,
      const cpp2::PagingScanContext& ctx,
      StoragePlan<IndexID>& plan,
      bool hasNullableCol,
      const std::vector<meta::cpp2::ColumnDef>& fields);

  std::unique_ptr<IndexOutputNode<IndexID>> buildPlanWithData(nebula::DataSet* result,
                                                              const cpp2::IndexQueryContext& ctx,
                                                              StoragePlan<IndexID>& plan);

  std::unique_ptr<IndexOutputNode<IndexID>> buildPlanWithData(nebula::DataSet* result,
                                                              const cpp2::PagingScanContext& ctx,
                                                              StoragePlan<IndexID>& plan);

  std::unique_ptr<IndexOutputNode<IndexID>> buildPlanWithFilter(nebula::DataSet* result,
                                                                const cpp2::IndexQueryContext& ctx,
                                                                StoragePlan<IndexID>& plan,
                                                                StorageExpressionContext* exprCtx,
                                                                Expression* exp);

  std::unique_ptr<IndexOutputNode<IndexID>> buildPlanWithFilter(nebula::DataSet* result,
                                                                const cpp2::PagingScanContext& ctx,
                                                                StoragePlan<IndexID>& plan,
                                                                StorageExpressionContext* exprCtx,
                                                                Expression* exp);

  std::unique_ptr<IndexOutputNode<IndexID>> buildPlanWithDataAndFilter(
      nebula::DataSet* result,
      const cpp2::IndexQueryContext& ctx,
      StoragePlan<IndexID>& plan,
      StorageExpressionContext* exprCtx,
      Expression* exp);

  std::unique_ptr<IndexOutputNode<IndexID>> buildPlanWithDataAndFilter(
      nebula::DataSet* result,
      const cpp2::PagingScanContext& ctx,
      StoragePlan<IndexID>& plan,
      StorageExpressionContext* exprCtx,
      Expression* exp);

  void profilePlan(StoragePlan<IndexID>& plan);

 protected:
  GraphSpaceID spaceId_;
  std::unique_ptr<PlanContext> planContext_;
  std::unique_ptr<RuntimeContext> context_;
  folly::Executor* executor_{nullptr};
  nebula::DataSet resultDataSet_;
  std::vector<nebula::DataSet> partResults_;
  std::vector<cpp2::IndexQueryContext> indexContexts_{};
  std::vector<cpp2::PagingScanContext> pagingScanContexts_{};
  std::vector<cpp2::PagingScanContext> nextScanContexts_{};
  std::vector<std::string> yieldCols_{};
  std::vector<IndexFilterItem> filterItems_;
  // Save schemas when column is out of index, need to read from data
  std::vector<std::shared_ptr<const meta::NebulaSchemaProvider>> schemas_;
  std::vector<size_t> deDupColPos_;
  int64_t limit_ = -1;
  bool isPagingScan_ = false;
  std::vector<PartitionID> parts_;
};

}  // namespace storage
}  // namespace nebula

#include "storage/index/LookupBaseProcessor-inl.h"
#endif  // STORAGE_QUERY_LOOKUPBASEPROCESSOR_H_
