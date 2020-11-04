/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_LOOKUPBASEPROCESSOR_H_
#define STORAGE_QUERY_LOOKUPBASEPROCESSOR_H_

#include "common/base/Base.h"
#include "storage/BaseProcessor.h"
#include "storage/exec/StoragePlan.h"
#include "storage/exec/AggregateNode.h"
#include "storage/exec/IndexScanNode.h"
#include "storage/exec/IndexVertexNode.h"
#include "storage/exec/IndexEdgeNode.h"
#include "storage/exec/IndexFilterNode.h"
#include "storage/exec/IndexOutputNode.h"

namespace nebula {
namespace storage {
using IndexFilterItem =
    std::unordered_map<int32_t, std::pair<std::unique_ptr<StorageExpressionContext>,
                                          std::unique_ptr<Expression>>>;

template<typename REQ, typename RESP>
class LookupBaseProcessor : public BaseProcessor<RESP> {
public:
    virtual ~LookupBaseProcessor() = default;

    virtual void process(const REQ& req) = 0;

protected:
    LookupBaseProcessor(StorageEnv* env,
                        stats::Stats* stats = nullptr,
                        VertexCache* cache = nullptr)
        : BaseProcessor<RESP>(env, stats)
        , vertexCache_(cache) {}

    virtual void onProcessFinished() = 0;

    cpp2::ErrorCode requestCheck(const cpp2::LookupIndexRequest& req);

    bool isOutsideIndex(Expression* filter, const meta::cpp2::IndexItem* index);

    StatusOr<StoragePlan<IndexID>> buildPlan();

    std::unique_ptr<IndexOutputNode<IndexID>>
    buildPlanBasic(const cpp2::IndexQueryContext& ctx,
                   StoragePlan<IndexID>& plan,
                   bool hasNullableCol,
                   const std::vector<meta::cpp2::ColumnDef>& fields);

    std::unique_ptr<IndexOutputNode<IndexID>>
    buildPlanWithData(const cpp2::IndexQueryContext& ctx, StoragePlan<IndexID>& plan);

    std::unique_ptr<IndexOutputNode<IndexID>>
    buildPlanWithFilter(const cpp2::IndexQueryContext& ctx,
                        StoragePlan<IndexID>& plan,
                        StorageExpressionContext* exprCtx,
                        Expression* exp);

    std::unique_ptr<IndexOutputNode<IndexID>>
    buildPlanWithDataAndFilter(const cpp2::IndexQueryContext& ctx,
                               StoragePlan<IndexID>& plan,
                               StorageExpressionContext* exprCtx,
                               Expression* exp);

protected:
    GraphSpaceID                                spaceId_;
    std::unique_ptr<PlanContext>                planContext_;
    VertexCache*                                vertexCache_{nullptr};
    nebula::DataSet                             resultDataSet_;
    std::vector<cpp2::IndexQueryContext>        contexts_{};
    std::vector<std::string>                    yieldCols_{};
    IndexFilterItem                             filterItems_;
    // Used to identify no fields in the index
    bool                                        noProp_{false};
};
}  // namespace storage
}  // namespace nebula
#include "storage/index/LookupBaseProcessor.inl"
#endif  // STORAGE_QUERY_LOOKUPBASEPROCESSOR_H_
