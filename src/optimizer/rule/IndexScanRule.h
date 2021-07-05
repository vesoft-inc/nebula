/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef OPTIMIZER_INDEXSCANRULE_H_
#define OPTIMIZER_INDEXSCANRULE_H_

#include "optimizer/OptRule.h"
#include "planner/plan/Query.h"
#include "optimizer/OptimizerUtils.h"

namespace nebula {
namespace opt {

using graph::QueryContext;
using storage::cpp2::IndexQueryContext;
using storage::cpp2::IndexColumnHint;
using BVO = graph::OptimizerUtils::BoundValueOperator;
using IndexItem = std::shared_ptr<meta::cpp2::IndexItem>;

class OptContext;

class IndexScanRule final : public OptRule {
    FRIEND_TEST(IndexScanRuleTest, BoundValueTest);
    FRIEND_TEST(IndexScanRuleTest, IQCtxTest);
    FRIEND_TEST(IndexScanRuleTest, BoundValueRangeTest);

public:
    const Pattern& pattern() const override;

    bool match(OptContext* ctx, const MatchedResult& matched) const override;
    StatusOr<TransformResult> transform(OptContext* ctx,
                                        const MatchedResult& matched) const override;
    std::string toString() const override;

private:
    struct ScanKind {
        enum class Kind {
            kUnknown = 0,
            kMultipleScan,
            kSingleScan,
        };

    private:
        Kind kind_;

    public:
        ScanKind() {
            kind_ = Kind::kUnknown;
        }
        void setKind(Kind k) {
            kind_ = k;
        }
        Kind getKind() {
            return kind_;
        }
        bool isSingleScan() {
            return kind_ == Kind::kSingleScan;
        }
    };

    // col_   : index column name
    // relOP_ : Relational operator , for example c1 > 1 , the relOP_ == kRelGT
    //                                            1 > c1 , the relOP_ == kRelLT
    // value_ : Constant value. from ConstantExpression.
    struct FilterItem {
        std::string                 col_;
        RelationalExpression::Kind  relOP_;
        Value                       value_;

        FilterItem(const std::string& col,
                   RelationalExpression::Kind relOP,
                   const Value& value)
                   : col_(col)
                   , relOP_(relOP)
                   , value_(value) {}
    };

    // FilterItems used for optimal index fetch and index scan context optimize.
    // for example : where c1 > 1 and c1 < 2 , the FilterItems should be :
    //               {c1, kRelGT, 1} , {c1, kRelLT, 2}
    struct FilterItems {
        std::vector<FilterItem> items;
        FilterItems() {}
        explicit FilterItems(const std::vector<FilterItem>& i) {
            items = i;
        }
        void addItem(const std::string& field,
                      RelationalExpression::Kind kind,
                      const Value& v) {
            items.emplace_back(FilterItem(field, kind, v));
        }
    };

    static std::unique_ptr<OptRule> kInstance;

    IndexScanRule();

    Status createIndexQueryCtx(std::vector<graph::IndexScan::IndexQueryContext>& iqctx,
                               ScanKind kind,
                               const FilterItems& items,
                               graph::QueryContext* qctx,
                               const OptGroupNode* groupNode) const;

    Status createIndexQueryCtx(std::vector<graph::IndexScan::IndexQueryContext>& iqctx,
                               graph::QueryContext* qctx,
                               const OptGroupNode* groupNode) const;

    Status createSingleIQC(std::vector<graph::IndexScan::IndexQueryContext> &iqctx,
                           const FilterItems& items,
                           graph::QueryContext *qctx,
                           const OptGroupNode *groupNode) const;

    Status createMultipleIQC(std::vector<graph::IndexScan::IndexQueryContext> &iqctx,
                             const FilterItems& items,
                             graph::QueryContext *qctx,
                             const OptGroupNode *groupNode) const;

    Status appendIQCtx(const IndexItem& index,
                       const FilterItems& items,
                       std::vector<graph::IndexScan::IndexQueryContext> &iqctx,
                       const std::string& filter = "") const;

    Status appendIQCtx(const IndexItem& index,
                       std::vector<graph::IndexScan::IndexQueryContext> &iqctx) const;

    Status appendColHint(std::vector<IndexColumnHint>& hitns,
                         const FilterItems& items,
                         const meta::cpp2::ColumnDef& col) const;

    size_t hintCount(const FilterItems& items) const noexcept;

    bool isEdge(const OptGroupNode *groupNode) const;

    int32_t schemaId(const OptGroupNode *groupNode) const;

    GraphSpaceID spaceId(const OptGroupNode *groupNode) const;

    Expression* filterExpr(const OptGroupNode *groupNode) const;

    Status analyzeExpression(Expression* expr, FilterItems* items,
                             ScanKind* kind, bool isEdge) const;

    template <typename E,
              typename = std::enable_if_t<std::is_same<E, EdgePropertyExpression>::value ||
                                          std::is_same<E, TagPropertyExpression>::value>>
    Status addFilterItem(RelationalExpression* expr, FilterItems* items) const;

    IndexItem findOptimalIndex(graph::QueryContext *qctx,
                               const OptGroupNode *groupNode,
                               const FilterItems& items) const;

    IndexItem findLightestIndex(graph::QueryContext *qctx,
                                const OptGroupNode *groupNode) const;

    std::vector<IndexItem>
    allIndexesBySchema(graph::QueryContext *qctx, const OptGroupNode *groupNode) const;

    std::vector<IndexItem> findValidIndex(graph::QueryContext *qctx,
                                          const OptGroupNode *groupNode,
                                          const FilterItems& items) const;

    std::vector<IndexItem> findIndexForEqualScan(const std::vector<IndexItem>& indexes,
                                                 const FilterItems& items) const;

    std::vector<IndexItem> findIndexForRangeScan(const std::vector<IndexItem>& indexes,
                                                 const FilterItems& items) const;

    bool isEmptyResultSet(const OptGroupNode *groupNode) const;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_INDEXSCANRULE_H_
