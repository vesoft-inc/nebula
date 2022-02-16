/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef GRAPH_OPTIMIZER_INDEXSCANRULE_H_
#define GRAPH_OPTIMIZER_INDEXSCANRULE_H_

#include <gtest/gtest_prod.h>  // for FRIEND_TEST
#include <stddef.h>            // for size_t
#include <stdint.h>            // for int32_t

#include <memory>       // for shared_ptr, uniq...
#include <string>       // for string, basic_st...
#include <type_traits>  // for is_same, enable_...
#include <vector>       // for vector

#include "common/base/Status.h"                      // for Status
#include "common/base/StatusOr.h"                    // for StatusOr
#include "common/datatypes/Value.h"                  // for Value
#include "common/expression/Expression.h"            // for Expression (ptr ...
#include "common/expression/RelationalExpression.h"  // for RelationalExpres...
#include "common/thrift/ThriftTypes.h"               // for GraphSpaceID
#include "graph/optimizer/OptRule.h"                 // for MatchedResult (p...
#include "graph/optimizer/OptimizerUtils.h"
#include "graph/planner/plan/Query.h"  // for IndexScan, Index...

namespace nebula {
class EdgePropertyExpression;
class LabelTagPropertyExpression;
class TagPropertyExpression;
namespace graph {
class QueryContext;
}  // namespace graph
namespace meta {
namespace cpp2 {
class ColumnDef;
class IndexItem;
}  // namespace cpp2
}  // namespace meta
namespace opt {
class OptGroupNode;
}  // namespace opt
namespace storage {
namespace cpp2 {
class IndexColumnHint;
class IndexQueryContext;
}  // namespace cpp2
}  // namespace storage

class EdgePropertyExpression;
class LabelTagPropertyExpression;
class TagPropertyExpression;
namespace graph {
class QueryContext;
}  // namespace graph
namespace meta {
namespace cpp2 {
class ColumnDef;
class IndexItem;
}  // namespace cpp2
}  // namespace meta
namespace storage {
namespace cpp2 {
class IndexColumnHint;
class IndexQueryContext;
}  // namespace cpp2
}  // namespace storage

namespace opt {
class OptGroupNode;

using graph::QueryContext;
using storage::cpp2::IndexColumnHint;
using storage::cpp2::IndexQueryContext;
using IndexItem = std::shared_ptr<meta::cpp2::IndexItem>;

class OptContext;

class IndexScanRule final : public OptRule {
  FRIEND_TEST(IndexScanRuleTest, BoundValueTest);
  FRIEND_TEST(IndexScanRuleTest, IQCtxTest);
  FRIEND_TEST(IndexScanRuleTest, BoundValueRangeTest);

 public:
  const Pattern& pattern() const override;

  bool match(OptContext* ctx, const MatchedResult& matched) const override;
  StatusOr<TransformResult> transform(OptContext* ctx, const MatchedResult& matched) const override;
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
    std::string col_;
    RelationalExpression::Kind relOP_;
    Value value_;

    FilterItem(const std::string& col, RelationalExpression::Kind relOP, const Value& value)
        : col_(col), relOP_(relOP), value_(value) {}
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
    void addItem(const std::string& field, RelationalExpression::Kind kind, const Value& v) {
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

  Status createSingleIQC(std::vector<graph::IndexScan::IndexQueryContext>& iqctx,
                         const FilterItems& items,
                         graph::QueryContext* qctx,
                         const OptGroupNode* groupNode) const;

  Status createMultipleIQC(std::vector<graph::IndexScan::IndexQueryContext>& iqctx,
                           const FilterItems& items,
                           graph::QueryContext* qctx,
                           const OptGroupNode* groupNode) const;

  Status appendIQCtx(const IndexItem& index,
                     const FilterItems& items,
                     std::vector<graph::IndexScan::IndexQueryContext>& iqctx,
                     const Expression* filter = nullptr) const;

  Status appendIQCtx(const IndexItem& index,
                     std::vector<graph::IndexScan::IndexQueryContext>& iqctx) const;

  Status appendColHint(std::vector<IndexColumnHint>& hitns,
                       const FilterItems& items,
                       const meta::cpp2::ColumnDef& col) const;

  size_t hintCount(const FilterItems& items) const noexcept;

  bool isEdge(const OptGroupNode* groupNode) const;

  int32_t schemaId(const OptGroupNode* groupNode) const;

  GraphSpaceID spaceId(const OptGroupNode* groupNode) const;

  Expression* filterExpr(const OptGroupNode* groupNode) const;

  Status analyzeExpression(
      Expression* expr, FilterItems* items, ScanKind* kind, bool isEdge, QueryContext* qctx) const;

  template <typename E,
            typename = std::enable_if_t<std::is_same<E, EdgePropertyExpression>::value ||
                                        std::is_same<E, LabelTagPropertyExpression>::value ||
                                        std::is_same<E, TagPropertyExpression>::value>>
  Status addFilterItem(RelationalExpression* expr, FilterItems* items, QueryContext* qctx) const;

  IndexItem findOptimalIndex(graph::QueryContext* qctx,
                             const OptGroupNode* groupNode,
                             const FilterItems& items) const;

  IndexItem findLightestIndex(graph::QueryContext* qctx, const OptGroupNode* groupNode) const;

  std::vector<IndexItem> allIndexesBySchema(graph::QueryContext* qctx,
                                            const OptGroupNode* groupNode) const;

  std::vector<IndexItem> findValidIndex(graph::QueryContext* qctx,
                                        const OptGroupNode* groupNode,
                                        const FilterItems& items) const;

  std::vector<IndexItem> findIndexForEqualScan(const std::vector<IndexItem>& indexes,
                                               const FilterItems& items) const;

  std::vector<IndexItem> findIndexForRangeScan(const std::vector<IndexItem>& indexes,
                                               const FilterItems& items) const;

  bool isEmptyResultSet(const OptGroupNode* groupNode) const;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_INDEXSCANRULE_H_
