/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_
#define NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_

#include "graph/util/SchemaUtil.h"

namespace nebula {

class Expression;

namespace meta {
namespace cpp2 {
class ColumnDef;
class IndexItem;
}  // namespace cpp2
}  // namespace meta

namespace storage {
namespace cpp2 {
class IndexQueryContext;
}  // namespace cpp2
}  // namespace storage

namespace graph {

class IndexScan;

class OptimizerUtils {
 public:
  enum class BoundValueOperator {
    GREATER_THAN = 0,
    LESS_THAN,
    MAX,
    MIN,
  };

  OptimizerUtils() = delete;

  static Value boundValue(const meta::cpp2::ColumnDef& col,
                          BoundValueOperator op,
                          const Value& v = Value());

  static Value boundValueWithGT(const meta::cpp2::ColumnDef& col, const Value& v);

  static Value boundValueWithLT(const meta::cpp2::ColumnDef& col, const Value& v);

  static Value boundValueWithMax(const meta::cpp2::ColumnDef& col);

  static Value boundValueWithMin(const meta::cpp2::ColumnDef& col);

  static Value normalizeValue(const meta::cpp2::ColumnDef& col, const Value& v);

  static Status boundValue(Expression::Kind kind,
                           const Value& val,
                           const meta::cpp2::ColumnDef& col,
                           Value& begin,
                           Value& end);

  static void eraseInvalidIndexItems(
      int32_t schemaId, std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>* indexItems);

  // Find optimal index according to filter expression and all valid indexes.
  //
  // For relational condition expression:
  //   1. iterate all indexes
  //   2. select the best column hint for each index
  //     2.1. generate column hint according to the first field of index
  //
  // For logical condition expression(only logical `AND' expression):
  //   1. same steps as above 1, 2
  //   2. for multiple columns combined index:
  //     * iterate each field of index
  //     * iterate each operand expression of filter condition
  //     * collect all column hints generated by operand expression for each
  //     index field
  //     * process collected column hints, for example, merge the begin and end
  //     values of
  //       range scan
  //   3. sort all index results generated by each index
  //   4. select the largest score index result
  //   5. process the selected index result:
  //     * find the first not prefix column hint and ignore all followed hints
  //     except first
  //       range hint
  //     * check whether filter conditions are used, if not, place the unused
  //     expression parts
  //       into column hint filter
  //
  // For logical `OR' condition expression, use above steps to generate
  // different `IndexQueryContext' for each operand of filter condition, nebula
  // storage will union all results of multiple index contexts
  static bool findOptimalIndex(
      const Expression* condition,
      const std::vector<std::shared_ptr<nebula::meta::cpp2::IndexItem>>& indexItems,
      bool* isPrefixScan,
      nebula::storage::cpp2::IndexQueryContext* ictx);

  static void copyIndexScanData(const nebula::graph::IndexScan* from, nebula::graph::IndexScan* to);
};

}  // namespace graph
}  // namespace nebula
#endif  // NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_
