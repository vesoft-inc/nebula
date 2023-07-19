/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_INDEXFULLSCANBASERULE_H_
#define GRAPH_OPTIMIZER_RULE_INDEXFULLSCANBASERULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
class PlanNode;

namespace graph {
class IndexScan;
}  // namespace graph

namespace opt {

class IndexFullScanBaseRule : public OptRule {
 public:
  bool match(OptContext *ctx, const MatchedResult &matched) const override;
  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

 protected:
  virtual graph::IndexScan *scan(OptContext *ctx, const graph::PlanNode *node) const = 0;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_INDEXFULLSCANBASERULE_H_
