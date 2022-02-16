/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_GETEDGESTRANSFORMRULE_H
#define GRAPH_OPTIMIZER_RULE_GETEDGESTRANSFORMRULE_H

#include <memory>  // for unique_ptr
#include <string>  // for string

#include "common/base/StatusOr.h"     // for StatusOr
#include "graph/optimizer/OptRule.h"  // for MatchedResult (ptr only)

namespace nebula {

namespace nebula {
namespace graph {
class QueryContext;
}  // namespace graph
namespace opt {
class OptContext;
}  // namespace opt
}  // namespace nebula

namespace graph {
class ScanEdges;
class Project;
class Traverse;
class PlanNode;
class QueryContext;
}  // namespace graph

namespace opt {
class OptContext;

// e.g. match ()-[e]->(?) return e
// Optimize to get edges directly
class GetEdgesTransformRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;
  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  GetEdgesTransformRule();

  static graph::ScanEdges *traverseToScanEdges(const graph::Traverse *traverse);

  static graph::Project *projectEdges(graph::QueryContext *qctx,
                                      graph::PlanNode *input,
                                      const std::string &colName);

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
#endif
