/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_OPTIMIZER_H_
#define GRAPH_OPTIMIZER_OPTIMIZER_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/thrift/ThriftTypes.h"

DECLARE_bool(enable_optimizer_property_pruner_rule);

namespace nebula {
namespace graph {
class PlanNode;
class QueryContext;
}  // namespace graph

namespace opt {

class OptContext;
class OptGroup;
class OptGroupNode;
class RuleSet;

class Optimizer final {
 public:
  explicit Optimizer(std::vector<const RuleSet *> ruleSets);
  ~Optimizer() = default;

  StatusOr<const graph::PlanNode *> findBestPlan(graph::QueryContext *qctx);

 private:
  Status postprocess(graph::PlanNode *root, graph::QueryContext *qctx, GraphSpaceID spaceID);

  StatusOr<OptGroup *> prepare(OptContext *ctx, graph::PlanNode *root);

  Status doExploration(OptContext *octx, OptGroup *rootGroup);

  OptGroup *convertToGroup(OptContext *ctx,
                           graph::PlanNode *node,
                           std::unordered_map<int64_t, OptGroup *> *visited);
  void addBodyToGroupNode(OptContext *ctx,
                          const graph::PlanNode *node,
                          OptGroupNode *gnode,
                          std::unordered_map<int64_t, OptGroup *> *visited);

  static Status rewriteArgumentInputVar(
      graph::PlanNode *root, std::unordered_set<const graph::PlanNode *> &visitedPlanNode);
  static Status rewriteArgumentInputVarInternal(
      graph::PlanNode *root,
      std::vector<const graph::PlanNode *> &path,
      std::unordered_set<const graph::PlanNode *> &visitedPlanNode);

  Status checkPlanDepth(const graph::PlanNode *root) const;

  static constexpr int8_t kMaxIterationRound = 5;

  std::vector<const RuleSet *> ruleSets_;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_OPTIMIZER_H_
