/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/Optimizer.h"

#include "graph/context/QueryContext.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/optimizer/OptRule.h"
#include "graph/planner/plan/ExecutionPlan.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/visitor/PrunePropertiesVisitor.h"

using nebula::graph::BinaryInputNode;
using nebula::graph::Loop;
using nebula::graph::PlanNode;
using nebula::graph::QueryContext;
using nebula::graph::Select;
using nebula::graph::SingleDependencyNode;

DEFINE_bool(enable_optimizer_property_pruner_rule, true, "");
DEFINE_uint64(max_plan_depth, 512, "The max depth of plan tree");

namespace nebula {
namespace opt {

Optimizer::Optimizer(std::vector<const RuleSet *> ruleSets) : ruleSets_(std::move(ruleSets)) {}

// Optimizer entrance
StatusOr<const PlanNode *> Optimizer::findBestPlan(QueryContext *qctx) {
  DCHECK(qctx != nullptr);
  auto optCtx = std::make_unique<OptContext>(qctx);

  auto root = qctx->plan()->root();
  auto spaceID = qctx->rctx()->session()->space().id;

  NG_RETURN_IF_ERROR(checkPlanDepth(root));
  auto ret = prepare(optCtx.get(), root);
  NG_RETURN_IF_ERROR(ret);
  auto rootGroup = std::move(ret).value();
  rootGroup->setRootGroup();

  NG_RETURN_IF_ERROR(doExploration(optCtx.get(), rootGroup));
  auto *newRoot = rootGroup->getPlan();

  NG_RETURN_IF_ERROR(postprocess(const_cast<PlanNode *>(newRoot), qctx, spaceID));
  return newRoot;
}

// Just for Properties Pruning
Status Optimizer::postprocess(PlanNode *root, graph::QueryContext *qctx, GraphSpaceID spaceID) {
  std::unordered_set<const PlanNode *> visitedPlanNode;
  NG_RETURN_IF_ERROR(rewriteArgumentInputVar(root, visitedPlanNode));
  if (FLAGS_enable_optimizer_property_pruner_rule) {
    graph::PropertyTracker propsUsed;
    graph::PrunePropertiesVisitor visitor(propsUsed, qctx, spaceID);
    root->accept(&visitor);
    if (!visitor.ok()) {
      LOG(INFO) << "Failed to prune properties of query plan in post process of optimizer: "
                << visitor.status();
    }
  }
  return Status::OK();
}

StatusOr<OptGroup *> Optimizer::prepare(OptContext *ctx, PlanNode *root) {
  std::unordered_map<int64_t, OptGroup *> visited;
  return convertToGroup(ctx, root, &visited);
}

Status Optimizer::doExploration(OptContext *octx, OptGroup *rootGroup) {
  // Terminate when the maximum number of iterations(RuleSets) is reached or the execution plan is
  // unchanged
  int8_t appliedTimes = kMaxIterationRound;
  while (octx->changed()) {
    if (--appliedTimes < 0) break;
    octx->setChanged(false);
    for (auto ruleSet : ruleSets_) {
      for (auto rule : ruleSet->rules()) {
        // Explore until the maximum number of iterations(Rules) is reached
        NG_RETURN_IF_ERROR(rootGroup->exploreUntilMaxRound(rule));
        octx->visited_.clear();
        NG_RETURN_IF_ERROR(rootGroup->validate(rule));
        octx->visited_.clear();
        rootGroup->setUnexplored(rule);
        octx->visited_.clear();
      }
    }
  }
  return Status::OK();
}

// Create Memo structure
OptGroup *Optimizer::convertToGroup(OptContext *ctx,
                                    PlanNode *node,
                                    std::unordered_map<int64_t, OptGroup *> *visited) {
  auto iter = visited->find(node->id());
  if (iter != visited->cend()) {
    return iter->second;
  }

  auto group = OptGroup::create(ctx);
  auto groupNode = group->makeGroupNode(node);

  if (node->kind() == PlanNode::Kind::kSelect) {
    auto select = static_cast<Select *>(node);
    addBodyToGroupNode(ctx, select->then(), groupNode, visited);
    addBodyToGroupNode(ctx, select->otherwise(), groupNode, visited);
  } else if (node->kind() == PlanNode::Kind::kLoop) {
    auto loop = static_cast<Loop *>(node);
    addBodyToGroupNode(ctx, loop->body(), groupNode, visited);
  }

  for (size_t i = 0; i < node->numDeps(); ++i) {
    auto dep = const_cast<PlanNode *>(node->dep(i));
    groupNode->dependsOn(convertToGroup(ctx, dep, visited));
  }

  visited->emplace(node->id(), group);
  return group;
}

void Optimizer::addBodyToGroupNode(OptContext *ctx,
                                   const PlanNode *node,
                                   OptGroupNode *gnode,
                                   std::unordered_map<int64_t, OptGroup *> *visited) {
  auto n = const_cast<PlanNode *>(node);
  auto body = convertToGroup(ctx, n, visited);
  gnode->addBody(body);
}

namespace {

// The plan node referenced by argument always is in the left side of plan tree. So we only need to
// check whether the left root child of binary input plan node contains what the argument needs in
// its output columns
bool findArgumentRefPlanNodeInPath(const std::vector<const PlanNode *> &path, PlanNode *argument) {
  DCHECK_EQ(argument->kind(), PlanNode::Kind::kArgument);
  for (int i = path.size() - 1; i >= 0; i--) {
    const auto *pn = path[i];
    if (pn->isBiInput()) {
      DCHECK_LT(i, path.size() - 1);
      const auto *bpn = static_cast<const BinaryInputNode *>(pn);
      if (bpn->right() == path[i + 1]) {
        // Argument is in the right side dependency of binary plan node, check the left child
        // output columns
        if (argument->isColumnsIncludedIn(bpn->left())) {
          if (argument->inputVar().empty()) {
            argument->setInputVar(bpn->left()->outputVar());
            return true;
          } else {
            return argument->inputVar() == bpn->left()->outputVar();
          }
        }
      } else {
        // Argument is in the left side dependency of binary plan node, continue to find
        // next parent plan node
        DCHECK_EQ(bpn->left(), path[i + 1]);
      }
    }
  }
  return false;
}

}  // namespace

// static
Status Optimizer::rewriteArgumentInputVarInternal(
    PlanNode *root,
    std::vector<const PlanNode *> &path,
    std::unordered_set<const PlanNode *> &visitedPlanNode) {
  if (!root) return Status::OK();
  if (!visitedPlanNode.emplace(root).second) {
    return Status::OK();
  }

  path.push_back(root);
  switch (root->numDeps()) {
    case 0: {
      if (root->kind() == PlanNode::Kind::kArgument) {
        if (!findArgumentRefPlanNodeInPath(path, root) || root->inputVar().empty()) {
          DCHECK(!root->outputVarPtr()->colNames.empty());
          auto outColumn = root->outputVarPtr()->colNames.back();
          return Status::Error(
              "Could not generate valid query plan since the argument plan node could not find its "
              "input data, please review your query and pay attention to the symbol `%s` usage "
              "especially.",
              outColumn.c_str());
        }
      }
      break;
    }
    case 1: {
      auto *dep = const_cast<PlanNode *>(root->dep());
      NG_RETURN_IF_ERROR(rewriteArgumentInputVarInternal(dep, path, visitedPlanNode));
      break;
    }
    case 2: {
      auto *bpn = static_cast<BinaryInputNode *>(root);
      auto *left = const_cast<PlanNode *>(bpn->left());
      NG_RETURN_IF_ERROR(rewriteArgumentInputVarInternal(left, path, visitedPlanNode));
      auto *right = const_cast<PlanNode *>(bpn->right());
      NG_RETURN_IF_ERROR(rewriteArgumentInputVarInternal(right, path, visitedPlanNode));
      break;
    }
    default: {
      return Status::Error(
          "Invalid dependencies of plan node `%s': %lu", root->toString().c_str(), root->numDeps());
    }
  }
  path.pop_back();

  if (root->kind() == PlanNode::Kind::kLoop) {
    auto loop = static_cast<Loop *>(root);
    NG_RETURN_IF_ERROR(
        rewriteArgumentInputVar(const_cast<PlanNode *>(loop->body()), visitedPlanNode));
  }

  if (root->kind() == PlanNode::Kind::kSelect) {
    auto sel = static_cast<Select *>(root);
    NG_RETURN_IF_ERROR(
        rewriteArgumentInputVar(const_cast<PlanNode *>(sel->then()), visitedPlanNode));
    NG_RETURN_IF_ERROR(
        rewriteArgumentInputVar(const_cast<PlanNode *>(sel->otherwise()), visitedPlanNode));
  }

  return Status::OK();
}

// static
Status Optimizer::rewriteArgumentInputVar(PlanNode *root,
                                          std::unordered_set<const PlanNode *> &visitedPlanNode) {
  std::vector<const PlanNode *> path;
  return rewriteArgumentInputVarInternal(root, path, visitedPlanNode);
}

Status Optimizer::checkPlanDepth(const PlanNode *root) const {
  std::queue<const PlanNode *> queue;
  std::unordered_set<const PlanNode *> visited;
  queue.push(root);
  visited.emplace(root);
  size_t depth = 0;
  while (!queue.empty()) {
    size_t size = queue.size();
    for (size_t i = 0; i < size; ++i) {
      const PlanNode *node = queue.front();
      queue.pop();
      for (size_t j = 0; j < node->numDeps(); j++) {
        const auto *dep = node->dep(j);
        if (visited.emplace(dep).second) {
          queue.push(dep);
        }
      }
    }

    ++depth;
    if (depth > FLAGS_max_plan_depth) {
      return Status::Error("The depth of plan tree has exceeded the max %lu level",
                           FLAGS_max_plan_depth);
    }
  }

  return Status::OK();
}

}  // namespace opt
}  // namespace nebula
