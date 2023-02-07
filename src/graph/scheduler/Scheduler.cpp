/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/scheduler/Scheduler.h"

#include <atomic>
#include <limits>

#include "graph/context/QueryContext.h"
#include "graph/executor/ExecutionError.h"
#include "graph/executor/Executor.h"
#include "graph/executor/logic/LoopExecutor.h"
#include "graph/executor/logic/PassThroughExecutor.h"
#include "graph/executor/logic/SelectExecutor.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/scheduler/AsyncMsgNotifyBasedScheduler.h"
#include "graph/scheduler/PushBasedScheduler.h"

DECLARE_bool(enable_lifetime_optimize);

namespace nebula {
namespace graph {

std::unique_ptr<Scheduler> Scheduler::create(QueryContext* qctx) {
  if (usePushBasedScheduler(DCHECK_NOTNULL(qctx->plan())->root())) {
    return std::make_unique<PushBasedScheduler>(qctx);
  }
  return std::make_unique<AsyncMsgNotifyBasedScheduler>(qctx);
}

Scheduler::Scheduler(QueryContext* qctx)
    : qctx_(DCHECK_NOTNULL(qctx)), query_(DCHECK_NOTNULL(qctx->rctx())->query()) {}

Scheduler::~Scheduler() {}

/*static*/
void Scheduler::analyzeLifetime(const PlanNode* root, std::size_t loopLayers) {
  std::stack<std::tuple<const PlanNode*, std::size_t>> stack;
  stack.push(std::make_tuple(root, loopLayers));
  while (!stack.empty()) {
    const auto& current = stack.top();
    const auto* currentNode = std::get<0>(current);
    const auto currentLoopLayers = std::get<1>(current);
    for (auto& inputVar : currentNode->inputVars()) {
      if (inputVar != nullptr) {
        inputVar->userCount.fetch_add(1, std::memory_order_relaxed);
      }
    }
    auto* currentMutNode = const_cast<PlanNode*>(currentNode);
    currentMutNode->setLoopLayers(currentLoopLayers);
    stack.pop();

    for (auto dep : currentNode->dependencies()) {
      stack.push(std::make_tuple(dep, currentLoopLayers));
    }
    switch (currentNode->kind()) {
      case PlanNode::Kind::kSelect: {
        auto sel = static_cast<const Select*>(currentNode);
        // used by scheduler
        sel->outputVarPtr()->userCount.store(std::numeric_limits<uint64_t>::max(),
                                             std::memory_order_relaxed);
        stack.push(std::make_tuple(sel->then(), currentLoopLayers));
        stack.push(std::make_tuple(sel->otherwise(), currentLoopLayers));
        break;
      }
      case PlanNode::Kind::kLoop: {
        auto loop = const_cast<Loop*>(static_cast<const Loop*>(currentNode));
        // used by scheduler
        loop->outputVarPtr()->userCount.store(std::numeric_limits<uint64_t>::max(),
                                              std::memory_order_relaxed);
        loop->setLoopLayers(currentLoopLayers + 1);
        stack.push(std::make_tuple(loop->body(), loop->loopLayers()));
        break;
      }
      default:
        break;
    }
  }
}

Executor* Scheduler::makeExecutor() const {
  auto root = qctx_->plan()->root();
  if (FLAGS_enable_lifetime_optimize) {
    // special for root
    root->outputVarPtr()->userCount.store(std::numeric_limits<uint64_t>::max(),
                                          std::memory_order_relaxed);
    analyzeLifetime(root);
  }
  auto executor = Executor::create(root, qctx_);
  DLOG(INFO) << formatPrettyDependencyTree(executor);
  return executor;
}

bool Scheduler::usePushBasedScheduler(const PlanNode* node) {
  switch (DCHECK_NOTNULL(node)->kind()) {
    case PlanNode::Kind::kStart:
    case PlanNode::Kind::kArgument: {
      return true;
    }
    case PlanNode::Kind::kAggregate:
    case PlanNode::Kind::kProject:
    case PlanNode::Kind::kFilter:
    case PlanNode::Kind::kSort:
    case PlanNode::Kind::kDedup:
    case PlanNode::Kind::kPassThrough:
    case PlanNode::Kind::kAppendVertices:
    case PlanNode::Kind::kTraverse:
    case PlanNode::Kind::kUnwind:
    case PlanNode::Kind::kShortestPath: {
      return usePushBasedScheduler(node->dep());
    }
    case PlanNode::Kind::kHashInnerJoin:
    case PlanNode::Kind::kHashLeftJoin:
    case PlanNode::Kind::kCrossJoin: {
      auto join = static_cast<const BinaryInputNode*>(node);
      return usePushBasedScheduler(join->left()) && usePushBasedScheduler(join->right());
    }
    default: {
      return false;
    }
  }
}

std::string Scheduler::formatPrettyId(Executor* executor) {
  return fmt::format("[{},{}]", executor->name(), executor->id());
}

std::string Scheduler::formatPrettyDependencyTree(Executor* root) {
  std::stringstream ss;
  size_t spaces = 0;
  appendExecutor(spaces, root, ss);
  return ss.str();
}

void Scheduler::appendExecutor(size_t spaces, Executor* executor, std::stringstream& ss) {
  ss << std::string(spaces, ' ') << formatPrettyId(executor) << std::endl;
  for (auto depend : executor->depends()) {
    appendExecutor(spaces + 1, depend, ss);
  }
}

}  // namespace graph
}  // namespace nebula
