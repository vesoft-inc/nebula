/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/scheduler/PushBasedScheduler.h"

#include "graph/context/QueryContext.h"
#include "graph/executor/Executor.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {

PushBasedScheduler::PushBasedScheduler(QueryContext* qctx) : Scheduler(qctx) {}

folly::Future<Status> PushBasedScheduler::schedule() {
  buildDepMap(makeExecutor());
  return runExecutor();
}

void PushBasedScheduler::buildDepMap(Executor* exec) {
  auto node = exec->node();
  if (node->kind() == PlanNode::Kind::kArgument) {
    argumentMap_[node->inputVar()].emplace_back(exec);
  } else if (exec->depends().empty()) {
    leafExecutors_.emplace(exec);
  } else {
    for (auto* dep : exec->depends()) {
      buildDepMap(dep);
    }
  }
}

folly::Future<Status> PushBasedScheduler::runExecutor() {
  std::vector<folly::Future<Status>> futures;
  futures.reserve(leafExecutors_.size());
  for (auto* exec : leafExecutors_) {
    DCHECK_NE(exec->node()->kind(), PlanNode::Kind::kArgument);
    futures.emplace_back(runExecutor(exec));
  }
  auto* runner = qctx_->rctx()->runner();
  return folly::collect(futures).via(runner).thenValue([](std::vector<Status>&& stats) {
    for (auto& s : stats) {
      NG_RETURN_IF_ERROR(s);
    }
    return Status::OK();
  });
}

folly::Future<Status> PushBasedScheduler::runExecutor(Executor* exec) {
  auto* runner = qctx_->rctx()->runner();
  return folly::via(runner, [exec]() { return DCHECK_NOTNULL(exec)->execute(); })
      .thenValue([this, exec, runner](Status s) -> folly::Future<Status> {
        NG_RETURN_IF_ERROR(s);

        std::vector<folly::Future<Status>> futures;

        auto varname = exec->node()->outputVar();
        auto iter = argumentMap_.find(varname);
        if (iter != argumentMap_.end()) {
          for (auto* arg : iter->second) {
            futures.emplace_back(runExecutor(arg));
          }
        }

        for (auto* succ : exec->successors()) {
          if (succ->finishDep() == succ->depends().size()) {
            futures.emplace_back(runExecutor(succ));
          }
        }

        if (futures.empty()) {
          return Status::OK();
        }
        return folly::collect(futures).via(runner).thenValue([](std::vector<Status>&& stats) {
          for (auto& st : stats) {
            NG_RETURN_IF_ERROR(st);
          }
          return Status::OK();
        });
      });
}

}  // namespace graph
}  // namespace nebula
