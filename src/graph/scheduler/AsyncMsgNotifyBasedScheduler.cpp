/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/scheduler/AsyncMsgNotifyBasedScheduler.h"

#include "graph/executor/Executor.h"
#include "graph/planner/plan/PlanNode.h"

namespace nebula {
namespace graph {

AsyncMsgNotifyBasedScheduler::AsyncMsgNotifyBasedScheduler(QueryContext* qctx) : Scheduler(qctx) {}

void AsyncMsgNotifyBasedScheduler::waitFinish() {
  std::unique_lock<std::mutex> lck(emtx_);
  cv_.wait(lck, [this] {
    if (executing_ != 0) {
      DLOG(INFO) << "executing: " << executing_;
      return false;
    } else {
      DLOG(INFO) << " wait finish";
      return true;
    }
  });
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::schedule() {
  return doSchedule(makeExecutor());
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::doSchedule(Executor* root) const {
  std::unordered_map<int64_t, std::vector<folly::Promise<Status>>> promiseMap;
  std::unordered_map<int64_t, std::vector<folly::Future<Status>>> futureMap;
  std::queue<Executor*> queue;
  std::queue<Executor*> queue2;
  std::unordered_set<Executor*> visited;

  auto* runner = qctx_->rctx()->runner();
  folly::Promise<Status> promiseForRoot;
  auto resultFuture = promiseForRoot.getFuture();
  promiseMap[root->id()].emplace_back(std::move(promiseForRoot));
  queue.push(root);
  visited.emplace(root);
  while (!queue.empty()) {
    auto* exe = queue.front();
    queue.pop();
    queue2.push(exe);

    std::vector<folly::Future<Status>>& futures = futureMap[exe->id()];
    if (exe->node()->kind() == PlanNode::Kind::kArgument) {
      auto nodeInputVar = exe->node()->inputVar();
      const auto& writtenBy = qctx_->symTable()->getVar(nodeInputVar)->writtenBy;
      for (auto& node : writtenBy) {
        folly::Promise<Status> p;
        futures.emplace_back(p.getFuture());
        auto& promises = promiseMap[node->id()];
        promises.emplace_back(std::move(p));
      }
    } else {
      for (auto* dep : exe->depends()) {
        auto notVisited = visited.emplace(dep).second;
        if (notVisited) {
          queue.push(dep);
        }
        folly::Promise<Status> p;
        futures.emplace_back(p.getFuture());
        auto& promises = promiseMap[dep->id()];
        promises.emplace_back(std::move(p));
      }
    }
  }

  while (!queue2.empty()) {
    auto* exe = queue2.front();
    queue2.pop();

    auto currentFuturesFound = futureMap.find(exe->id());
    DCHECK(currentFuturesFound != futureMap.end());
    auto currentExeFutures = std::move(currentFuturesFound->second);

    auto currentPromisesFound = promiseMap.find(exe->id());
    DCHECK(currentPromisesFound != promiseMap.end());
    auto currentExePromises = std::move(currentPromisesFound->second);

    scheduleExecutor(std::move(currentExeFutures), exe, runner)
        // This is the root catch of bad_alloc for Executors,
        // all chained returned future is checked here
        .thenError(
            folly::tag_t<std::bad_alloc>{},
            [](const std::bad_alloc&) {
              return folly::makeFuture<Status>(Status::GraphMemoryExceeded(
                  "(%d)", static_cast<int32_t>(nebula::cpp2::ErrorCode::E_GRAPH_MEMORY_EXCEEDED)));
            })
        .thenTry([this, exe, pros = std::move(currentExePromises)](auto&& t) mutable {
          // any exception or status not ok handled with notifyError
          if (t.hasException()) {
            notifyError(pros, Status::Error(std::move(t).exception().what()));
          } else {
            auto v = std::move(t).value();
            if (v.ok()) {
              notifyOK(pros);
            } else {
              DLOG(INFO) << "[" << exe->name() << "," << exe->id() << "]"
                         << " fail with: " << v.toString();
              notifyError(pros, v);
            }
          }
        });
  }

  return resultFuture;
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::scheduleExecutor(
    std::vector<folly::Future<Status>>&& futures, Executor* exe, folly::Executor* runner) const {
  switch (exe->node()->kind()) {
    case PlanNode::Kind::kSelect: {
      auto select = static_cast<SelectExecutor*>(exe);
      return runSelect(std::move(futures), select, runner);
    }
    case PlanNode::Kind::kLoop: {
      auto loop = static_cast<LoopExecutor*>(exe);
      return runLoop(std::move(futures), loop, runner);
    }
    case PlanNode::Kind::kArgument: {
      return runExecutor(std::move(futures), exe, runner);
    }
    default: {
      if (exe->depends().empty()) {
        return runLeafExecutor(exe, runner);
      } else {
        return runExecutor(std::move(futures), exe, runner);
      }
    }
  }
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::runSelect(
    std::vector<folly::Future<Status>>&& futures,
    SelectExecutor* select,
    folly::Executor* runner) const {
  return folly::collect(futures)
      .via(runner)
      .thenValue([select, this](auto&& t) mutable -> folly::Future<Status> {
        NG_RETURN_IF_ERROR(checkStatus(std::move(t)));
        return execute(select);
      })
      .thenValue([select, this](auto&& selectStatus) mutable -> folly::Future<Status> {
        NG_RETURN_IF_ERROR(selectStatus);
        auto val = qctx_->ectx()->getValue(select->node()->outputVar());
        if (!val.isBool()) {
          std::stringstream ss;
          ss << "Loop produces a bad condition result: " << val << " type: " << val.type();
          return Status::Error(ss.str());
        }

        if (val.getBool()) {
          return doSchedule(select->thenBody());
        }
        return doSchedule(select->elseBody());
      });
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::runExecutor(
    std::vector<folly::Future<Status>>&& futures, Executor* exe, folly::Executor* runner) const {
  return folly::collect(futures).via(runner).thenValue(
      [exe, this](auto&& t) mutable -> folly::Future<Status> {
        NG_RETURN_IF_ERROR(checkStatus(std::move(t)));
        // Execute in current thread.
        return execute(exe);
      });
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::runLeafExecutor(Executor* exe,
                                                                    folly::Executor* runner) const {
  return std::move(execute(exe)).via(runner);
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::runLoop(
    std::vector<folly::Future<Status>>&& futures,
    LoopExecutor* loop,
    folly::Executor* runner) const {
  return folly::collect(futures)
      .via(runner)
      .thenValue([loop, this](auto&& t) mutable -> folly::Future<Status> {
        NG_RETURN_IF_ERROR(checkStatus(std::move(t)));
        return execute(loop);
      })
      .thenValue([loop, runner, this](auto&& loopStatus) mutable -> folly::Future<Status> {
        NG_RETURN_IF_ERROR(loopStatus);
        auto val = qctx_->ectx()->getValue(loop->node()->outputVar());
        if (!val.isBool()) {
          std::stringstream ss;
          ss << "Loop produces a bad condition result: " << val << " type: " << val.type();
          return Status::Error(ss.str());
        }
        if (!val.getBool()) {
          return Status::OK();
        }
        std::vector<folly::Future<Status>> fs;
        fs.emplace_back(doSchedule(loop->loopBody()));
        return runLoop(std::move(fs), loop, runner);
      });
}

Status AsyncMsgNotifyBasedScheduler::checkStatus(std::vector<Status>&& status) const {
  for (auto& s : status) {
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

void AsyncMsgNotifyBasedScheduler::notifyOK(std::vector<folly::Promise<Status>>& promises) const {
  for (auto& p : promises) {
    p.setValue(Status::OK());
  }
}

void AsyncMsgNotifyBasedScheduler::notifyError(std::vector<folly::Promise<Status>>& promises,
                                               Status status) const {
  for (auto& p : promises) {
    p.setValue(status);
  }
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::execute(Executor* executor) const {
  auto status = executor->open();
  if (!status.ok()) {
    return executor->error(std::move(status));
  }

  auto exeStatus = runExecute(executor);

  return std::move(exeStatus).thenValue([this, executor](Status s) {
    if (!s.ok()) {
      DLOG(INFO) << formatPrettyId(executor) << " failed with: " << s.toString();
      setFailStatus(s);
      removeExecuting(executor);
      return Status::from(s);
    }
    auto ret = executor->close();
    removeExecuting(executor);
    return ret;
  });
}

folly::Future<Status> AsyncMsgNotifyBasedScheduler::runExecute(Executor* executor) const {
  // catch Executor::execute here, upward call stack should only get Status, no exceptions.
  try {
    addExecuting(executor);
    if (hasFailStatus()) return failedStatus_.value();
    folly::Future<Status> status = Status::OK();
    {
      memory::MemoryCheckGuard guard;
      status = executor->execute();
    }
    return std::move(status).thenError(folly::tag_t<std::bad_alloc>{}, [](const std::bad_alloc&) {
      return folly::makeFuture<Status>(Status::GraphMemoryExceeded(
          "(%d)", static_cast<int32_t>(nebula::cpp2::ErrorCode::E_GRAPH_MEMORY_EXCEEDED)));
    });
  } catch (std::bad_alloc& e) {
    return folly::makeFuture<Status>(Status::GraphMemoryExceeded(
        "(%d)", static_cast<int32_t>(nebula::cpp2::ErrorCode::E_GRAPH_MEMORY_EXCEEDED)));
  } catch (std::exception& e) {
    return folly::makeFuture<Status>(Status::Error("%s", e.what()));
  } catch (...) {
    return folly::makeFuture<Status>(Status::Error("unknown error"));
  }
}

void AsyncMsgNotifyBasedScheduler::addExecuting(Executor* executor) const {
  std::unique_lock<std::mutex> lck(emtx_);
  executing_++;
  DLOG(INFO) << formatPrettyId(executor) << " add " << executing_;
}

void AsyncMsgNotifyBasedScheduler::removeExecuting(Executor* executor) const {
  std::unique_lock<std::mutex> lck(emtx_);
  executing_--;
  DLOG(INFO) << formatPrettyId(executor) << "remove: " << executing_;
  cv_.notify_one();
}

void AsyncMsgNotifyBasedScheduler::setFailStatus(Status status) const {
  std::unique_lock<std::mutex> lck(smtx_);
  if (!failedStatus_.has_value()) {
    failedStatus_ = status;
  }
}

bool AsyncMsgNotifyBasedScheduler::hasFailStatus() const {
  std::unique_lock<std::mutex> lck(smtx_);
  return failedStatus_.has_value();
}

}  // namespace graph
}  // namespace nebula
