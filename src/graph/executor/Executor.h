// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#ifndef GRAPH_EXECUTOR_EXECUTOR_H_
#define GRAPH_EXECUTOR_EXECUTOR_H_
#include <folly/futures/Future.h>

#include <boost/core/noncopyable.hpp>
#include <mutex>

#include "common/cpp/helpers.h"
#include "common/memory/MemoryTracker.h"
#include "common/time/Duration.h"
#include "common/time/ScopedTimer.h"
#include "graph/context/ExecutionContext.h"
#include "graph/context/QueryContext.h"
namespace nebula {
namespace graph {
class PlanNode;
class QueryContext;
class Executor : private boost::noncopyable, private cpp::NonMovable {
 public:
  // Create executor according to plan node
  static Executor *create(const PlanNode *node, QueryContext *qctx);

  virtual ~Executor();

  // Prepare or initialize executor before each execution
  virtual Status open();

  // Each executor inherited from this class should get input values from
  // ExecutionContext, evaluate expressions and save output result back to
  // ExecutionContext by `finish'
  virtual folly::Future<Status> execute() = 0;

  // Cleanup or reset executor some states after each execution
  virtual Status close();

  Status checkMemoryWatermark();

  QueryContext *qctx() const {
    return qctx_;
  }

  int64_t id() const {
    return id_;
  }

  const std::string &name() const {
    return name_;
  }

  const PlanNode *node() const {
    return node_;
  }

  const std::set<Executor *> &depends() const {
    return depends_;
  }

  const std::set<Executor *> &successors() const {
    return successors_;
  }

  Executor *dependsOn(Executor *dep) {
    depends_.emplace(dep);
    dep->successors_.emplace(this);
    return this;
  }

  template <typename T>
  static std::enable_if_t<std::is_base_of<PlanNode, T>::value, const T *> asNode(
      const PlanNode *node) {
    return static_cast<const T *>(node);
  }

  // Throw runtime error to stop whole execution early
  folly::Future<Status> error(Status status) const;

  static Status memoryExceededStatus() {
    return Status::GraphMemoryExceeded(
        "(%d)", static_cast<int32_t>(nebula::cpp2::ErrorCode::E_GRAPH_MEMORY_EXCEEDED));
  }

 protected:
  static Executor *makeExecutor(const PlanNode *node,
                                QueryContext *qctx,
                                std::unordered_map<int64_t, Executor *> *visited);

  static Executor *makeExecutor(QueryContext *qctx, const PlanNode *node);

  // Only allow derived executor to construct
  Executor(const std::string &name, const PlanNode *node, QueryContext *qctx);

  // Start a future chain and bind it to thread pool
  folly::Future<Status> start(Status status = Status::OK()) const;

  folly::Executor *runner() const;

  void drop();
  void drop(const PlanNode *node);
  void dropBody(const PlanNode *body);

  // Check whether the variable is movable, it's movable when reach end of lifetime
  // This method shouldn't call after `finish` method!
  bool movable(const Variable *var);
  bool movable(const std::string &var) {
    return movable(qctx_->symTable()->getVar(var));
  }

  // Store the result of this executor to execution context
  Status finish(Result &&result);
  // Store the default result which not used for later executor
  Status finish(Value &&value);

  size_t getBatchSize(size_t totalSize) const;

  // ScatterFunc: A callback function that handle partial records of a dataset.
  // GatherFunc: A callback function that gather all results of ScatterFunc, and do post works.
  // Iterator: An iterator of a dataset.
  template <
      class ScatterFunc,
      class ScatterResult = typename std::result_of<ScatterFunc(size_t, size_t, Iterator *)>::type,
      class GatherFunc>
  auto runMultiJobs(ScatterFunc &&scatter, GatherFunc &&gather, Iterator *iter);

  void addState(const std::string &name, const time::Duration &dur) {
    auto str = folly::sformat("{}(us)", dur.elapsedInUSec());
    std::lock_guard<std::mutex> l(statsLock_);
    otherStats_.emplace(name, std::move(str));
  }

  void addState(const std::string &name, const folly::dynamic &json) {
    auto str = folly::toPrettyJson(json);
    std::lock_guard<std::mutex> l(statsLock_);
    otherStats_.emplace(name, std::move(str));
  }

  void mergeStats(std::unordered_map<std::string, std::string> stats) {
    std::lock_guard<std::mutex> l(statsLock_);
    otherStats_.merge(std::move(stats));
  }

  int64_t id_;

  // Executor name
  std::string name_;

  // Relative Plan Node
  const PlanNode *node_;

  QueryContext *qctx_;
  // Execution context for saving some execution data
  ExecutionContext *ectx_;

  // Topology
  std::set<Executor *> depends_;
  std::set<Executor *> successors_;

  // profiling data
  uint64_t numRows_{0};
  uint64_t execTime_{0};
  time::Duration totalDuration_;

 private:
  std::mutex statsLock_;
  std::unordered_map<std::string, std::string> otherStats_;
};

template <class ScatterFunc, class ScatterResult, class GatherFunc>
auto Executor::runMultiJobs(ScatterFunc &&scatter, GatherFunc &&gather, Iterator *iter) {
  size_t totalSize = iter->size();
  size_t batchSize = getBatchSize(totalSize);

  // Start multiple jobs for handling the results
  std::vector<folly::Future<ScatterResult>> futures;
  size_t begin = 0, end = 0, dispathedCnt = 0;
  while (dispathedCnt < totalSize) {
    end = begin + batchSize > totalSize ? totalSize : begin + batchSize;
    futures.emplace_back(folly::via(
        runner(),
        [begin, end, tmpIter = iter->copy(), f = std::move(scatter)]() mutable -> ScatterResult {
          // MemoryTrackerVerified
          memory::MemoryCheckGuard guard;
          // Since not all iterators are linear, so iterates to the begin pos
          size_t tmp = 0;
          for (; tmpIter->valid() && tmp < begin; ++tmp) {
            tmpIter->next();
          }

          return f(begin, end, tmpIter.get());
        }));
    begin = end;
    dispathedCnt += batchSize;
  }

  // Gather all results and do post works
  return folly::collect(futures).via(runner()).thenValue(std::move(gather));
}
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_EXECUTOR_H_
