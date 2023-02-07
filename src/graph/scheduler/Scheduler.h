/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SCHEDULER_SCHEDULER_H_
#define GRAPH_SCHEDULER_SCHEDULER_H_

#include <boost/core/noncopyable.hpp>
#include <memory>

#include "common/base/Status.h"
#include "common/cpp/helpers.h"

namespace nebula {

class ObjectPool;

namespace graph {

class Executor;
class PlanNode;
class QueryContext;

class Scheduler : private boost::noncopyable, private cpp::NonMovable {
 public:
  static std::unique_ptr<Scheduler> create(QueryContext* qctx);

  virtual ~Scheduler();

  virtual folly::Future<Status> schedule() = 0;

  virtual void waitFinish() = 0;

  static void analyzeLifetime(const PlanNode* node, std::size_t loopLayers = 0);

 protected:
  explicit Scheduler(QueryContext* qctx);

  Executor* makeExecutor() const;

  static bool usePushBasedScheduler(const PlanNode* node);
  static std::string formatPrettyId(Executor* executor);
  static std::string formatPrettyDependencyTree(Executor* root);
  static void appendExecutor(size_t tabs, Executor* executor, std::stringstream& ss);

  QueryContext* qctx_{nullptr};
  // use by debugger to check query which crash in runtime
  std::string query_;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_SCHEDULER_SCHEDULER_H_
