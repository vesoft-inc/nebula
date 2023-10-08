/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_OPTCONTEXT_H_
#define GRAPH_OPTIMIZER_OPTCONTEXT_H_

#include <boost/core/noncopyable.hpp>
#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "common/cpp/helpers.h"

namespace nebula {

class ObjectPool;

namespace graph {
class QueryContext;
class PlanNode;
}  // namespace graph

namespace opt {

class OptGroupNode;
class OptGroup;
class Optimizer;

class OptContext final : private boost::noncopyable, private cpp::NonMovable {
 public:
  explicit OptContext(graph::QueryContext *qctx);

  graph::QueryContext *qctx() const {
    return qctx_;
  }

  ObjectPool *objPool() const {
    return objPool_.get();
  }

  bool changed() const {
    return changed_;
  }

  void setChanged(bool changed) {
    changed_ = changed;
  }

  void addPlanNodeAndOptGroupNode(int64_t planNodeId, const OptGroupNode *optGroupNode);
  const OptGroupNode *findOptGroupNodeByPlanNodeId(int64_t planNodeId) const;

 private:
  friend OptGroup;
  friend Optimizer;
  // A global flag to record whether this iteration caused a change to the plan
  bool changed_{true};
  graph::QueryContext *qctx_{nullptr};
  // Memo memory management in the Optimizer phase
  std::unique_ptr<ObjectPool> objPool_;
  std::unordered_map<int64_t, const OptGroupNode *> planNodeToOptGroupNodeMap_;
  std::unordered_set<const OptGroup *> visited_;
  std::unordered_map<const OptGroup *, const graph::PlanNode *> group2PlanNodeMap_;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_OPTCONTEXT_H_
