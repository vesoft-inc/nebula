/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_OPTCONTEXT_H_
#define GRAPH_OPTIMIZER_OPTCONTEXT_H_

#include <stdint.h>  // for int64_t

#include <boost/core/noncopyable.hpp>  // for noncopyable
#include <memory>                      // for unique_ptr
#include <unordered_map>               // for unordered_map

#include "common/cpp/helpers.h"  // for NonMovable

namespace nebula {

class ObjectPool;

namespace graph {

class QueryContext;
}  // namespace graph

namespace opt {

class OptGroupNode;

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
  bool changed_{true};
  graph::QueryContext *qctx_{nullptr};
  std::unique_ptr<ObjectPool> objPool_;
  std::unordered_map<int64_t, const OptGroupNode *> planNodeToOptGroupNodeMap_;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_OPTCONTEXT_H_
