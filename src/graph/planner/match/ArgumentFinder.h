/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_ARGUMENTFINDER_H
#define GRAPH_PLANNER_MATCH_ARGUMENTFINDER_H

#include <memory>  // for unique_ptr

#include "common/base/StatusOr.h"                // for StatusOr
#include "graph/planner/match/StartVidFinder.h"  // for StartVidFinder

namespace nebula {
namespace graph {
struct EdgeContext;
struct NodeContext;
struct SubPlan;

struct EdgeContext;
struct NodeContext;
struct SubPlan;

class ArgumentFinder final : public StartVidFinder {
 public:
  static std::unique_ptr<ArgumentFinder> make() {
    return std::unique_ptr<ArgumentFinder>(new ArgumentFinder());
  }

  bool matchNode(NodeContext* nodeCtx) override;

  bool matchEdge(EdgeContext* edgeCtx) override;

  StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) override;

  StatusOr<SubPlan> transformEdge(EdgeContext* edgeCtx) override;

 private:
  ArgumentFinder() = default;
};

}  // namespace graph
}  // namespace nebula
#endif
