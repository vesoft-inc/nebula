/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_STARTVIDFINDER_H_
#define GRAPH_PLANNER_MATCH_STARTVIDFINDER_H_

#include <functional>  // for function
#include <memory>      // for unique_ptr
#include <vector>      // for vector

#include "common/base/StatusOr.h"  // for StatusOr
#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/Planner.h"

namespace nebula {
namespace graph {
struct EdgeContext;
struct NodeContext;
struct PatternContext;
struct SubPlan;

class StartVidFinder;
struct EdgeContext;
struct NodeContext;
struct PatternContext;
struct SubPlan;

using StartVidFinderInstantiateFunc = std::function<std::unique_ptr<StartVidFinder>()>;

class StartVidFinder {
 public:
  virtual ~StartVidFinder() = default;

  static auto& finders() {
    static std::vector<StartVidFinderInstantiateFunc> finders;
    return finders;
  }

  bool match(PatternContext* patternCtx);

  virtual bool matchNode(NodeContext* nodeCtx) = 0;

  virtual bool matchEdge(EdgeContext* nodeCtx) = 0;

  StatusOr<SubPlan> transform(PatternContext* patternCtx);

  virtual StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) = 0;

  virtual StatusOr<SubPlan> transformEdge(EdgeContext* edgeCtx) = 0;

 protected:
  StartVidFinder() = default;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_STARTVIDFINDER_H_
