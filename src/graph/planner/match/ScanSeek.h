/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_SCANSEEK_H
#define GRAPH_PLANNER_MATCH_SCANSEEK_H

#include <memory>  // for unique_ptr

#include "common/base/StatusOr.h"  // for StatusOr
#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/match/StartVidFinder.h"  // for StartVidFinder

namespace nebula {
namespace graph {
struct EdgeContext;
struct NodeContext;
struct SubPlan;

struct EdgeContext;
struct NodeContext;
struct SubPlan;

/*
 * The ScanSeek was designed to find if could get the starting vids in
 * filter.
 */
class ScanSeek final : public StartVidFinder {
 public:
  static std::unique_ptr<ScanSeek> make() {
    return std::unique_ptr<ScanSeek>(new ScanSeek());
  }

  bool matchNode(NodeContext* nodeCtx) override;

  bool matchEdge(EdgeContext* edgeCtx) override;

  StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) override;

  StatusOr<SubPlan> transformEdge(EdgeContext* edgeCtx) override;

 private:
  ScanSeek() = default;
};
}  // namespace graph
}  // namespace nebula
#endif
