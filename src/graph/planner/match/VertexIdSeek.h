/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_VERTEXIDSEEK_H_
#define GRAPH_PLANNER_MATCH_VERTEXIDSEEK_H_

#include <memory>  // for unique_ptr
#include <string>  // for string

#include "common/base/StatusOr.h"  // for StatusOr
#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/match/StartVidFinder.h"  // for StartVidFinder

namespace nebula {
class Expression;
namespace graph {
class QueryContext;
struct EdgeContext;
struct NodeContext;
struct SubPlan;
}  // namespace graph
struct List;

class Expression;
struct List;

namespace graph {
class QueryContext;
struct EdgeContext;
struct NodeContext;
struct SubPlan;

/*
 * The VertexIdSeek was designed to find if could get the starting vids in
 * filter.
 */
class VertexIdSeek final : public StartVidFinder {
 public:
  static std::unique_ptr<VertexIdSeek> make() {
    return std::unique_ptr<VertexIdSeek>(new VertexIdSeek());
  }

  bool matchNode(NodeContext* nodeCtx) override;

  bool matchEdge(EdgeContext* edgeCtx) override;

  StatusOr<const Expression*> extractVids(const std::string& alias, const Expression* filter);

  StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) override;

  StatusOr<SubPlan> transformEdge(EdgeContext* edgeCtx) override;

  std::string listToAnnoVarVid(QueryContext* qctx, const List& list);

 private:
  VertexIdSeek() = default;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_VERTEXIDSEEK_H_
