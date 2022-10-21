/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_VERTEXIDSEEK_H_
#define GRAPH_PLANNER_MATCH_VERTEXIDSEEK_H_

#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/match/StartVidFinder.h"

namespace nebula {
namespace graph {
// The VertexIdSeek find if a plan could get the starting vids in filters.
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

  const char* name() const override {
    return "VertexIdSeekFinder";
  }

 private:
  VertexIdSeek() = default;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_VERTEXIDSEEK_H_
