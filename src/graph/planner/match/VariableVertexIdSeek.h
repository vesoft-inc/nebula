/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_VARIABLEVERTEXIDSEEK_H_
#define GRAPH_PLANNER_MATCH_VARIABLEVERTEXIDSEEK_H_

#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/match/StartVidFinder.h"

namespace nebula {
namespace graph {

// The VariableVertexIdSeek find if a plan could get the starting vids in filters.
class VariableVertexIdSeek final : public StartVidFinder {
 public:
  static std::unique_ptr<VariableVertexIdSeek> make() {
    return std::unique_ptr<VariableVertexIdSeek>(new VariableVertexIdSeek());
  }

  bool matchNode(NodeContext* nodeCtx) override;

  StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) override;

  const char* name() const override {
    return "VariableIdVertexSeekFinder";
  }

 private:
  VariableVertexIdSeek() = default;

  bool extractVidPredicate(const std::string& nodeAlias, Expression* filter, std::string* var);
  static bool isVidPredicate(const std::string& nodeAlias,
                             const Expression* filter,
                             std::string* var);
  static bool isIdFunCallExpr(const std::string& nodeAlias, const Expression* filter);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PLANNER_MATCH_VARIABLEVERTEXIDSEEK_H_
