/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_STARTVIDFINDER_H_
#define GRAPH_PLANNER_MATCH_STARTVIDFINDER_H_

#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/Planner.h"

namespace nebula {
namespace graph {
class StartVidFinder;

using StartVidFinderInstantiateFunc = std::function<std::unique_ptr<StartVidFinder>()>;

// A StartVidFinder finds a generally good solution for traversing from the vids.
// Currently we have five StartVidFinders:
// 1. VertexIdSeek find if a plan could traverse from a given vid.
// MATCH(n) WHERE id(n) = value RETURN n
//
// 2. ArgumentFinder finds if a plan could traverse from some vids that already
// beed traversed.
// MATCH (n)-[]-(l), (l)-[]-(m) return n,l,m
// MATCH (n)-[]-(l) MATCH (l)-[]-(m) return n,l,m
//
// 3. PropIndexSeek finds if a plan could traverse from some vids that could be
// read from the property indices.
// MATCH(n:Tag{prop:value}) RETURN n
// MATCH(n:Tag) WHERE n.Tag.prop = value RETURN n
//
// 4. VariableVertexIdSeek
// WITH "xx" AS vid MATCH (v:Tag) WHERE id(v)==vid RETURN v
//
// 5. VariablePropIndexSeek
// WITH "xx" AS prop MATCH (v:Tag) WHERE v.Tag.prop==prop RETURN v
//
// 6. LabelIndexSeek finds if a plan could traverse from some vids that could be
// read from the label indices.
// MATCH(n: tag) RETURN n
// MATCH(s)-[:edge]->(e) RETURN e
//
// 7. ScanSeek finds if a plan could traverse from some vids by scanning.
//
class StartVidFinder {
 public:
  virtual ~StartVidFinder() = default;

  static auto& finders() {
    static std::vector<StartVidFinderInstantiateFunc> finders;
    return finders;
  }

  bool match(PatternContext* patternCtx);

  // The derived class should implement matchNode if the finder has
  // the ability to find vids from node pattern.
  virtual bool matchNode(NodeContext* /* nodeCtx */) {
    return false;
  }

  // The derived class should implement matchEdge if the finder has
  // the ability to find vids from edge pattern.
  virtual bool matchEdge(EdgeContext* /* edgeCtx */) {
    return false;
  }

  StatusOr<SubPlan> transform(PatternContext* patternCtx);

  virtual StatusOr<SubPlan> transformNode(NodeContext* /* nodeCtx */) {
    return Status::Error("Unimplemented");
  }

  virtual StatusOr<SubPlan> transformEdge(EdgeContext* /* edgeCtx */) {
    return Status::Error("Unimplemented");
  }

  virtual const char* name() const = 0;

 protected:
  StartVidFinder() = default;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_STARTVIDFINDER_H_
