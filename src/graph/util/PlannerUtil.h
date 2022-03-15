// Copyright (c) 2021 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_UTIL_PLANNER_UTIL_H_
#define GRAPH_UTIL_PLANNER_UTIL_H_
#include "common/base/Base.h"

namespace nebula {
namespace graph {
class QueryContext;
struct Starts;
struct SubPlan;
class PlanNode;
class PlannerUtil final {
 public:
  PlannerUtil() = delete;
  // Generates a dataset where each row is a vid from starts.vids, and saves the dataset in variable
  // vidsVar
  static void buildConstantInput(QueryContext* qctx, Starts& starts, std::string& vidsVar);

  // Builds a starts subplan when in the runtime
  static SubPlan buildRuntimeInput(QueryContext* qctx, Starts& starts);

  // Builds a starts subplan from the Starts object.
  // If starts.vids is not empty and the starts.originalSrc is null, call buildConstantInput(),
  // otherwise build a runtime input
  static SubPlan buildStart(QueryContext* qctx, Starts& starts, std::string& vidsVar);

  // Deduplicates the dst in the getNeighbor plan node and outputs the result to variable output
  static PlanNode* extractDstFromGN(QueryContext* qctx, PlanNode* gn, const std::string& output);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_UTIL_PLANNER_UTIL_H_
