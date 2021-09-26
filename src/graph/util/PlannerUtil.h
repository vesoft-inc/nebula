/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

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

  static void buildConstantInput(QueryContext* qctx, Starts& starts, std::string& vidsVar);

  static SubPlan buildRuntimeInput(QueryContext* qctx, Starts& starts);

  static SubPlan buildStart(QueryContext* qctx, Starts& starts, std::string& vidsVar);

  static PlanNode* extractDstFromGN(QueryContext* qctx, PlanNode* gn, const std::string& output);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_UTIL_PLANNER_UTIL_H_
