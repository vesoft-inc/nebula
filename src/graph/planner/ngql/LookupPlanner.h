/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_NGQL_LOOKUPPLANNER_H_
#define GRAPH_PLANNER_NGQL_LOOKUPPLANNER_H_

#include <memory>  // for unique_ptr

#include "common/base/StatusOr.h"   // for StatusOr
#include "graph/planner/Planner.h"  // for Planner

namespace nebula {
namespace nebula {
namespace graph {
struct SubPlan;
}  // namespace graph
}  // namespace nebula

class Expression;
class YieldColumns;

namespace graph {

struct LookupContext;
struct AstContext;
struct SubPlan;

class LookupPlanner final : public Planner {
 public:
  static std::unique_ptr<Planner> make();
  static bool match(AstContext* astCtx);

  StatusOr<SubPlan> transform(AstContext* astCtx) override;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_PLANNER_NGQL_LOOKUPPLANNER_H_
