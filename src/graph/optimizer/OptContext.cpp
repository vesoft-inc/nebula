/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/OptContext.h"

#include <folly/Likely.h>  // for UNLIKELY

#include <cstdint>  // for int64_t
#include <ostream>  // for operator<<, basic_ostream
#include <string>   // for operator<<
#include <utility>  // for pair

#include "common/base/Logging.h"          // for CheckNotNull, DCHECK_NOTNULL
#include "common/base/ObjectPool.h"       // for ObjectPool
#include "graph/optimizer/OptGroup.h"     // for OptGroupNode
#include "graph/planner/plan/PlanNode.h"  // for PlanNode

namespace nebula {
namespace opt {

OptContext::OptContext(graph::QueryContext *qctx)
    : qctx_(DCHECK_NOTNULL(qctx)), objPool_(std::make_unique<ObjectPool>()) {}

void OptContext::addPlanNodeAndOptGroupNode(int64_t planNodeId, const OptGroupNode *optGroupNode) {
  auto pair = planNodeToOptGroupNodeMap_.emplace(planNodeId, optGroupNode);
  if (UNLIKELY(!pair.second)) {
    const auto &pn = pair.first->second->node()->toString();
    LOG(ERROR) << "PlanNode(" << planNodeId << ") has existed in OptContext: " << pn;
  }
}

const OptGroupNode *OptContext::findOptGroupNodeByPlanNodeId(int64_t planNodeId) const {
  auto found = planNodeToOptGroupNodeMap_.find(planNodeId);
  return found == planNodeToOptGroupNodeMap_.end() ? nullptr : found->second;
}

}  // namespace opt
}  // namespace nebula
