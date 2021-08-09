/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "optimizer/OptContext.h"

#include "common/base/Logging.h"
#include "common/base/ObjectPool.h"
#include "optimizer/OptGroup.h"
#include "planner/Planner.h"

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

}   // namespace opt
}   // namespace nebula
