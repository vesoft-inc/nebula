
/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Admin.h"

#include "common/interface/gen-cpp2/graph_types.h"
#include "util/ToJson.h"

namespace nebula {
namespace graph {

std::unique_ptr<cpp2::PlanNodeDescription> CreateSpace::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("ifNotExists", util::toJson(ifNotExists_), desc.get());
    addDescription("spaceDesc", folly::toJson(util::toJson(props_)), desc.get());
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> DropSpace::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("spaceName", spaceName_, desc.get());
    addDescription("ifExists", util::toJson(ifExists_), desc.get());
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> DescSpace::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("spaceName", spaceName_, desc.get());
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> ShowCreateSpace::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("spaceName", spaceName_, desc.get());
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> DropSnapshot::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("snapshotName", snapshotName_, desc.get());
    return desc;
}

std::unique_ptr<cpp2::PlanNodeDescription> ShowParts::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("spaceId", folly::to<std::string>(spaceId_), desc.get());
    addDescription("partIds", folly::toJson(util::toJson(partIds_)), desc.get());
    return desc;
}
}   // namespace graph
}   // namespace nebula
