/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/plan/Mutate.h"

#include "common/interface/gen-cpp2/storage_types.h"
#include "util/ToJson.h"

namespace nebula {
namespace graph {

std::unique_ptr<PlanNodeDescription> InsertVertices::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("spaceId", folly::to<std::string>(spaceId_), desc.get());
    addDescription("ifNotExists", util::toJson(ifNotExists_), desc.get());

    folly::dynamic tagPropsArr = folly::dynamic::array();
    for (const auto &p : tagPropNames_) {
        folly::dynamic obj = folly::dynamic::object();
        obj.insert("tagId", p.first);
        obj.insert("props", util::toJson(p.second));
        tagPropsArr.push_back(obj);
    }
    addDescription("tagPropNames", folly::toJson(tagPropsArr), desc.get());
    addDescription("vertices", folly::toJson(util::toJson(vertices_)), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> InsertEdges::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("spaceId", folly::to<std::string>(spaceId_), desc.get());
    addDescription("ifNotExists", util::toJson(ifNotExists_), desc.get());
    addDescription("propNames", folly::toJson(util::toJson(propNames_)), desc.get());
    addDescription("edges", folly::toJson(util::toJson(edges_)), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> Update::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("spaceId", folly::to<std::string>(spaceId_), desc.get());
    addDescription("schemaName", schemaName_, desc.get());
    addDescription("insertable", folly::to<std::string>(insertable_), desc.get());
    addDescription("updatedProps", folly::toJson(util::toJson(updatedProps_)), desc.get());
    addDescription("returnProps", folly::toJson(util::toJson(returnProps_)), desc.get());
    addDescription("condition", condition_, desc.get());
    addDescription("yieldNames", folly::toJson(util::toJson(yieldNames_)), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> UpdateVertex::explain() const {
    auto desc = Update::explain();
    addDescription("vid", vId_.toString(), desc.get());
    addDescription("tagId", folly::to<std::string>(tagId_), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> UpdateEdge::explain() const {
    auto desc = Update::explain();
    addDescription("srcId", srcId_.toString(), desc.get());
    addDescription("dstId", dstId_.toString(), desc.get());
    addDescription("rank", folly::to<std::string>(rank_), desc.get());
    addDescription("edgeType", folly::to<std::string>(edgeType_), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> DeleteVertices::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("space", folly::to<std::string>(space_), desc.get());
    addDescription("vidRef", vidRef_ ? vidRef_->toString() : "", desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> DeleteEdges::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("space", folly::to<std::string>(space_), desc.get());
    addDescription("edgeKeyRef", folly::toJson(util::toJson(edgeKeyRef_)), desc.get());
    return desc;
}

}   // namespace graph
}   // namespace nebula
