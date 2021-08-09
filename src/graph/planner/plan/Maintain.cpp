/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/plan/Maintain.h"

#include <sstream>

#include "util/ToJson.h"

namespace nebula {
namespace graph {

std::unique_ptr<PlanNodeDescription> CreateSchemaNode::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("name", name_, desc.get());
    addDescription("ifNotExists", util::toJson(ifNotExists_), desc.get());
    addDescription("schema", folly::toJson(util::toJson(schema_)), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> AlterSchemaNode::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("space", folly::to<std::string>(space_), desc.get());
    addDescription("name", name_, desc.get());
    addDescription("schemaItems", folly::toJson(util::toJson(schemaItems_)), desc.get());
    addDescription("schemaProp", folly::toJson(util::toJson(schemaProp_)), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> DescSchemaNode::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("name", name_, desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> DropSchemaNode::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("name", name_, desc.get());
    addDescription("ifExists", util::toJson(ifExists_), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> CreateIndexNode::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("schemaName", schemaName_, desc.get());
    addDescription("indexName", indexName_, desc.get());
    std::vector<std::string> fields;
    for (const auto& field : fields_) {
        fields.emplace_back(field.get_name());
    }
    addDescription("fields", folly::toJson(util::toJson(fields)), desc.get());
    addDescription("ifNotExists", folly::to<std::string>(ifNotExists_), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> DescIndexNode::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("indexName", indexName_, desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> DropIndexNode::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("indexName", indexName_, desc.get());
    addDescription("ifExists", util::toJson(ifExists_), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> DropFTIndexNode::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("indexName", name_, desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> CreateFTIndexNode::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("indexName", indexName_, desc.get());
    std::vector<std::string> fields;
    addDescription("fields", folly::toJson(util::toJson(index_.get_fields())), desc.get());
    return desc;
}

}   // namespace graph
}   // namespace nebula
