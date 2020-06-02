/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/GetPropProcessor.h"
#include "storage/exec/GetPropNode.h"
#include "storage/exec/AggregateNode.h"

namespace nebula {
namespace storage {

void GetPropProcessor::process(const cpp2::GetPropRequest& req) {
    spaceId_ = req.get_space_id();
    auto retCode = getSpaceVidLen(spaceId_);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            pushResultCode(retCode, p.first);
        }
        onFinished();
        return;
    }

    retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            pushResultCode(retCode, p.first);
        }
        onFinished();
        return;
    }

    std::unordered_set<PartitionID> failedParts;
    if (!isEdge_) {
        auto plan = buildTagPlan(&resultDataSet_);
        for (const auto& partEntry : req.get_parts()) {
            auto partId = partEntry.first;
            for (const auto& row : partEntry.second) {
                auto vId = row.columns[0].getStr();
                auto ret = plan.go(partId, vId);
                if (ret != kvstore::ResultCode::SUCCEEDED &&
                    failedParts.find(partId) == failedParts.end()) {
                    failedParts.emplace(partId);
                    handleErrorCode(ret, spaceId_, partId);
                }
            }
        }
    } else {
        auto plan = buildEdgePlan(&resultDataSet_);
        for (const auto& partEntry : req.get_parts()) {
            auto partId = partEntry.first;
            for (const auto& row : partEntry.second) {
                cpp2::EdgeKey edgeKey;
                edgeKey.src = row.columns[0].getStr();
                edgeKey.edge_type = row.columns[1].getInt();
                edgeKey.ranking = row.columns[2].getInt();
                edgeKey.dst = row.columns[3].getStr();
                auto ret = plan.go(partId, edgeKey);
                if (ret != kvstore::ResultCode::SUCCEEDED &&
                    failedParts.find(partId) == failedParts.end()) {
                    failedParts.emplace(partId);
                    handleErrorCode(ret, spaceId_, partId);
                }
            }
        }
    }
    onProcessFinished();
    onFinished();
}

StoragePlan<VertexID> GetPropProcessor::buildTagPlan(nebula::DataSet* result) {
    StoragePlan<VertexID> plan;
    std::vector<TagNode*> tags;
    for (const auto& tc : tagContext_.propContexts_) {
        auto tag = std::make_unique<TagNode>(
                &tagContext_, env_, spaceId_, spaceVidLen_, tc.first, &tc.second, exp_.get());
        tags.emplace_back(tag.get());
        plan.addNode(std::move(tag));
    }
    auto output = std::make_unique<GetTagPropNode>(tags);
    for (auto* tag : tags) {
        output->addDependency(tag);
    }
    auto aggrNode = std::make_unique<AggregateNode<VertexID>>(output.get(), result);
    aggrNode->addDependency(output.get());
    plan.addNode(std::move(output));
    plan.addNode(std::move(aggrNode));
    return plan;
}

StoragePlan<cpp2::EdgeKey> GetPropProcessor::buildEdgePlan(nebula::DataSet* result) {
    StoragePlan<cpp2::EdgeKey> plan;
    std::vector<EdgeNode<cpp2::EdgeKey>*> edges;
    for (const auto& ec : edgeContext_.propContexts_) {
        auto edge = std::make_unique<FetchEdgeNode>(
                &edgeContext_, env_, spaceId_, spaceVidLen_, ec.first, &ec.second, exp_.get());
        edges.emplace_back(edge.get());
        plan.addNode(std::move(edge));
    }
    auto output = std::make_unique<GetEdgePropNode>(edges, spaceVidLen_);
    for (auto* edge : edges) {
        output->addDependency(edge);
    }
    auto aggrNode = std::make_unique<AggregateNode<cpp2::EdgeKey>>(output.get(), result);
    aggrNode->addDependency(output.get());
    plan.addNode(std::move(output));
    plan.addNode(std::move(aggrNode));
    return plan;
}

cpp2::ErrorCode GetPropProcessor::checkColumnNames(const std::vector<std::string>& colNames) {
    // Column names for the pass-in data. When getting the vertex props, the first
    // column has to be "_vid", when getting the edge props, the first four columns
    // have to be "_src", "_type", "_ranking", and "_dst"
    if (colNames.size() != 1 && colNames.size() != 4) {
        return cpp2::ErrorCode::E_INVALID_OPERATION;
    }
    if (colNames.size() == 1 && colNames[0] == "_vid") {
        isEdge_ = false;
        return cpp2::ErrorCode::SUCCEEDED;
    } else if (colNames.size() == 4 &&
               colNames[0] == "_src" &&
               colNames[1] == "_type" &&
               colNames[2] == "_ranking" &&
               colNames[3] == "_dst") {
        isEdge_ = true;
        return cpp2::ErrorCode::SUCCEEDED;
    }
    return cpp2::ErrorCode::E_INVALID_OPERATION;
}

cpp2::ErrorCode GetPropProcessor::checkAndBuildContexts(const cpp2::GetPropRequest& req) {
    auto code = checkColumnNames(req.column_names);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    if (!isEdge_) {
        code = getSpaceVertexSchema();
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return code;
        }
        return buildTagContext(req);
    } else {
        code = getSpaceEdgeSchema();
        if (code != cpp2::ErrorCode::SUCCEEDED) {
            return code;
        }
        return buildEdgeContext(req);
    }
}

cpp2::ErrorCode GetPropProcessor::buildTagContext(const cpp2::GetPropRequest& req) {
    std::vector<ReturnProp> returnProps;
    if (req.props.empty()) {
        // If no props specified, get all property of all tagId in space
        returnProps = buildAllTagProps();
    } else {
        auto ret = prepareVertexProps(req.props, returnProps);
        if (ret != cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }
    }
    // generate tag prop context
    auto ret = handleVertexProps(returnProps);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildTagTTLInfo();
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetPropProcessor::buildEdgeContext(const cpp2::GetPropRequest& req) {
    std::vector<ReturnProp> returnProps;
    if (req.props.empty()) {
        // If no props specified, get all property of all tagId in space
        returnProps = buildAllEdgeProps(cpp2::EdgeDirection::BOTH);
    } else {
        auto ret = prepareEdgeProps(req.props, returnProps);
        if (ret != cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }
    }
    // generate edge prop context
    auto ret = handleEdgeProps(returnProps);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildEdgeTTLInfo();
    return cpp2::ErrorCode::SUCCEEDED;
}

kvstore::ResultCode GetPropProcessor::processOneVertex(PartitionID partId,
                                                       const std::string& prefix) {
    UNUSED(partId); UNUSED(prefix);
    return kvstore::ResultCode::SUCCEEDED;
}

void GetPropProcessor::onProcessFinished() {
    resp_.set_props(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
