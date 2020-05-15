/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/GetNeighborsProcessor.h"
#include "storage/StorageFlags.h"
#include "storage/exec/FilterNode.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/EdgeNode.h"

namespace nebula {
namespace storage {

void GetNeighborsProcessor::process(const cpp2::GetNeighborsRequest& req) {
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
    for (const auto& partEntry : req.get_parts()) {
        auto partId = partEntry.first;
        for (const auto& input : partEntry.second) {
            CHECK_GE(input.columns.size(), 1);
            auto vId = input.columns[0].getStr();

            FilterNode filter;
            nebula::Row resultRow;
            // vertexId is the first column
            resultRow.columns.emplace_back(vId);
            // reserve second column for stat
            resultRow.columns.emplace_back(NullType::__NULL__);

            // the first column of each row would be the vertex id
            auto dag = buildDAG(partId, vId, &filter, &resultRow);
            auto ret = dag->go().get();
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                if (failedParts.find(partId) == failedParts.end()) {
                    failedParts.emplace(partId);
                    handleErrorCode(ret, spaceId_, partId);
                }
            } else {
                resultDataSet_.rows.emplace_back(std::move(resultRow));
            }
        }
    }
    onProcessFinished();
    onFinished();
}

std::unique_ptr<StorageDAG> GetNeighborsProcessor::buildDAG(PartitionID partId,
                                                            const VertexID& vId,
                                                            FilterNode* filter,
                                                            nebula::Row* row) {
    auto dag = std::make_unique<StorageDAG>();
    auto tag = std::make_unique<TagNode>(
            &tagContext_, env_, spaceId_, partId, spaceVidLen_, vId, filter, row);
    auto tagIdx = dag->addNode(std::move(tag));
    auto edge = std::make_unique<EdgeTypePrefixScanNode>(
            &edgeContext_, env_, spaceId_, partId, spaceVidLen_, vId, exp_.get(), filter, row);
    edge->addDependency(dag->getNode(tagIdx));
    dag->addNode(std::move(edge));
    return dag;
}

cpp2::ErrorCode GetNeighborsProcessor::checkAndBuildContexts(const cpp2::GetNeighborsRequest& req) {
    resultDataSet_.colNames.emplace_back("_vid");
    resultDataSet_.colNames.emplace_back("_stats");

    auto code = getSpaceSchema();
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = buildTagContext(req);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = buildEdgeContext(req);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = buildFilter(req);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::getSpaceSchema() {
    auto tags = env_->schemaMan_->getAllVerTagSchema(spaceId_);
    if (!tags.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    auto edges = env_->schemaMan_->getAllVerEdgeSchema(spaceId_);
    if (!edges.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }

    tagContext_.schemas_ = std::move(tags).value();
    edgeContext_.schemas_ = std::move(edges).value();
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::buildTagContext(const cpp2::GetNeighborsRequest& req) {
    std::vector<ReturnProp> returnProps;
    if (!req.__isset.vertex_props) {
        // If no tagId specified, get all property of all tagId in space
        returnProps = buildAllTagProps();
    } else {
        auto ret = prepareVertexProps(req.vertex_props, returnProps);
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

cpp2::ErrorCode GetNeighborsProcessor::prepareVertexProps(
        const std::vector<std::string>& vertexProps,
        std::vector<ReturnProp>& returnProps) {
    // todo(doodle): wait
    /*
    for (auto& vertexProp : vertexProps) {
        // If there is no property specified, add all property of latest schema to vertexProps
        if (vertexProp.names.empty()) {
            auto tagId = vertexProp.tag;
            auto tagSchema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
            if (!tagSchema) {
                VLOG(1) << "Can't find spaceId " << spaceId_ << " tag " << tagId;
                return cpp2::ErrorCode::E_TAG_NOT_FOUND;
            }

            auto count = tagSchema->getNumFields();
            for (size_t i = 0; i < count; i++) {
                auto name = tagSchema->getFieldName(i);
                vertexProp.names.emplace_back(std::move(name));
            }
        }
    }
    */
    UNUSED(vertexProps);
    UNUSED(returnProps);
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::buildEdgeContext(const cpp2::GetNeighborsRequest& req) {
    std::vector<ReturnProp> returnProps;
    // If no edgeType specified, get all property of all edge type in space
    if (!req.__isset.edge_props) {
        scanAllEdges_ = true;
        returnProps = buildAllEdgeProps(req.edge_direction);
    } else {
        // generate related props if no edge type or property specified
        auto ret = prepareEdgeProps(req.edge_props, returnProps);
        if (ret != cpp2::ErrorCode::SUCCEEDED) {
            return ret;
        }
    }

    // generate edge prop context
    auto ret = handleEdgeProps(returnProps);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    ret = handleEdgeStatProps(req.stat_props);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildEdgeTTLInfo();
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::prepareEdgeProps(const std::vector<std::string>& edgeProps,
                                                        std::vector<ReturnProp>& returnProps) {
    // todo(doodle): wait
    /*
    for (auto& edgeProp : edgeProps) {
        // If there is no property specified, add all property of latest schema to edgeProps
        if (edgeProp.names.empty()) {
            auto edgeType = edgeProp.type;
            auto edgeSchema = env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
            if (!edgeSchema) {
                VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
                return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
            }

            auto count = edgeSchema->getNumFields();
            for (size_t i = 0; i < count; i++) {
                auto name = edgeSchema->getFieldName(i);
                edgeProp.names.emplace_back(std::move(name));
            }
        }
    }
    */
    UNUSED(edgeProps);
    UNUSED(returnProps);
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::handleEdgeStatProps(
        const std::vector<cpp2::StatProp>& statProps) {
    statCount_ = statProps.size();
    // todo(doodle): since we only keep one kind of stat in PropContext, there could be a problem
    // if we specified multiple stat of same prop
    for (size_t idx = 0; idx < statCount_; idx++) {
        // todo(doodle): wait
        /*
        const auto& prop = statProps[idx];
        const auto edgeType = prop.type;
        const auto& name = prop.name;

        auto schema = env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
        if (!schema) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        const meta::SchemaProviderIf::Field* field = nullptr;
        if (name != "_rank") {
            field = schema->field(name);
            if (field == nullptr) {
                VLOG(1) << "Can't find prop " << name << " edgeType " << edgeType;
                return cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            }
            auto ret = checkStatType(field->type(), prop.stat);
            if (ret != cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
        }

        // find if corresponding edgeType contexts exists
        auto edgeIter = edgeContext_.indexMap_.find(edgeType);
        if (edgeIter != edgeContext_.indexMap_.end()) {
            // find if corresponding PropContext exists
            auto& ctxs = edgeContext_.propContexts_[edgeIter->second].second;
            auto propIter = std::find_if(ctxs.begin(), ctxs.end(),
                [&] (const auto& propContext) {
                    return propContext.name_ == name;
                });
            if (propIter != ctxs.end()) {
                propIter->hasStat_ = true;
                propIter->statIndex_ = idx;
                propIter->statType_ = prop.stat;
                continue;
            } else {
                auto ctx = buildPropContextWithStat(name, idx, prop.stat, field);
                ctxs.emplace_back(std::move(ctx));
            }
        } else {
            std::vector<PropContext> ctxs;
            auto ctx = buildPropContextWithStat(name, idx, prop.stat, field);
            ctxs.emplace_back(std::move(ctx));
            edgeContext_.propContexts_.emplace_back(edgeType, std::move(ctxs));
            edgeContext_.indexMap_.emplace(edgeType, edgeContext_.propContexts_.size() - 1);
        }
        */
    }

    return cpp2::ErrorCode::SUCCEEDED;
}

PropContext GetNeighborsProcessor::buildPropContextWithStat(
        const std::string& name,
        size_t idx,
        const cpp2::StatType& statType,
        const meta::SchemaProviderIf::Field* field) {
    PropContext ctx(name.c_str());
    ctx.hasStat_ = true;
    ctx.statIndex_ = idx;
    ctx.statType_ = statType;
    ctx.field_ = field;
    // for rank stat
    if (name == "_rank") {
        ctx.propInKeyType_ = PropContext::PropInKeyType::RANK;
    }
    return ctx;
}

cpp2::ErrorCode GetNeighborsProcessor::checkStatType(const meta::cpp2::PropertyType& fType,
                                                     cpp2::StatType statType) {
    switch (statType) {
        case cpp2::StatType::SUM:
        case cpp2::StatType::AVG:
        case cpp2::StatType::MIN:
        case cpp2::StatType::MAX: {
            if (fType == meta::cpp2::PropertyType::INT64 ||
                fType == meta::cpp2::PropertyType::INT32 ||
                fType == meta::cpp2::PropertyType::INT16 ||
                fType == meta::cpp2::PropertyType::INT8 ||
                fType == meta::cpp2::PropertyType::FLOAT ||
                fType == meta::cpp2::PropertyType::DOUBLE) {
                return cpp2::ErrorCode::SUCCEEDED;
            }
            return cpp2::ErrorCode::E_INVALID_STAT_TYPE;
        }
        case cpp2::StatType::COUNT: {
             break;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

void GetNeighborsProcessor::onProcessFinished() {
    resp_.set_vertices(std::move(resultDataSet_));
}


}  // namespace storage
}  // namespace nebula
