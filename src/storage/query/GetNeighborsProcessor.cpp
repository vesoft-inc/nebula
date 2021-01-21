/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/GetNeighborsProcessor.h"
#include "storage/StorageFlags.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/EdgeNode.h"
#include "storage/exec/HashJoinNode.h"
#include "storage/exec/FilterNode.h"
#include "storage/exec/AggregateNode.h"
#include "storage/exec/GetNeighborsNode.h"

namespace nebula {
namespace storage {

void GetNeighborsProcessor::process(const cpp2::GetNeighborsRequest& req) {
    if (executor_ != nullptr) {
        executor_->add([&req, this] () {
            this->doProcess(req);
        });
    } else {
        doProcess(req);
    }
}

void GetNeighborsProcessor::doProcess(const cpp2::GetNeighborsRequest& req) {
    spaceId_ = req.get_space_id();
    auto retCode = getSpaceVidLen(spaceId_);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            pushResultCode(retCode, p.first);
        }
        onFinished();
        return;
    }
    planContext_ = std::make_unique<PlanContext>(env_, spaceId_, spaceVidLen_, isIntId_);
    expCtx_ = std::make_unique<StorageExpressionContext>(spaceVidLen_, isIntId_);

    retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            pushResultCode(retCode, p.first);
        }
        onFinished();
        return;
    }

    int64_t limit = FLAGS_max_edge_returned_per_vertex;
    bool random = false;
    if (req.traverse_spec.__isset.limit) {
        if (req.traverse_spec.limit >= 0) {
            limit = req.traverse_spec.limit;
        }
        if (req.traverse_spec.__isset.random) {
            random = req.traverse_spec.random;
        }
    }

    auto plan = buildPlan(&resultDataSet_, limit, random);
    std::unordered_set<PartitionID> failedParts;
    for (const auto& partEntry : req.get_parts()) {
        auto partId = partEntry.first;
        for (const auto& row : partEntry.second) {
            CHECK_GE(row.values.size(), 1);
            auto vId = row.values[0].getStr();

            if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vId)) {
                LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                           << " space vid len: " << spaceVidLen_ << ",  vid is " << vId;
                pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
                onFinished();
                return;
            }

            // the first column of each row would be the vertex id
            auto ret = plan.go(partId, vId);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                if (failedParts.find(partId) == failedParts.end()) {
                    failedParts.emplace(partId);
                    handleErrorCode(ret, spaceId_, partId);
                }
            }
        }
    }
    onProcessFinished();
    onFinished();
}

StoragePlan<VertexID> GetNeighborsProcessor::buildPlan(nebula::DataSet* result,
                                                       int64_t limit,
                                                       bool random) {
    /*
    The StoragePlan looks like this:
                 +--------+---------+
                 | GetNeighborsNode |
                 +--------+---------+
                          |
                 +--------+---------+
                 |   AggregateNode  |
                 +--------+---------+
                          |
                 +--------+---------+
                 |    FilterNode    |
                 +--------+---------+
                          |
                 +--------+---------+
             +-->+   HashJoinNode   +<----+
             |   +------------------+     |
    +--------+---------+        +---------+--------+
    |     TagNodes     |        |     EdgeNodes    |
    +------------------+        +------------------+
    */
    StoragePlan<VertexID> plan;
    std::vector<TagNode*> tags;
    for (const auto& tc : tagContext_.propContexts_) {
        auto tag = std::make_unique<TagNode>(
                planContext_.get(), &tagContext_, tc.first, &tc.second);
        tags.emplace_back(tag.get());
        plan.addNode(std::move(tag));
    }
    std::vector<EdgeNode<VertexID>*> edges;
    for (const auto& ec : edgeContext_.propContexts_) {
        auto edge = std::make_unique<SingleEdgeNode>(
                planContext_.get(), &edgeContext_, ec.first, &ec.second);
        edges.emplace_back(edge.get());
        plan.addNode(std::move(edge));
    }

    auto hashJoin = std::make_unique<HashJoinNode>(
            planContext_.get(), tags, edges, &tagContext_, &edgeContext_, expCtx_.get());
    for (auto* tag : tags) {
        hashJoin->addDependency(tag);
    }
    for (auto* edge : edges) {
        hashJoin->addDependency(edge);
    }
    IterateNode<VertexID>* join = hashJoin.get();
    IterateNode<VertexID>* upstream = hashJoin.get();
    plan.addNode(std::move(hashJoin));

    if (filter_) {
        auto filter = std::make_unique<FilterNode<VertexID>>(
                planContext_.get(), upstream, expCtx_.get(), filter_.get());
        filter->addDependency(upstream);
        upstream = filter.get();
        plan.addNode(std::move(filter));
    }

    if (edgeContext_.statCount_ > 0) {
        auto agg = std::make_unique<AggregateNode<VertexID>>(
                planContext_.get(), upstream, &edgeContext_);
        agg->addDependency(upstream);
        upstream = agg.get();
        plan.addNode(std::move(agg));
    }

    std::unique_ptr<GetNeighborsNode> output;
    if (random) {
        output = std::make_unique<GetNeighborsSampleNode>(
                planContext_.get(), join, upstream, &edgeContext_, result, limit);
    } else {
        output = std::make_unique<GetNeighborsNode>(
                planContext_.get(), join, upstream, &edgeContext_, result, limit);
    }
    output->addDependency(upstream);
    plan.addNode(std::move(output));

    return plan;
}

cpp2::ErrorCode GetNeighborsProcessor::checkAndBuildContexts(const cpp2::GetNeighborsRequest& req) {
    resultDataSet_.colNames.emplace_back(kVid);
    // reserve second colname for stat
    resultDataSet_.colNames.emplace_back("_stats");

    auto code = getSpaceVertexSchema();
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = getSpaceEdgeSchema();
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = buildTagContext(req.get_traverse_spec());
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = buildEdgeContext(req.get_traverse_spec());
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = buildYields(req);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    code = buildFilter(req);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::buildTagContext(const cpp2::TraverseSpec& req) {
    cpp2::ErrorCode ret = cpp2::ErrorCode::SUCCEEDED;
    if (!req.__isset.vertex_props) {
        // If the list is not given, no prop will be returned.
        return cpp2::ErrorCode::SUCCEEDED;
    } else if (req.vertex_props.empty()) {
        // If no props specified, get all property of all tagId in space
        auto returnProps = buildAllTagProps();
        // generate tag prop context
        ret = handleVertexProps(returnProps);
        buildTagColName(returnProps);
    } else {
        // Generate related props according to property specified.
        // not use const reference because we need to modify it when all property need to return
        auto returnProps = std::move(req.vertex_props);
        ret = handleVertexProps(returnProps);
        buildTagColName(returnProps);
    }

    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildTagTTLInfo();
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::buildEdgeContext(const cpp2::TraverseSpec& req) {
    edgeContext_.offset_ = tagContext_.propContexts_.size() + 2;
    cpp2::ErrorCode ret = cpp2::ErrorCode::SUCCEEDED;
    if (!req.__isset.edge_props) {
        // If the list is not given, no prop will be returned.
        return cpp2::ErrorCode::SUCCEEDED;
    } else if (req.edge_props.empty()) {
        // If no props specified, get all property of all edge type in space
        auto returnProps = buildAllEdgeProps(req.edge_direction);
        // generate edge prop context
        ret = handleEdgeProps(returnProps);
        buildEdgeColName(returnProps);
    } else {
        // Generate related props according to property specified.
        // not use const reference because we need to modify it when all property need to return
        auto returnProps = std::move(req.edge_props);
        ret = handleEdgeProps(returnProps);
        buildEdgeColName(returnProps);
    }

    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    // TODO : verify req.__isset.stat_props
    ret = handleEdgeStatProps(req.stat_props);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildEdgeTTLInfo();
    return cpp2::ErrorCode::SUCCEEDED;
}

void GetNeighborsProcessor::buildTagColName(const std::vector<cpp2::VertexProp>& tagProps) {
    for (const auto& tagProp : tagProps) {
        auto tagId = tagProp.tag;
        auto tagName = tagContext_.tagNames_[tagId];
        std::string colName = "_tag:" + tagName;
        for (const auto& prop : tagProp.props) {
            colName += ":" + std::move(prop);
        }
        VLOG(1) << "append col name: " << colName;
        resultDataSet_.colNames.emplace_back(std::move(colName));
    }
}

void GetNeighborsProcessor::buildEdgeColName(const std::vector<cpp2::EdgeProp>& edgeProps) {
    for (const auto& edgeProp : edgeProps) {
        auto edgeType = edgeProp.type;
        auto edgeName = edgeContext_.edgeNames_[edgeType];
        std::string colName = "_edge:";
        colName.append(edgeType > 0 ? "+" : "-")
               .append(edgeName);
        for (const auto& prop : edgeProp.props) {
            colName += ":" + std::move(prop);
        }
        VLOG(1) << "append col name: " << colName;
        resultDataSet_.colNames.emplace_back(std::move(colName));
    }
}

cpp2::ErrorCode GetNeighborsProcessor::handleEdgeStatProps(
        const std::vector<cpp2::StatProp>& statProps) {
    edgeContext_.statCount_ = statProps.size();
    std::string colName = "_stats";
    for (size_t statIdx = 0; statIdx < statProps.size(); statIdx++) {
        const auto& statProp = statProps[statIdx];
        auto exp = Expression::decode(statProp.prop);
        if (exp == nullptr) {
            return cpp2::ErrorCode::E_INVALID_STAT_TYPE;
        }

        // we only support edge property/rank expression for now
        switch (exp->kind()) {
            case Expression::Kind::kEdgeRank:
            case Expression::Kind::kEdgeProperty: {
                auto* edgeExp = static_cast<const PropertyExpression*>(exp.get());
                const auto* edgeName = edgeExp->sym();
                const auto* propName = edgeExp->prop();
                auto edgeRet = this->env_->schemaMan_->toEdgeType(spaceId_, *edgeName);
                if (!edgeRet.ok()) {
                    VLOG(1) << "Can't find edge " << *edgeName << ", in space " << spaceId_;
                    return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
                }

                auto edgeType = edgeRet.value();
                auto iter = edgeContext_.schemas_.find(std::abs(edgeType));
                if (iter == edgeContext_.schemas_.end()) {
                    VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType "
                            << std::abs(edgeType);
                    return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
                }
                CHECK(!iter->second.empty());
                const auto& edgeSchema = iter->second.back();

                const meta::SchemaProviderIf::Field* field = nullptr;
                if (exp->kind() == Expression::Kind::kEdgeProperty) {
                    field = edgeSchema->field(*propName);
                    if (field == nullptr) {
                        VLOG(1) << "Can't find related prop " << *propName
                                << " on edge " << *edgeName;
                        return cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
                    }
                    auto ret = checkStatType(field, statProp.stat);
                    if (ret != cpp2::ErrorCode::SUCCEEDED) {
                        return ret;
                    }
                }
                auto statInfo = std::make_pair(statIdx, statProp.stat);
                addPropContextIfNotExists(edgeContext_.propContexts_,
                                          edgeContext_.indexMap_,
                                          edgeContext_.edgeNames_,
                                          edgeType,
                                          edgeName,
                                          propName,
                                          field,
                                          false,
                                          false,
                                          &statInfo);
                break;
            }
            default: {
                return cpp2::ErrorCode::E_INVALID_STAT_TYPE;
            }
        }
        colName += ":" + std::move(statProp.alias);
    }
    resultDataSet_.colNames[1] = std::move(colName);

    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode GetNeighborsProcessor::checkStatType(const meta::SchemaProviderIf::Field* field,
                                                     cpp2::StatType statType) {
    // todo(doodle): how to deal with nullable fields? For now, null add anything is null,
    // if there is even one null, the result will be invalid
    auto fType = field->type();
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
