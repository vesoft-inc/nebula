/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/GetPropProcessor.h"
#include "storage/exec/GetPropNode.h"

namespace nebula {
namespace storage {

ProcessorCounters kGetPropCounters;

void GetPropProcessor::process(const cpp2::GetPropRequest& req) {
    if (executor_ != nullptr) {
        executor_->add([req, this] () {
            this->doProcess(req);
        });
    } else {
        doProcess(req);
    }
}

void GetPropProcessor::doProcess(const cpp2::GetPropRequest& req) {
    spaceId_ = req.get_space_id();
    auto retCode = getSpaceVidLen(spaceId_);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            pushResultCode(retCode, p.first);
        }
        onFinished();
        return;
    }
    planContext_ = std::make_unique<PlanContext>(env_, spaceId_, spaceVidLen_, isIntId_);

    retCode = checkAndBuildContexts(req);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            pushResultCode(retCode, p.first);
        }
        onFinished();
        return;
    }

    // todo(doodle): specify by each query
    if (!FLAGS_query_concurrently) {
        runInSingleThread(req);
    } else {
        runInMultipleThread(req);
    }
}

void GetPropProcessor::runInSingleThread(const cpp2::GetPropRequest& req) {
    contexts_.emplace_back(RunTimeContext(planContext_.get()));
    std::unordered_set<PartitionID> failedParts;
    if (!isEdge_) {
        auto plan = buildTagPlan(&contexts_.front(), &resultDataSet_);
        for (const auto& partEntry : req.get_parts()) {
            auto partId = partEntry.first;
            for (const auto& row : partEntry.second) {
                auto vId = row.values[0].getStr();

                if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vId)) {
                    LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                               << " space vid len: " << spaceVidLen_ << ",  vid is " << vId;
                    pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_VID, partId);
                    onFinished();
                    return;
                }

                auto ret = plan.go(partId, vId);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED &&
                    failedParts.find(partId) == failedParts.end()) {
                    failedParts.emplace(partId);
                    handleErrorCode(ret, spaceId_, partId);
                }
            }
        }
    } else {
        auto plan = buildEdgePlan(&contexts_.front(), &resultDataSet_);
        for (const auto& partEntry : req.get_parts()) {
            auto partId = partEntry.first;
            for (const auto& row : partEntry.second) {
                cpp2::EdgeKey edgeKey;
                edgeKey.set_src(row.values[0].getStr());
                edgeKey.set_edge_type(row.values[1].getInt());
                edgeKey.set_ranking(row.values[2].getInt());
                edgeKey.set_dst(row.values[3].getStr());

                if (!NebulaKeyUtils::isValidVidLen(
                        spaceVidLen_,
                        (*edgeKey.src_ref()).getStr(),
                        (*edgeKey.dst_ref()).getStr())) {
                    LOG(ERROR) << "Space " << spaceId_ << " vertex length invalid, "
                               << "space vid len: " << spaceVidLen_
                               << ", edge srcVid: " << *edgeKey.src_ref()
                               << ", dstVid: " << *edgeKey.dst_ref();
                    pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_VID, partId);
                    onFinished();
                    return;
                }

                auto ret = plan.go(partId, edgeKey);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED &&
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

void GetPropProcessor::runInMultipleThread(const cpp2::GetPropRequest& req) {
    for (size_t i = 0; i < req.get_parts().size(); i++) {
        nebula::DataSet result = resultDataSet_;
        results_.emplace_back(std::move(result));
        contexts_.emplace_back(RunTimeContext(planContext_.get()));
    }
    size_t i = 0;
    std::vector<folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>>> futures;
    for (const auto& [partId, rows] : req.get_parts()) {
        futures.emplace_back(runInExecutor(&contexts_[i], &results_[i], partId, rows));
        i++;
    }

    folly::collectAll(futures).via(executor_).thenTry([this] (auto&& t) mutable {
        CHECK(!t.hasException());
        const auto& tries = t.value();
        for (size_t j = 0; j < tries.size(); j++) {
            CHECK(!tries[j].hasException());
            const auto& [code, partId] = tries[j].value();
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                handleErrorCode(code, spaceId_, partId);
            } else {
                resultDataSet_.append(std::move(results_[j]));
            }
        }
        this->onProcessFinished();
        this->onFinished();
    });
}

folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>>
GetPropProcessor::runInExecutor(RunTimeContext* context,
                                nebula::DataSet* result,
                                PartitionID partId,
                                const std::vector<nebula::Row>& rows) {
    return folly::via(
        executor_,
        [this, context, result, partId, input = std::move(rows)]() {
            if (!isEdge_) {
                auto plan = buildTagPlan(context, result);
                for (const auto& row : input) {
                    auto vId = row.values[0].getStr();

                    if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vId)) {
                        LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                                << " space vid len: " << spaceVidLen_ << ",  vid is " << vId;
                        return std::make_pair(nebula::cpp2::ErrorCode::E_INVALID_VID, partId);
                    }

                    auto ret = plan.go(partId, vId);
                    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                        return std::make_pair(ret, partId);
                    }
                }
                return std::make_pair(nebula::cpp2::ErrorCode::SUCCEEDED, partId);
            } else {
                auto plan = buildEdgePlan(context, result);
                for (const auto& row : input) {
                    cpp2::EdgeKey edgeKey;
                    edgeKey.set_src(row.values[0].getStr());
                    edgeKey.set_edge_type(row.values[1].getInt());
                    edgeKey.set_ranking(row.values[2].getInt());
                    edgeKey.set_dst(row.values[3].getStr());

                    if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_,
                                                       (*edgeKey.src_ref()).getStr(),
                                                       (*edgeKey.dst_ref()).getStr())) {
                        LOG(ERROR) << "Space " << spaceId_ << " vertex length invalid, "
                                << "space vid len: " << spaceVidLen_
                                << ", edge srcVid: " << *edgeKey.src_ref()
                                << ", dstVid: " << *edgeKey.dst_ref();
                        return std::make_pair(nebula::cpp2::ErrorCode::E_INVALID_VID, partId);
                    }

                    auto ret = plan.go(partId, edgeKey);
                    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                        return std::make_pair(ret, partId);
                    }
                }
                return std::make_pair(nebula::cpp2::ErrorCode::SUCCEEDED, partId);
            }
        });
}

StoragePlan<VertexID> GetPropProcessor::buildTagPlan(RunTimeContext* context,
                                                     nebula::DataSet* result) {
    StoragePlan<VertexID> plan;
    std::vector<TagNode*> tags;
    for (const auto& tc : tagContext_.propContexts_) {
        auto tag = std::make_unique<TagNode>(context, &tagContext_, tc.first, &tc.second);
        tags.emplace_back(tag.get());
        plan.addNode(std::move(tag));
    }
    auto output = std::make_unique<GetTagPropNode>(context, tags, result);
    for (auto* tag : tags) {
        output->addDependency(tag);
    }
    plan.addNode(std::move(output));
    return plan;
}

StoragePlan<cpp2::EdgeKey> GetPropProcessor::buildEdgePlan(RunTimeContext* context,
                                                           nebula::DataSet* result) {
    StoragePlan<cpp2::EdgeKey> plan;
    std::vector<EdgeNode<cpp2::EdgeKey>*> edges;
    for (const auto& ec : edgeContext_.propContexts_) {
        auto edge = std::make_unique<FetchEdgeNode>(context, &edgeContext_, ec.first, &ec.second);
        edges.emplace_back(edge.get());
        plan.addNode(std::move(edge));
    }
    auto output = std::make_unique<GetEdgePropNode>(context, edges, result);
    for (auto* edge : edges) {
        output->addDependency(edge);
    }
    plan.addNode(std::move(output));
    return plan;
}

nebula::cpp2::ErrorCode
GetPropProcessor::checkRequest(const cpp2::GetPropRequest& req) {
    if (!req.vertex_props_ref().has_value() && !req.edge_props_ref().has_value()) {
        return nebula::cpp2::ErrorCode::E_INVALID_OPERATION;
    } else if (req.vertex_props_ref().has_value() && req.edge_props_ref().has_value()) {
        return nebula::cpp2::ErrorCode::E_INVALID_OPERATION;
    }
    if (req.vertex_props_ref().has_value()) {
        isEdge_ = false;
    } else {
        isEdge_ = true;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode
GetPropProcessor::checkAndBuildContexts(const cpp2::GetPropRequest& req) {
    auto code = checkRequest(req);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return code;
    }
    if (!isEdge_) {
        code = getSpaceVertexSchema();
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return code;
        }
        return buildTagContext(req);
    } else {
        code = getSpaceEdgeSchema();
        if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return code;
        }
        return buildEdgeContext(req);
    }
}

nebula::cpp2::ErrorCode GetPropProcessor::buildTagContext(const cpp2::GetPropRequest& req) {
    // req.vertex_props_ref().has_value() checked in methon checkRequest
    auto returnProps = (*req.vertex_props_ref()).empty()
                     ? buildAllTagProps()
                     : *req.vertex_props_ref();
    auto ret = handleVertexProps(returnProps);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildTagColName(std::move(returnProps));
    buildTagTTLInfo();
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode GetPropProcessor::buildEdgeContext(const cpp2::GetPropRequest& req) {
    // req.edge_props_ref().has_value() checked in methon checkRequest
    auto returnProps = (*req.edge_props_ref()).empty()
                     ? buildAllEdgeProps(cpp2::EdgeDirection::BOTH)
                     : *req.edge_props_ref();
    auto ret = handleEdgeProps(returnProps);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildEdgeColName(std::move(returnProps));
    buildEdgeTTLInfo();
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void GetPropProcessor::buildTagColName(const std::vector<cpp2::VertexProp>& tagProps) {
    resultDataSet_.colNames.emplace_back(kVid);
    for (const auto& tagProp : tagProps) {
        auto tagId = tagProp.get_tag();
        auto tagName = tagContext_.tagNames_[tagId];
        for (const auto& prop : *tagProp.props_ref()) {
            resultDataSet_.colNames.emplace_back(tagName + "." + prop);
        }
    }
}

void GetPropProcessor::buildEdgeColName(const std::vector<cpp2::EdgeProp>& edgeProps) {
    for (const auto& edgeProp : edgeProps) {
        auto edgeType = edgeProp.get_type();
        auto edgeName = edgeContext_.edgeNames_[edgeType];
        for (const auto& prop : *edgeProp.props_ref()) {
            resultDataSet_.colNames.emplace_back(edgeName + "." + prop);
        }
    }
}

void GetPropProcessor::onProcessFinished() {
    resp_.set_props(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
