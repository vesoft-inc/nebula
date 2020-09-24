/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include "storage/exec/EdgeNode.h"
#include "storage/exec/FilterNode.h"
#include "storage/exec/UpdateNode.h"
#include "storage/exec/UpdateResultNode.h"

namespace nebula {
namespace storage {

void UpdateEdgeProcessor::process(const cpp2::UpdateEdgeRequest& req) {
    spaceId_ = req.get_space_id();
    auto partId = req.get_part_id();
    edgeKey_ = req.get_edge_key();
    updatedProps_ = req.get_updated_props();
    if (req.__isset.insertable) {
        insertable_ = *req.get_insertable();
    }

    auto retCode = getSpaceVidLen(spaceId_);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        pushResultCode(retCode, partId);
        onFinished();
        return;
    }

    if (!NebulaKeyUtils::isValidVidLen(
            spaceVidLen_, edgeKey_.src.getStr(), edgeKey_.dst.getStr())) {
        LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                   << " space vid len: " << spaceVidLen_ << ",  edge srcVid: " << edgeKey_.src
                   << " dstVid: " << edgeKey_.dst;
        pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
        onFinished();
        return;
    }

    planContext_ = std::make_unique<PlanContext>(env_, spaceId_, spaceVidLen_);

    retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Failure build contexts!";
        pushResultCode(retCode, partId);
        onFinished();
        return;
    }

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getEdgeIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    VLOG(3) << "Update edge, spaceId: " << spaceId_ << ", partId:  " << partId
            << ", src: " << edgeKey_.get_src() << ", edge_type: " << edgeKey_.get_edge_type()
            << ", dst: " << edgeKey_.get_dst() << ", ranking: " << edgeKey_.get_ranking();

    auto plan = buildPlan(&resultDataSet_);

    auto ret = plan.go(partId, edgeKey_);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(ret, spaceId_, partId);
        if (ret == kvstore::ResultCode::ERR_RESULT_FILTERED) {
            onProcessFinished();
        }
    } else {
        onProcessFinished();
    }
    onFinished();
    return;
}

cpp2::ErrorCode
UpdateEdgeProcessor::checkAndBuildContexts(const cpp2::UpdateEdgeRequest& req) {
    // Build edgeContext_.schemas_
    auto retCode = buildEdgeSchema();
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    // Build edgeContext_.propContexts_ edgeTypeProps_
    retCode = buildEdgeContext(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    // Build edgeContext_.ttlInfo_
    buildEdgeTTLInfo();
    return cpp2::ErrorCode::SUCCEEDED;
}

/*
The storage plan of update(upsert) edge looks like this:
             +--------+----------+
             | UpdateEdgeResNode |
             +--------+----------+
                      |
             +--------+----------+
             |   UpdateEdgeNode  |
             +--------+----------+
                      |
             +--------+----------+
             |    FilterNode     |
             +--------+----------+
                      |
             +--------+----------+
             |   FetchEdgeNode   |
             +-------------------+
*/
StoragePlan<cpp2::EdgeKey> UpdateEdgeProcessor::buildPlan(nebula::DataSet* result) {
    StoragePlan<cpp2::EdgeKey> plan;
    // because update edgetype is one
    auto edgeUpdate = std::make_unique<FetchEdgeNode>(planContext_.get(),
                                                      &edgeContext_,
                                                      edgeContext_.propContexts_[0].first,
                                                      &(edgeContext_.propContexts_[0].second));

    auto filterNode = std::make_unique<FilterNode<cpp2::EdgeKey>>(planContext_.get(),
                                                                  edgeUpdate.get(),
                                                                  expCtx_.get(),
                                                                  filterExp_.get());
    filterNode->addDependency(edgeUpdate.get());

    auto updateNode = std::make_unique<UpdateEdgeNode>(planContext_.get(),
                                                       indexes_,
                                                       updatedProps_,
                                                       filterNode.get(),
                                                       insertable_,
                                                       depPropMap_,
                                                       expCtx_.get(),
                                                       &edgeContext_);
    updateNode->addDependency(filterNode.get());

    auto resultNode = std::make_unique<UpdateResNode<cpp2::EdgeKey>>(planContext_.get(),
                                                                     updateNode.get(),
                                                                     getReturnPropsExp(),
                                                                     expCtx_.get(),
                                                                     result);
    resultNode->addDependency(updateNode.get());
    plan.addNode(std::move(edgeUpdate));
    plan.addNode(std::move(filterNode));
    plan.addNode(std::move(updateNode));
    plan.addNode(std::move(resultNode));
    return plan;
}

// Get all edge schema in spaceID
cpp2::ErrorCode UpdateEdgeProcessor::buildEdgeSchema() {
    auto edges = env_->schemaMan_->getAllVerEdgeSchema(spaceId_);
    if (!edges.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }

    edgeContext_.schemas_ = std::move(edges).value();
    return cpp2::ErrorCode::SUCCEEDED;
}

// edgeContext.propContexts_ return prop, filter prop, update prop
cpp2::ErrorCode
UpdateEdgeProcessor::buildEdgeContext(const cpp2::UpdateEdgeRequest& req) {
    // Build default edge context
    auto edgeNameRet = env_->schemaMan_->toEdgeName(spaceId_, std::abs(edgeKey_.edge_type));
    if (!edgeNameRet.ok()) {
        VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType "
                << std::abs(edgeKey_.edge_type);
        return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
    }
    auto edgeName = edgeNameRet.value();

    std::vector<PropContext> ctxs;
    edgeContext_.propContexts_.emplace_back(edgeKey_.edge_type, std::move(ctxs));
    edgeContext_.indexMap_.emplace(edgeKey_.edge_type, edgeContext_.propContexts_.size() - 1);
    edgeContext_.edgeNames_.emplace(edgeKey_.edge_type, edgeName);

    // Build context of the update edge prop
    for (auto& edgeProp : updatedProps_) {
        EdgePropertyExpression edgePropExp(new std::string(edgeName),
                                           new std::string(edgeProp.get_name()));
        auto retCode = checkExp(&edgePropExp, false, false);
        if (retCode != cpp2::ErrorCode::SUCCEEDED) {
            VLOG(1) << "Invalid update edge expression!";
            return retCode;
        }

        auto updateExp = Expression::decode(edgeProp.get_value());
        if (!updateExp) {
            VLOG(1) << "Can't decode the prop's value " << edgeProp.get_value();
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }

        valueProps_.clear();
        retCode = checkExp(updateExp.get(), false, false, insertable_);
        if (retCode != cpp2::ErrorCode::SUCCEEDED) {
            return retCode;
        }
        if (insertable_) {
            depPropMap_.emplace_back(std::make_pair(edgeProp.get_name(), valueProps_));
        }
    }

    // Return props
    if (req.__isset.return_props) {
        for (auto& prop : *req.get_return_props()) {
            auto colExp = Expression::decode(prop);
            if (!colExp) {
                VLOG(1) << "Can't decode the return expression";
                return cpp2::ErrorCode::E_INVALID_UPDATER;
            }
            auto retCode = checkExp(colExp.get(), true, false);
            if (retCode != cpp2::ErrorCode::SUCCEEDED) {
                return retCode;
            }
            returnPropsExp_.emplace_back(std::move(colExp));
        }
    }

    // Condition
    if (req.__isset.condition) {
        const auto& filterStr = *req.get_condition();
        if (!filterStr.empty()) {
            filterExp_ = Expression::decode(filterStr);
            if (!filterExp_) {
                VLOG(1) << "Can't decode the filter " << filterStr;
                return cpp2::ErrorCode::E_INVALID_FILTER;
            }
            auto retCode = checkExp(filterExp_.get(), false, true);
            if (retCode != cpp2::ErrorCode::SUCCEEDED) {
                return retCode;
            }
        }
    }

    // update edge only handle one edgetype
    // maybe no updated prop, filter prop, return prop
    auto iter = edgeContext_.edgeNames_.find(edgeKey_.edge_type);
    if (edgeContext_.edgeNames_.size() != 1 ||
        iter == edgeContext_.edgeNames_.end()) {
        VLOG(1) << "should only contain one edge in update edge!";
        return cpp2::ErrorCode::E_INVALID_UPDATER;
    }

    planContext_->edgeType_ = edgeKey_.edge_type;
    planContext_->edgeName_ = iter->second;
    auto schemaMap = edgeContext_.schemas_;
    auto iterSchema = schemaMap.find(std::abs(edgeKey_.edge_type));
    if (iterSchema != schemaMap.end()) {
        auto schemas = iterSchema->second;
        auto schema = schemas.back().get();
        if (!schema) {
            VLOG(1) << "Fail to get schema in edgeType " << edgeKey_.edge_type;
            return cpp2::ErrorCode::E_UNKNOWN;
        }
        planContext_->edgeSchema_ = schema;
    } else {
        VLOG(1) << "Fail to get schema in edgeType " << edgeKey_.edge_type;
        return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
    }

    if (expCtx_ == nullptr) {
        expCtx_ = std::make_unique<StorageExpressionContext>(spaceVidLen_,
                                                             planContext_->edgeName_,
                                                             planContext_->edgeSchema_,
                                                             true);
    }

    return cpp2::ErrorCode::SUCCEEDED;
}

void UpdateEdgeProcessor::onProcessFinished() {
    resp_.set_props(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
