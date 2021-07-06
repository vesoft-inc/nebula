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

ProcessorCounters kUpdateEdgeCounters;

void UpdateEdgeProcessor::process(const cpp2::UpdateEdgeRequest& req) {
    if (executor_ != nullptr) {
        executor_->add([req, this] () {
            this->doProcess(req);
        });
    } else {
        doProcess(req);
    }
}

void UpdateEdgeProcessor::doProcess(const cpp2::UpdateEdgeRequest& req) {
    spaceId_ = req.get_space_id();
    auto partId = req.get_part_id();
    edgeKey_ = req.get_edge_key();
    updatedProps_ = req.get_updated_props();
    if (req.insertable_ref().has_value()) {
        insertable_ = *req.insertable_ref();
    }

    auto retCode = getSpaceVidLen(spaceId_);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        pushResultCode(retCode, partId);
        onFinished();
        return;
    }

    if (!NebulaKeyUtils::isValidVidLen(
            spaceVidLen_, edgeKey_.get_src().getStr(), edgeKey_.get_dst().getStr())) {
        LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                   << " space vid len: " << spaceVidLen_
                   << ",  edge srcVid: " << edgeKey_.get_src()
                   << " dstVid: " << edgeKey_.get_dst();
        pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_VID, partId);
        onFinished();
        return;
    }

    planContext_ = std::make_unique<PlanContext>(env_, spaceId_, spaceVidLen_, isIntId_);
    context_ = std::make_unique<RunTimeContext>(planContext_.get());
    if (env_->txnMan_ && env_->txnMan_->enableToss(spaceId_)) {
        planContext_->defaultEdgeVer_ = 1L;
    }
    retCode = checkAndBuildContexts(req);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Failure build contexts: " << apache::thrift::util::enumNameSafe(retCode);
        pushResultCode(retCode, partId);
        onFinished();
        return;
    }

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getEdgeIndexes(spaceId_);
    if (!iRet.ok()) {
        LOG(ERROR) << iRet.status();
        pushResultCode(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, partId);
        onFinished();
        return;
    }
    indexes_ = std::move(iRet).value();

    VLOG(3) << "Update edge, spaceId: " << spaceId_ << ", partId:  " << partId
            << ", src: " << edgeKey_.get_src() << ", edge_type: " << edgeKey_.get_edge_type()
            << ", dst: " << edgeKey_.get_dst() << ", ranking: " << edgeKey_.get_ranking();

    auto plan = buildPlan(&resultDataSet_);

    auto ret = plan.go(partId, edgeKey_);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(ret, spaceId_, partId);
        if (ret == nebula::cpp2::ErrorCode::E_FILTER_OUT) {
            onProcessFinished();
        }
    } else {
        onProcessFinished();
    }
    onFinished();
    return;
}

nebula::cpp2::ErrorCode
UpdateEdgeProcessor::checkAndBuildContexts(const cpp2::UpdateEdgeRequest& req) {
    // Build edgeContext_.schemas_
    auto retCode = buildEdgeSchema();
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    // Build edgeContext_.propContexts_ edgeTypeProps_
    retCode = buildEdgeContext(req);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    // Build edgeContext_.ttlInfo_
    buildEdgeTTLInfo();
    return nebula::cpp2::ErrorCode::SUCCEEDED;
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
    auto edgeUpdate = std::make_unique<FetchEdgeNode>(context_.get(),
                                                      &edgeContext_,
                                                      edgeContext_.propContexts_[0].first,
                                                      &(edgeContext_.propContexts_[0].second));

    auto filterNode = std::make_unique<FilterNode<cpp2::EdgeKey>>(context_.get(),
                                                                  edgeUpdate.get(),
                                                                  expCtx_.get(),
                                                                  filterExp_);
    filterNode->addDependency(edgeUpdate.get());

    auto updateNode = std::make_unique<UpdateEdgeNode>(context_.get(),
                                                       indexes_,
                                                       updatedProps_,
                                                       filterNode.get(),
                                                       insertable_,
                                                       depPropMap_,
                                                       expCtx_.get(),
                                                       &edgeContext_);
    updateNode->addDependency(filterNode.get());

    auto resultNode = std::make_unique<UpdateResNode<cpp2::EdgeKey>>(context_.get(),
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
nebula::cpp2::ErrorCode UpdateEdgeProcessor::buildEdgeSchema() {
    auto edges = env_->schemaMan_->getAllVerEdgeSchema(spaceId_);
    if (!edges.ok()) {
        return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }

    edgeContext_.schemas_ = std::move(edges).value();
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

// edgeContext.propContexts_ return prop, filter prop, update prop
nebula::cpp2::ErrorCode
UpdateEdgeProcessor::buildEdgeContext(const cpp2::UpdateEdgeRequest& req) {
    // Build default edge context
    auto edgeNameRet = env_->schemaMan_->toEdgeName(spaceId_, std::abs(edgeKey_.get_edge_type()));
    if (!edgeNameRet.ok()) {
        VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType "
                << std::abs(edgeKey_.get_edge_type());
        return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
    }
    auto edgeName = edgeNameRet.value();

    std::vector<PropContext> ctxs;
    edgeContext_.propContexts_.emplace_back(edgeKey_.get_edge_type(), std::move(ctxs));
    edgeContext_.indexMap_.emplace(edgeKey_.get_edge_type(), edgeContext_.propContexts_.size() - 1);
    edgeContext_.edgeNames_.emplace(edgeKey_.get_edge_type(), edgeName);

    auto pool = context_->objPool();
    // Build context of the update edge prop
    for (auto& edgeProp : updatedProps_) {
        auto edgePropExp = EdgePropertyExpression::make(pool, edgeName, edgeProp.get_name());
        auto retCode = checkExp(edgePropExp, false, false);
        if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
            VLOG(1) << "Invalid update edge expression!";
            return retCode;
        }

        auto updateExp = Expression::decode(pool, edgeProp.get_value());
        if (!updateExp) {
            VLOG(1) << "Can't decode the prop's value " << edgeProp.get_value();
            return nebula::cpp2::ErrorCode::E_INVALID_UPDATER;
        }

        valueProps_.clear();
        retCode = checkExp(updateExp, false, false, insertable_);
        if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return retCode;
        }
        if (insertable_) {
            depPropMap_.emplace_back(std::make_pair(edgeProp.get_name(), valueProps_));
        }
    }

    // Return props
    if (req.return_props_ref().has_value()) {
        for (auto& prop : *req.return_props_ref()) {
            auto colExp = Expression::decode(pool, prop);
            if (!colExp) {
                VLOG(1) << "Can't decode the return expression";
                return nebula::cpp2::ErrorCode::E_INVALID_UPDATER;
            }
            auto retCode = checkExp(colExp, true, false);
            if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return retCode;
            }
            returnPropsExp_.emplace_back(std::move(colExp));
        }
    }

    // Condition
    if (req.condition_ref().has_value()) {
        const auto& filterStr = *req.condition_ref();
        if (!filterStr.empty()) {
            filterExp_ = Expression::decode(pool, filterStr);
            if (!filterExp_) {
                VLOG(1) << "Can't decode the filter " << filterStr;
                return nebula::cpp2::ErrorCode::E_INVALID_FILTER;
            }
            auto retCode = checkExp(filterExp_, false, true);
            if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return retCode;
            }
        }
    }

    // update edge only handle one edgetype
    // maybe no updated prop, filter prop, return prop
    auto iter = edgeContext_.edgeNames_.find(edgeKey_.get_edge_type());
    if (edgeContext_.edgeNames_.size() != 1 ||
        iter == edgeContext_.edgeNames_.end()) {
        VLOG(1) << "should only contain one edge in update edge!";
        return nebula::cpp2::ErrorCode::E_MUTATE_EDGE_CONFLICT;
    }

    context_->edgeType_ = edgeKey_.get_edge_type();
    context_->edgeName_ = iter->second;
    auto schemaMap = edgeContext_.schemas_;
    auto iterSchema = schemaMap.find(std::abs(edgeKey_.get_edge_type()));
    if (iterSchema != schemaMap.end()) {
        auto schemas = iterSchema->second;
        auto schema = schemas.back().get();
        if (!schema) {
            VLOG(1) << "Fail to get schema in edgeType " << edgeKey_.get_edge_type();
            return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        context_->edgeSchema_ = schema;
    } else {
        VLOG(1) << "Fail to get schema in edgeType " << edgeKey_.get_edge_type();
        return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
    }

    if (expCtx_ == nullptr) {
        expCtx_ = std::make_unique<StorageExpressionContext>(spaceVidLen_,
                                                             isIntId_,
                                                             context_->edgeName_,
                                                             context_->edgeSchema_,
                                                             true);
    }

    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void UpdateEdgeProcessor::onProcessFinished() {
    resp_.set_props(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
