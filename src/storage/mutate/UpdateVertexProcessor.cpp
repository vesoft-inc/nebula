/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "storage/mutate/UpdateVertexProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include "storage/exec/TagNode.h"
#include "storage/exec/FilterNode.h"
#include "storage/exec/UpdateNode.h"
#include "storage/exec/UpdateResultNode.h"

namespace nebula {
namespace storage {

ProcessorCounters kUpdateVertexCounters;

void UpdateVertexProcessor::process(const cpp2::UpdateVertexRequest& req) {
    if (executor_ != nullptr) {
        executor_->add([req, this] () {
            this->doProcess(req);
        });
    } else {
        doProcess(req);
    }
}

void UpdateVertexProcessor::doProcess(const cpp2::UpdateVertexRequest& req) {
    spaceId_ = req.get_space_id();
    auto partId = req.get_part_id();
    auto vId = req.get_vertex_id();
    tagId_ = req.get_tag_id();
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

    if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vId.getStr())) {
        LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                   << " space vid len: " << spaceVidLen_ << ",  vid is " << vId;
        pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_VID, partId);
        onFinished();
        return;
    }
    planContext_ = std::make_unique<PlanContext>(env_, spaceId_, spaceVidLen_, isIntId_);
    context_ = std::make_unique<RunTimeContext>(planContext_.get());

    retCode = checkAndBuildContexts(req);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Failure build contexts!";
        pushResultCode(retCode, partId);
        onFinished();
        return;
    }

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getTagIndexes(spaceId_);
    if (!iRet.ok()) {
        LOG(ERROR) << iRet.status();
        pushResultCode(nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND, partId);
        onFinished();
        return;
    }
    indexes_ = std::move(iRet).value();

    VLOG(3) << "Update vertex, spaceId: " << spaceId_
            << ", partId: " << partId << ", vId: " << vId;
    auto plan = buildPlan(&resultDataSet_);
    auto ret = plan.go(partId, vId.getStr());

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
UpdateVertexProcessor::checkAndBuildContexts(const cpp2::UpdateVertexRequest& req) {
    // Build tagContext_.schemas_
    auto retCode = buildTagSchema();
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    // Build tagContext_.propContexts_  tagIdProps_
    retCode = buildTagContext(req);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    // Build tagContext_.ttlInfo_
    buildTagTTLInfo();
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

/*
The storage plan of update(upsert) vertex looks like this:
             +--------+---------+
             | UpdateTagResNode |
             +--------+---------+
                      |
             +--------+---------+
             |   UpdateTagNode  |
             +--------+---------+
                      |
             +--------+---------+
             |    FilterNode    |
             +--------+---------+
                      |
             +--------+---------+
             |     TagNode      |
             +------------------+
*/
StoragePlan<VertexID> UpdateVertexProcessor::buildPlan(nebula::DataSet* result) {
    StoragePlan<VertexID> plan;
    // handle tag props, return prop, filter prop, update prop
    auto tagUpdate = std::make_unique<TagNode>(context_.get(),
                                               &tagContext_,
                                               tagContext_.propContexts_[0].first,
                                               &(tagContext_.propContexts_[0].second));

    auto filterNode = std::make_unique<FilterNode<VertexID>>(context_.get(),
                                                             tagUpdate.get(),
                                                             expCtx_.get(),
                                                             filterExp_);
    filterNode->addDependency(tagUpdate.get());

    auto updateNode = std::make_unique<UpdateTagNode>(context_.get(),
                                                      indexes_,
                                                      updatedProps_,
                                                      filterNode.get(),
                                                      insertable_,
                                                      depPropMap_,
                                                      expCtx_.get(),
                                                      &tagContext_);
    updateNode->addDependency(filterNode.get());

    auto resultNode = std::make_unique<UpdateResNode<VertexID>>(context_.get(),
                                                                updateNode.get(),
                                                                getReturnPropsExp(),
                                                                expCtx_.get(),
                                                                result);
    resultNode->addDependency(updateNode.get());
    plan.addNode(std::move(tagUpdate));
    plan.addNode(std::move(filterNode));
    plan.addNode(std::move(updateNode));
    plan.addNode(std::move(resultNode));
    return plan;
}

// Get all tag schema in spaceID
nebula::cpp2::ErrorCode UpdateVertexProcessor::buildTagSchema() {
    auto tags = env_->schemaMan_->getAllVerTagSchema(spaceId_);
    if (!tags.ok()) {
        return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    tagContext_.schemas_ = std::move(tags).value();
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

// tagContext_.propContexts_ has return prop, filter prop, update prop
// returnPropsExp_ has return expression
// filterExp_      has filter expression
// updatedVertexProps_  has update expression
nebula::cpp2::ErrorCode
UpdateVertexProcessor::buildTagContext(const cpp2::UpdateVertexRequest& req) {
    // Build context of the update vertex tag props
    auto vId = req.get_vertex_id();
    auto tagNameRet = env_->schemaMan_->toTagName(spaceId_, tagId_);
    if (!tagNameRet.ok()) {
        VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId_;
        return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
    }
    auto tagName = tagNameRet.value();

    auto pool = context_->objPool();
    for (auto& prop : updatedProps_) {
        auto sourcePropExp = SourcePropertyExpression::make(pool, tagName, prop.get_name());
        auto retCode = checkExp(sourcePropExp, false, false);
        if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
            VLOG(1) << "Invalid update vertex expression!";
            return retCode;
        }

        auto updateExp = Expression::decode(pool, prop.get_value());
        if (!updateExp) {
            VLOG(1) << "Can't decode the prop's value " << prop.get_value();
            return nebula::cpp2::ErrorCode::E_INVALID_UPDATER;
        }

        valueProps_.clear();
        retCode = checkExp(updateExp, false, false, insertable_);
        if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return retCode;
        }
        if (insertable_) {
            depPropMap_.emplace_back(std::make_pair(prop.get_name(), valueProps_));
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

    // update vertex only handle one tagId
    // maybe no updated prop, filter prop, return prop
    auto iter = tagContext_.tagNames_.find(tagId_);
    if (tagContext_.tagNames_.size() != 1 ||
        iter == tagContext_.tagNames_.end()) {
        VLOG(1) << "should only contain one tag in update vertex!";
        return nebula::cpp2::ErrorCode::E_MUTATE_TAG_CONFLICT;
    }

    context_->tagId_ = tagId_;
    context_->tagName_ = iter->second;
    auto schemaMap = tagContext_.schemas_;
    auto iterSchema = schemaMap.find(tagId_);
    if (iterSchema != schemaMap.end()) {
        auto schemas = iterSchema->second;
        auto schema = schemas.back().get();
        if (!schema) {
            VLOG(1) << "Fail to get schema in TagId " << tagId_;
            return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        context_->tagSchema_ = schema;
    } else {
        VLOG(1) << "Fail to get schema in TagId " << tagId_;
        return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
    }

    if (expCtx_ == nullptr) {
        expCtx_ = std::make_unique<StorageExpressionContext>(spaceVidLen_,
                                                             isIntId_,
                                                             context_->tagName_,
                                                             context_->tagSchema_,
                                                             false);
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void UpdateVertexProcessor::onProcessFinished() {
    resp_.set_props(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
