/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/CreateTagIndexProcessor.h"

namespace nebula {
namespace meta {

void CreateTagIndexProcessor::process(const cpp2::CreateTagIndexReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);
    const auto &indexName = req.get_index_name();
    auto &tagName = req.get_tag_name();
    auto &fieldNames = req.get_fields();
    if (fieldNames.empty()) {
        LOG(ERROR) << "The index field of an tag should not be empty.";
        handleErrorCode(cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }
    std::set<std::string> columnSet(fieldNames.begin(), fieldNames.end());
    if (fieldNames.size() != columnSet.size()) {
        LOG(ERROR) << "Conflict field in the tag index.";
        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    // A maximum of 16 columns are allowed in the index.
    if (columnSet.size() > 16) {
        LOG(ERROR) << "The number of index columns exceeds maximum limit 16";
        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagIndexLock());
    auto ret = getIndexID(space, indexName);
    if (ret.ok()) {
        LOG(ERROR) << "Create Tag Index Failed: " << indexName << " have existed";
        if (req.get_if_not_exists()) {
            resp_.set_id(to(ret.value(), EntryType::INDEX));
            handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        } else {
            handleErrorCode(cpp2::ErrorCode::E_EXISTED);
        }
        onFinished();
        return;
    }

    auto tagIDRet = getTagId(space, tagName);
    if (!tagIDRet.ok()) {
        LOG(ERROR) << "Create Tag Index Failed: Tag " << tagName << " not exist";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto tagID = tagIDRet.value();
    auto prefix = MetaServiceUtils::indexPrefix(space);
    std::unique_ptr<kvstore::KVIterator> checkIter;
    auto checkRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &checkIter);
    if (checkRet != kvstore::ResultCode::SUCCEEDED) {
        resp_.set_code(MetaCommon::to(checkRet));
        onFinished();
        return;
    }

    while (checkIter->valid()) {
        auto val = checkIter->val();
        auto item = MetaServiceUtils::parseIndex(val);
        if (item.get_schema_id().getType() != cpp2::SchemaID::Type::tag_id ||
            fieldNames.size() > item.get_fields().size() ||
            tagID != item.get_schema_id().get_tag_id()) {
            checkIter->next();
            continue;
        }

        if (checkIndexExist(fieldNames, item)) {
            resp_.set_code(cpp2::ErrorCode::E_EXISTED);
            onFinished();
            return;
        }
        checkIter->next();
    }

    auto schemaRet = getLatestTagSchema(space, tagID);
    if (!schemaRet.ok()) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto latestTagSchema = schemaRet.value();
    if (tagOrEdgeHasTTL(latestTagSchema)) {
       LOG(ERROR) << "Tag: " << tagName << " has ttl, not create index";
       handleErrorCode(cpp2::ErrorCode::E_INDEX_WITH_TTL);
       onFinished();
       return;
    }

    const auto& schemaCols = latestTagSchema.get_columns();
    std::vector<cpp2::ColumnDef> columns;
    for (auto &field : fieldNames) {
        auto iter = std::find_if(schemaCols.begin(), schemaCols.end(),
                                 [field](const auto& col) { return field == col.get_name(); });
        if (iter == schemaCols.end()) {
            LOG(ERROR) << "Field " << field << " not found in Tag " << tagName;
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        } else {
            columns.emplace_back(*iter);
        }
    }

    std::vector<kvstore::KV> data;
    auto tagIndexRet = autoIncrementId();
    if (!nebula::ok(tagIndexRet)) {
        LOG(ERROR) << "Create tag index failed : Get tag index ID failed";
        handleErrorCode(nebula::error(tagIndexRet));
        onFinished();
        return;
    }

    auto tagIndex = nebula::value(tagIndexRet);
    cpp2::IndexItem item;
    item.set_index_id(tagIndex);
    item.set_index_name(indexName);
    cpp2::SchemaID schemaID;
    schemaID.set_tag_id(tagID);
    item.set_schema_id(schemaID);
    item.set_schema_name(tagName);
    item.set_fields(std::move(columns));

    data.emplace_back(MetaServiceUtils::indexIndexKey(space, indexName),
                      std::string(reinterpret_cast<const char*>(&tagIndex), sizeof(IndexID)));
    data.emplace_back(MetaServiceUtils::indexKey(space, tagIndex),
                      MetaServiceUtils::indexVal(item));
    LOG(INFO) << "Create Tag Index " << indexName << ", tagIndex " << tagIndex;
    resp_.set_id(to(tagIndex, EntryType::INDEX));
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula

