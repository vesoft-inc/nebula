/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/AlterTagProcessor.h"
#include "meta/processors/schemaMan/SchemaUtil.h"

namespace nebula {
namespace meta {

void AlterTagProcessor::process(const cpp2::AlterTagReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    GraphSpaceID spaceId = req.get_space_id();
    folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getTagId(spaceId, req.get_tag_name());
    if (!ret.ok()) {
        handleErrorCode(MetaCommon::to(ret.status()));
        onFinished();
        return;
    }
    auto tagId = ret.value();

    // Check the tag belongs to the space
    std::unique_ptr<kvstore::KVIterator> iter;
    auto tagPrefix = MetaServiceUtils::schemaTagPrefix(spaceId, tagId);
    auto code = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, tagPrefix, &iter);
    if (code != kvstore::ResultCode::SUCCEEDED || !iter->valid()) {
        LOG(ERROR) << "Tag could not be found " << req.get_tag_name()
                   << ", spaceId " << spaceId
                   << ", tagId " << tagId;
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    // Get the last version of the tag
    auto version = MetaServiceUtils::parseTagVersion(iter->key()) + 1;
    auto schema = MetaServiceUtils::parseSchema(iter->val());
    auto columns = schema.get_columns();
    auto prop = schema.get_schema_prop();

    // Update schema column
    auto& tagItems = req.get_tag_items();

    auto iCode = getIndexes(spaceId, tagId);
    if (!iCode.ok()) {
        handleErrorCode(MetaCommon::to(iCode.status()));
        onFinished();
        return;
    }
    auto indexes = std::move(iCode).value();
    auto existIndex = !indexes.empty();
    if (existIndex) {
        auto iStatus = indexCheck(indexes, tagItems);
        if (iStatus != cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Alter tag error, index conflict : " << static_cast<int32_t>(iStatus);
            handleErrorCode(iStatus);
            onFinished();
            return;
        }
    }

    for (auto& tagItem : tagItems) {
        auto &cols = tagItem.get_schema().get_columns();
        for (auto& col : cols) {
            auto retCode = MetaServiceUtils::alterColumnDefs(columns, prop, col, tagItem.op);
            if (retCode != cpp2::ErrorCode::SUCCEEDED) {
                LOG(ERROR) << "Alter tag column error " << static_cast<int32_t>(retCode);
                handleErrorCode(retCode);
                onFinished();
                return;
            }
        }
    }

    if (!SchemaUtil::checkType(columns)) {
        handleErrorCode(cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    // Update schema property if tag not index
    auto& alterSchemaProp = req.get_schema_prop();
    auto retCode = MetaServiceUtils::alterSchemaProp(columns, prop, alterSchemaProp, existIndex);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Alter tag property error " << static_cast<int32_t>(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    if (!existIndex) {
        schema.set_schema_prop(std::move(prop));
    }
    schema.set_columns(std::move(columns));

    std::vector<kvstore::KV> data;
    LOG(INFO) << "Alter Tag " << req.get_tag_name() << ", tagId " << tagId;
    data.emplace_back(MetaServiceUtils::schemaTagKey(spaceId, tagId, version),
                      MetaServiceUtils::schemaVal(req.get_tag_name(), schema));
    resp_.set_id(to(tagId, EntryType::TAG));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula

