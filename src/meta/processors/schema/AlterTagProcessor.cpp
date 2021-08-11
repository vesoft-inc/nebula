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
    GraphSpaceID spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);
    auto tagName = req.get_tag_name();

    folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getTagId(spaceId, tagName);
    if (!nebula::ok(ret)) {
        auto retCode = nebula::error(ret);
        LOG(ERROR) << "Failed to get tag " << tagName << " error "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto tagId = nebula::value(ret);

    // Check the tag belongs to the space
    auto tagPrefix = MetaServiceUtils::schemaTagPrefix(spaceId, tagId);
    auto retPre = doPrefix(tagPrefix);
    if (!nebula::ok(retPre)) {
        auto retCode = nebula::error(retPre);
        LOG(ERROR) << "Tag Prefix failed, tagname: " << tagName
                   << ", spaceId " << spaceId << " error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto iter = nebula::value(retPre).get();
    if (!iter->valid()) {
        LOG(ERROR) << "Tag could not be found, spaceId " << spaceId
                   << ", tagname: " << tagName;
        handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
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
    if (!nebula::ok(iCode)) {
        handleErrorCode(nebula::error(iCode));
        onFinished();
        return;
    }

    auto indexes = std::move(nebula::value(iCode));
    auto existIndex = !indexes.empty();
    if (existIndex) {
        auto iStatus = indexCheck(indexes, tagItems);
        if (iStatus != nebula::cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Alter tag error, index conflict : "
                       << apache::thrift::util::enumNameSafe(iStatus);
            handleErrorCode(iStatus);
            onFinished();
            return;
        }
    }

    auto& alterSchemaProp = req.get_schema_prop();

    if (existIndex) {
        int64_t duration = 0;
        if (alterSchemaProp.get_ttl_duration()) {
            duration = *alterSchemaProp.get_ttl_duration();
        }
        std::string col;
        if (alterSchemaProp.get_ttl_col()) {
            col = *alterSchemaProp.get_ttl_col();
        }
        if (!col.empty() && duration > 0) {
            LOG(ERROR) << "Alter tag error, index and ttl conflict";
            handleErrorCode(nebula::cpp2::ErrorCode::E_UNSUPPORTED);
            onFinished();
            return;
        }
    }

    // check fulltext index
    auto ftIdxRet = getFTIndex(spaceId, tagId);
    if (nebula::ok(ftIdxRet)) {
        auto fti = std::move(nebula::value(ftIdxRet));
        auto ftStatus = ftIndexCheck(fti.get_fields(), tagItems);
        if (ftStatus != nebula::cpp2::ErrorCode::SUCCEEDED) {
            handleErrorCode(ftStatus);
            onFinished();
            return;
        }
    } else if (nebula::error(ftIdxRet) != nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND) {
        handleErrorCode(nebula::error(ftIdxRet));
        onFinished();
        return;
    }

    for (auto& tagItem : tagItems) {
        auto &cols = tagItem.get_schema().get_columns();
        for (auto& col : cols) {
            auto retCode = MetaServiceUtils::alterColumnDefs(columns, prop, col, *tagItem.op_ref());
            if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
                LOG(ERROR) << "Alter tag column error "
                           << apache::thrift::util::enumNameSafe(retCode);
                handleErrorCode(retCode);
                onFinished();
                return;
            }
        }
    }

    if (!SchemaUtil::checkType(columns)) {
        handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    // Update schema property if tag not index
    auto retCode = MetaServiceUtils::alterSchemaProp(columns, prop, alterSchemaProp, existIndex);
    if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Alter tag property error "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    schema.set_schema_prop(std::move(prop));
    schema.set_columns(std::move(columns));

    std::vector<kvstore::KV> data;
    LOG(INFO) << "Alter Tag " << tagName << ", tagId " << tagId;
    data.emplace_back(MetaServiceUtils::schemaTagKey(spaceId, tagId, version),
                      MetaServiceUtils::schemaVal(tagName, schema));
    resp_.set_id(to(tagId, EntryType::TAG));
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula

