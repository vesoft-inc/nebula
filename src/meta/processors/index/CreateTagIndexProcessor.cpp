/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/CommonMacro.h"
#include "meta/processors/indexMan/CreateTagIndexProcessor.h"

namespace nebula {
namespace meta {

void CreateTagIndexProcessor::process(const cpp2::CreateTagIndexReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);
    const auto &indexName = req.get_index_name();
    auto &tagName = req.get_tag_name();
    const auto &fields = req.get_fields();

    std::set<std::string> columnSet;
    for (const auto& field : fields) {
        columnSet.emplace(field.get_name());
    }
    if (fields.size() != columnSet.size()) {
        LOG(ERROR) << "Conflict field in the tag index.";
        handleErrorCode(nebula::cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    // A maximum of 16 columns are allowed in the index.
    if (columnSet.size() > maxIndexLimit) {
        LOG(ERROR) << "The number of index columns exceeds maximum limit "
                   << maxIndexLimit;
        handleErrorCode(nebula::cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagIndexLock());
    auto ret = getIndexID(space, indexName);
    if (nebula::ok(ret)) {
        if (req.get_if_not_exists()) {
            handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
        } else {
            LOG(ERROR) << "Create Tag Index Failed: " << indexName << " has existed";
            handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
        }
        resp_.set_id(to(nebula::value(ret), EntryType::INDEX));
        onFinished();
        return;
    } else {
        auto retCode = nebula::error(ret);
        if (retCode != nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND) {
            LOG(ERROR) << "Create Tag Index Failed, index name " << indexName << " error: "
                       << apache::thrift::util::enumNameSafe(retCode);
            handleErrorCode(retCode);
            onFinished();
            return;
        }
    }

    auto tagIDRet = getTagId(space, tagName);
    if (!nebula::ok(tagIDRet)) {
        auto retCode = nebula::error(tagIDRet);
        LOG(ERROR) << "Create Tag Index Failed, Tag " << tagName << " error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto tagID = nebula::value(tagIDRet);

    const auto& prefix = MetaServiceUtils::indexPrefix(space);
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "Tag indexes prefix failed, space id " << space
                   << " error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto checkIter = nebula::value(iterRet).get();

    while (checkIter->valid()) {
        auto val = checkIter->val();
        auto item = MetaServiceUtils::parseIndex(val);
        if (item.get_schema_id().getType() != cpp2::SchemaID::Type::tag_id ||
            fields.size() > item.get_fields().size() ||
            tagID != item.get_schema_id().get_tag_id()) {
            checkIter->next();
            continue;
        }

        if (checkIndexExist(fields, item)) {
            resp_.set_code(nebula::cpp2::ErrorCode::E_EXISTED);
            onFinished();
            return;
        }
        checkIter->next();
    }

    auto schemaRet = getLatestTagSchema(space, tagID);
    if (!nebula::ok(schemaRet)) {
        auto retCode = nebula::error(schemaRet);
         LOG(ERROR) << "Get tag schema failed, space id " << space << " tagName " << tagName
                   << " error: " << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto latestTagSchema = std::move(nebula::value(schemaRet));
    const auto& schemaCols = latestTagSchema.get_columns();
    std::vector<cpp2::ColumnDef> columns;
    for (auto &field : fields) {
        auto iter = std::find_if(schemaCols.begin(), schemaCols.end(),
                                 [field](const auto& col) {
                                     return field.get_name() == col.get_name();
                                 });
        if (iter == schemaCols.end()) {
            LOG(ERROR) << "Field " << field.get_name() << " not found in Tag " << tagName;
            handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
            onFinished();
            return;
        }
        cpp2::ColumnDef col = *iter;
        if (col.type.get_type() == meta::cpp2::PropertyType::FIXED_STRING) {
            if (*col.type.get_type_length() > MAX_INDEX_TYPE_LENGTH) {
                LOG(ERROR) << "Unsupport index type lengths greater than "
                           << MAX_INDEX_TYPE_LENGTH << " : "
                           << field.get_name();
                handleErrorCode(nebula::cpp2::ErrorCode::E_UNSUPPORTED);
                onFinished();
                return;
            }
        } else if (col.type.get_type() == meta::cpp2::PropertyType::STRING) {
            if (!field.type_length_ref().has_value()) {
                LOG(ERROR) << "No type length set : " << field.get_name();
                handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
                onFinished();
                return;
            }
            if (*field.get_type_length() > MAX_INDEX_TYPE_LENGTH) {
                LOG(ERROR) << "Unsupport index type lengths greater than "
                           << MAX_INDEX_TYPE_LENGTH << " : "
                           << field.get_name();
                handleErrorCode(nebula::cpp2::ErrorCode::E_UNSUPPORTED);
                onFinished();
                return;
            }
            col.type.set_type(meta::cpp2::PropertyType::FIXED_STRING);
            col.type.set_type_length(*field.get_type_length());
        } else if (field.type_length_ref().has_value()) {
            LOG(ERROR) << "No need to set type length : " << field.get_name();
            handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
            onFinished();
            return;
        }
        columns.emplace_back(col);
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
    if (req.comment_ref().has_value()) {
        item.set_comment(*req.comment_ref());
    }

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

