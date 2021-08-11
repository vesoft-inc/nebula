/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/CommonMacro.h"
#include "meta/processors/indexMan/FTIndexProcessor.h"

namespace nebula {
namespace meta {

void CreateFTIndexProcessor::process(const cpp2::CreateFTIndexReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::fulltextIndexLock());
    const auto& index = req.get_index();
    const std::string& name = req.get_fulltext_index_name();
    CHECK_SPACE_ID_AND_RETURN(index.get_space_id());
    auto isEdge = index.get_depend_schema().getType() == cpp2::SchemaID::Type::edge_type;
    folly::SharedMutex::ReadHolder rHolder(isEdge ? LockUtils::edgeLock() : LockUtils::tagLock());
    auto schemaPrefix = isEdge
                      ? MetaServiceUtils::schemaEdgePrefix(
                          index.get_space_id(), index.get_depend_schema().get_edge_type())
                      : MetaServiceUtils::schemaTagPrefix(
                          index.get_space_id(), index.get_depend_schema().get_tag_id());

    // Check tag or edge exist.
    auto retPre = doPrefix(schemaPrefix);
    if (!nebula::ok(retPre)) {
        auto retCode = nebula::error(retPre);
        LOG(ERROR) << "Tag or Edge Prefix failed, "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto iter = nebula::value(retPre).get();
    if (!iter->valid()) {
        LOG(ERROR) << "Edge or Tag could not be found";
        auto eCode = isEdge
                  ? nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND
                  : nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
        handleErrorCode(eCode);
        onFinished();
        return;
    }

    // verify the columns.
    auto schema = MetaServiceUtils::parseSchema(iter->val());
    auto columns = schema.get_columns();
    for (const auto& col : index.get_fields()) {
        auto targetCol = std::find_if(columns.begin(), columns.end(), [col] (const auto& c) {
            return col == c.get_name();
        });
        if (targetCol == columns.end()) {
            LOG(ERROR) << "Column not found : " << col;
            auto eCode = isEdge
                       ? nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND
                       : nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
            handleErrorCode(eCode);
            onFinished();
            return;
        }
        // Only string or fixed_string types are supported.
        if (targetCol->get_type().get_type() != meta::cpp2::PropertyType::STRING &&
            targetCol->get_type().get_type() != meta::cpp2::PropertyType::FIXED_STRING) {
            LOG(ERROR) << "Column data type error : "
                       << apache::thrift::util::enumNameSafe(targetCol->get_type().get_type());
            handleErrorCode(nebula::cpp2::ErrorCode::E_UNSUPPORTED);
            onFinished();
            return;
        }
        // if the data type is fixed_string,
        // the data length must be less than MAX_INDEX_TYPE_LENGTH.
        // else if the data type is string,
        // will be truncated to MAX_INDEX_TYPE_LENGTH bytes when data insert.
        if (targetCol->get_type().get_type() == meta::cpp2::PropertyType::FIXED_STRING &&
            *targetCol->get_type().get_type_length() > MAX_INDEX_TYPE_LENGTH) {
            LOG(ERROR) << "Unsupported data length more than "
                       << MAX_INDEX_TYPE_LENGTH << " bytes : "
                       << col << "(" << *targetCol->get_type().get_type_length() << ")";
            handleErrorCode(nebula::cpp2::ErrorCode::E_UNSUPPORTED);
            onFinished();
            return;
        }
    }

    // Check fulltext index exist.
    auto ftPrefix = MetaServiceUtils::fulltextIndexPrefix();
    auto ret = doPrefix(ftPrefix);
    if (!nebula::ok(ret)) {
        auto retCode = nebula::error(ret);
        LOG(ERROR) << "Fulltext Prefix failed, "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto it = nebula::value(ret).get();
    // If already have a full-text index that depend on this tag or edge,  reject to create it.
    // even though it's a different column.
    while (it->valid()) {
        auto indexName = MetaServiceUtils::parsefulltextIndexName(it->key());
        auto indexItem = MetaServiceUtils::parsefulltextIndex(it->val());
        if (indexName == name) {
            LOG(ERROR) << "Fulltext index exist : " << indexName;
            handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
            onFinished();
            return;
        }
        if (index.get_depend_schema() == indexItem.get_depend_schema()) {
            LOG(ERROR) << "Depends on the same schema , index : " << indexName;
            handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
            onFinished();
            return;
        }
        it->next();
    }
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::fulltextIndexKey(name),
                      MetaServiceUtils::fulltextIndexVal(index));
    doSyncPutAndUpdate(std::move(data));
}

void DropFTIndexProcessor::process(const cpp2::DropFTIndexReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::fulltextIndexLock());
    auto indexKey = MetaServiceUtils::fulltextIndexKey(req.get_fulltext_index_name());
    auto ret = doGet(indexKey);
    if (!nebula::ok(ret)) {
        auto retCode = nebula::error(ret);
        if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
            LOG(ERROR) << "Drop fulltext index failed, Fulltext index not exists.";
            handleErrorCode(nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND);
            onFinished();
            return;
        }
        LOG(ERROR) << "Drop fulltext index failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    doSyncMultiRemoveAndUpdate({std::move(indexKey)});
}

void ListFTIndexesProcessor::process(const cpp2::ListFTIndexesReq&) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::fulltextIndexLock());
    const auto& prefix = MetaServiceUtils::fulltextIndexPrefix();
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "List fulltext indexes failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto iter = nebula::value(iterRet).get();
    std::unordered_map<std::string, nebula::meta::cpp2::FTIndex> indexes;
    while (iter->valid()) {
        auto name = MetaServiceUtils::parsefulltextIndexName(iter->key());
        auto index = MetaServiceUtils::parsefulltextIndex(iter->val());
        indexes.emplace(std::move(name), std::move(index));
        iter->next();
    }
    resp_.set_indexes(std::move(indexes));
    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
