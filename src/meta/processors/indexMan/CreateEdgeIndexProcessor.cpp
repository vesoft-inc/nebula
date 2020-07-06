/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/CreateEdgeIndexProcessor.h"

namespace nebula {
namespace meta {

void CreateEdgeIndexProcessor::process(const cpp2::CreateEdgeIndexReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);
    const auto &indexName = req.get_index_name();
    auto &edgeName = req.get_edge_name();
    auto &fieldNames = req.get_fields();
    if (fieldNames.empty()) {
        LOG(ERROR) << "The index field of an edge type should not be empty.";
        handleErrorCode(cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    std::set<std::string> columnSet(fieldNames.begin(), fieldNames.end());
    if (fieldNames.size() != columnSet.size()) {
        LOG(ERROR) << "Conflict field in the edge index.";
        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    // A maximum of 16 columns are allowed in the index
    if (columnSet.size() > 16) {
        LOG(ERROR) << "The number of index columns exceeds maximum limit 16";
        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
        onFinished();
        return;
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeIndexLock());
    auto ret = getIndexID(space, indexName);
    if (ret.ok()) {
        LOG(ERROR) << "Create Edge Index Failed: " << indexName << " have existed";
        if (req.get_if_not_exists()) {
            handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        } else {
            handleErrorCode(cpp2::ErrorCode::E_EXISTED);
        }
        onFinished();
        return;
    }

    auto edgeTypeRet = getEdgeType(space, edgeName);
    if (!edgeTypeRet.ok()) {
        LOG(ERROR) << "Create Edge Index Failed: " << edgeName << " not exist";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto edgeType = edgeTypeRet.value();
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
        if (item.get_schema_id().getType() != cpp2::SchemaID::Type::edge_type ||
            fieldNames.size() > item.get_fields().size() ||
            edgeType != item.get_schema_id().get_edge_type()) {
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

    auto schemaRet = getLatestEdgeSchema(space, edgeType);
    if (!schemaRet.ok()) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto latestEdgeSchema = schemaRet.value();
    if (tagOrEdgeHasTTL(latestEdgeSchema)) {
       LOG(ERROR) << "Edge: " << edgeName  << " has ttl, not create index";
       handleErrorCode(cpp2::ErrorCode::E_INDEX_WITH_TTL);
       onFinished();
       return;
    }

    const auto& schemaCols = latestEdgeSchema.get_columns();
    std::vector<cpp2::ColumnDef> columns;
    for (auto &field : fieldNames) {
        auto iter = std::find_if(schemaCols.begin(), schemaCols.end(),
                                 [field](const auto& col) { return field == col.get_name(); });

        if (iter == schemaCols.end()) {
            LOG(ERROR) << "Field " << field << " not found in Edge " << edgeName;
            handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        } else {
            columns.emplace_back(*iter);
        }
    }

    std::vector<kvstore::KV> data;
    auto edgeIndexRet = autoIncrementId();
    if (!nebula::ok(edgeIndexRet)) {
        LOG(ERROR) << "Create edge index failed: Get edge index ID failed";
        handleErrorCode(nebula::error(edgeIndexRet));
        onFinished();
        return;
    }

    auto edgeIndex = nebula::value(edgeIndexRet);
    cpp2::IndexItem item;
    item.set_index_id(edgeIndex);
    item.set_index_name(indexName);
    cpp2::SchemaID schemaID;
    schemaID.set_edge_type(edgeType);
    item.set_schema_id(schemaID);
    item.set_schema_name(edgeName);
    item.set_fields(std::move(columns));

    data.emplace_back(MetaServiceUtils::indexIndexKey(space, indexName),
                      std::string(reinterpret_cast<const char*>(&edgeIndex), sizeof(IndexID)));
    data.emplace_back(MetaServiceUtils::indexKey(space, edgeIndex),
                      MetaServiceUtils::indexVal(item));
    LOG(INFO) << "Create Edge Index " << indexName << ", edgeIndex " << edgeIndex;
    resp_.set_id(to(edgeIndex, EntryType::INDEX));
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula

