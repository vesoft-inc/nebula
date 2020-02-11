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
        LOG(ERROR) << "Edge's Field should not empty";
        resp_.set_code(cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeIndexLock());
    auto ret = getIndexID(space, indexName);
    if (ret.ok()) {
        LOG(ERROR) << "Create Edge Index Failed: " << indexName << " have existed";
        if (req.get_if_not_exists()) {
            resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
        } else {
            resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        }
        onFinished();
        return;
    }

    std::map<std::string, std::vector<nebula::cpp2::ColumnDef>> edgeColumns;
    auto edgeTypeRet = getEdgeType(space, edgeName);
    if (!edgeTypeRet.ok()) {
        LOG(ERROR) << "Create Edge Index Failed: " << edgeName << " not exist";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto edgeType = edgeTypeRet.value();
    auto fieldsResult = getLatestEdgeFields(space, edgeType);
    if (!fieldsResult.ok()) {
        LOG(ERROR) << "Get Latest Edge Property Name Failed";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    auto fields = fieldsResult.value();
    std::vector<nebula::cpp2::ColumnDef> columns;
    for (auto &field : fieldNames) {
        auto iter = std::find_if(std::begin(fields), std::end(fields),
                                 [field](const auto& pair) { return field == pair.first; });

        if (iter == fields.end()) {
            LOG(ERROR) << "Field " << field << " not found in Edge " << edgeName;
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        } else {
            auto type = fields[field];
            nebula::cpp2::ColumnDef column;
            column.set_name(std::move(field));
            column.set_type(std::move(type));
            columns.emplace_back(std::move(column));
        }
    }

    std::vector<kvstore::KV> data;
    auto edgeIndexRet = autoIncrementId();
    if (!nebula::ok(edgeIndexRet)) {
        LOG(ERROR) << "Create edge index failed: Get edge index ID failed";
        resp_.set_code(nebula::error(edgeIndexRet));
        onFinished();
        return;
    }

    auto edgeIndex = nebula::value(edgeIndexRet);
    nebula::cpp2::IndexItem item;
    item.set_index_id(edgeIndex);
    item.set_index_name(indexName);
    nebula::cpp2::SchemaID schemaID;
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

