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
    const auto &properties = req.get_properties();
    if (properties.get_fields().empty()) {
        LOG(ERROR) << "Edge's Field should not empty";
        resp_.set_code(cpp2::ErrorCode::E_INVALID_PARM);
        onFinished();
        return;
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeIndexLock());
    auto ret = getEdgeIndexID(space, indexName);
    if (ret.ok()) {
        LOG(ERROR) << "Create Edge Index Failed: " << indexName << " have existed";
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    std::map<std::string, std::vector<nebula::cpp2::ColumnDef>> edgeColumns;
    for (auto const &element : properties.get_fields()) {
        auto edgeName = element.first;
        auto edgeType = getEdgeType(space, edgeName);
        if (!edgeType.ok()) {
            LOG(ERROR) << "Create Edge Index Failed: " << edgeName << " not exist";
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }

        auto fieldsResult = getLatestEdgeFields(space, edgeName);
        if (!fieldsResult.ok()) {
            LOG(ERROR) << "Get Latest Edge Property Name Failed";
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        auto fields = fieldsResult.value();
        std::vector<nebula::cpp2::ColumnDef> columns;
        for (auto &field : element.second) {
            auto iter = std::find_if(std::begin(fields), std::end(fields),
                                     [field](const auto& pair) {
                                         return field == pair.first;
                                     });

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
        edgeColumns.emplace(edgeName, std::move(columns));
    }
    nebula::meta::cpp2::IndexFields indexFields;
    indexFields.set_fields(std::move(edgeColumns));

    std::vector<kvstore::KV> data;
    auto edgeIndexRet = autoIncrementId();
    if (!nebula::ok(edgeIndexRet)) {
        LOG(ERROR) << "Create edge index failed: Get edge index ID failed";
        resp_.set_code(nebula::error(edgeIndexRet));
        onFinished();
        return;
    }

    auto edgeIndex = nebula::value(edgeIndexRet);
    data.emplace_back(MetaServiceUtils::indexEdgeIndexKey(space, indexName),
                      std::string(reinterpret_cast<const char*>(&edgeIndex), sizeof(EdgeIndexID)));
    data.emplace_back(MetaServiceUtils::edgeIndexKey(space, edgeIndex),
                      MetaServiceUtils::edgeIndexVal(indexName, indexFields));
    LOG(INFO) << "Create Edge Index " << indexName << ", edgeIndex " << edgeIndex;
    resp_.set_id(to(edgeIndex, EntryType::EDGE_INDEX));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

