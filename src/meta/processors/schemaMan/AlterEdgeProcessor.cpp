/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/AlterEdgeProcessor.h"

namespace nebula {
namespace meta {

void AlterEdgeProcessor::process(const cpp2::AlterEdgeReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    GraphSpaceID spaceId = req.get_space_id();
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto ret = getEdgeType(spaceId, req.get_edge_name());
    if (!ret.ok()) {
        resp_.set_code(to(ret.status()));
        onFinished();
        return;
    }
    auto edgeType = ret.value();

    // Check the edge belongs to the space
    std::unique_ptr<kvstore::KVIterator> iter;
    auto edgePrefix = MetaServiceUtils::schemaEdgePrefix(spaceId, edgeType);
    auto code = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, edgePrefix, &iter);
    if (code != kvstore::ResultCode::SUCCEEDED || !iter->valid()) {
        LOG(ERROR) << "Edge could not be found " << req.get_edge_name()
                   << ", spaceId " << spaceId
                   << ", edgeType " << edgeType;
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    // Get lasted version of edge
    auto version = MetaServiceUtils::parseEdgeVersion(iter->key()) + 1;
    auto schema = MetaServiceUtils::parseSchema(iter->val());
    auto columns = schema.get_columns();
    auto prop = schema.get_schema_prop();

    // Update schema column
    auto& edgeItems = req.get_edge_items();

    // Check the edge belongs to the index
    {
        std::unique_ptr<kvstore::KVIterator> indexIter;
        auto indexPrefix = MetaServiceUtils::indexPrefix(spaceId);
        auto iRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, indexPrefix, &indexIter);
        if (iRet != kvstore::ResultCode::SUCCEEDED) {
            resp_.set_code(to(iRet));
            onFinished();
            return;
        }
        std::vector<nebula::cpp2::IndexItem> items;
        while (indexIter->valid()) {
            auto item = MetaServiceUtils::parseIndex(indexIter->val());
            if (item.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::edge_type &&
                item.get_schema_id().get_edge_type() == edgeType) {
                items.emplace_back(std::move(item));
            }
            indexIter->next();
        }
        for (const auto& index : items) {
            for (const auto& edgeItem : edgeItems) {
                if (edgeItem.op == nebula::meta::cpp2::AlterSchemaOp::CHANGE ||
                    edgeItem.op == nebula::meta::cpp2::AlterSchemaOp::DROP) {
                    const auto& edgeCols = edgeItem.get_schema().get_columns();
                    const auto& indexCols = index.get_fields();
                    for (const auto& eCol : edgeCols) {
                        auto it = std::find_if(indexCols.begin(), indexCols.end(),
                                               [&] (const auto& iCol) {
                                                   return eCol.name == iCol.name;
                                               });
                        if (it != indexCols.end()) {
                            LOG(ERROR) << "Index conflict, index :" << index.get_index_name()
                                       << ", column : " << eCol.name;
                            resp_.set_code(cpp2::ErrorCode::E_INDEX_CONFLICT);
                            onFinished();
                            return;
                        }
                    }
                }
            }
        }
    }

    for (auto& edgeItem : edgeItems) {
        auto& cols = edgeItem.get_schema().get_columns();
        for (auto& col : cols) {
            auto retCode = MetaServiceUtils::alterColumnDefs(columns, prop, col, edgeItem.op);
            if (retCode != cpp2::ErrorCode::SUCCEEDED) {
                LOG(ERROR) << "Alter edge column error " << static_cast<int32_t>(retCode);
                resp_.set_code(retCode);
                onFinished();
                return;
            }
        }
    }

    // Update schema property
    auto& alterSchemaProp = req.get_schema_prop();
    auto retCode = MetaServiceUtils::alterSchemaProp(columns, prop, alterSchemaProp);

    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Alter edge property error " << static_cast<int32_t>(retCode);
        resp_.set_code(retCode);
        onFinished();
        return;
    }

    schema.set_columns(std::move(columns));
    schema.set_schema_prop(std::move(prop));

    std::vector<kvstore::KV> data;
    LOG(INFO) << "Alter edge " << req.get_edge_name() << ", edgeType " << edgeType;
    data.emplace_back(MetaServiceUtils::schemaEdgeKey(spaceId, edgeType, version),
                      MetaServiceUtils::schemaEdgeVal(req.get_edge_name(), schema));
    resp_.set_id(to(edgeType, EntryType::EDGE));
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula

