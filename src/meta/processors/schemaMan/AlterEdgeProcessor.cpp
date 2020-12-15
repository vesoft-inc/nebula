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
    folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto ret = getEdgeType(spaceId, req.get_edge_name());
    if (!ret.ok()) {
        handleErrorCode(MetaCommon::to(ret.status()));
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
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
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

    auto iRet = getIndexes(spaceId, edgeType);
    if (!iRet.ok()) {
        handleErrorCode(MetaCommon::to(iRet.status()));
        onFinished();
        return;
    }
    auto indexes = std::move(iRet).value();
    auto existIndex = !indexes.empty();
    if (existIndex) {
        auto iStatus = indexCheck(indexes, edgeItems);
        if (iStatus != cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Alter edge error, index conflict : " << static_cast<int32_t>(iStatus);
            handleErrorCode(iStatus);
            onFinished();
            return;
        }
    }

    for (auto& edgeItem : edgeItems) {
        auto& cols = edgeItem.get_schema().get_columns();
        for (auto& col : cols) {
            auto retCode = MetaServiceUtils::alterColumnDefs(columns, prop, col, edgeItem.op);
            if (retCode != cpp2::ErrorCode::SUCCEEDED) {
                LOG(ERROR) << "Alter edge column error " << static_cast<int32_t>(retCode);
                handleErrorCode(retCode);
                onFinished();
                return;
            }
        }
    }

    // Update schema property if edge not index
    auto& alterSchemaProp = req.get_schema_prop();
    auto retCode = MetaServiceUtils::alterSchemaProp(columns, prop, alterSchemaProp, existIndex);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Alter edge property error " << static_cast<int32_t>(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    if (!existIndex) {
        schema.set_schema_prop(std::move(prop));
    }
    schema.set_columns(std::move(columns));

    std::vector<kvstore::KV> data;
    LOG(INFO) << "Alter edge " << req.get_edge_name() << ", edgeType " << edgeType;
    data.emplace_back(MetaServiceUtils::schemaEdgeKey(spaceId, edgeType, version),
                      MetaServiceUtils::schemaVal(req.get_edge_name(), schema));
    resp_.set_id(to(edgeType, EntryType::EDGE));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula

