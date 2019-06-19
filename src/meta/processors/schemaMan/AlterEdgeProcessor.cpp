/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/AlterEdgeProcessor.h"
#include "time/TimeUtils.h"

namespace nebula {
namespace meta {

void AlterEdgeProcessor::process(const cpp2::AlterEdgeReq& req) {
    auto spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);
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
        LOG(WARNING) << "Edge could not be found " << req.get_edge_name()
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
    auto& edgeItems = req.get_edge_items();
    for (auto& edgeItem : edgeItems) {
        auto& cols = edgeItem.get_schema().get_columns();
        for (auto& col : cols) {
            auto retCode = MetaServiceUtils::alterColumnDefs(columns, col, edgeItem.op);
            if (retCode != cpp2::ErrorCode::SUCCEEDED) {
                LOG(WARNING) << "Alter edge error " << static_cast<int32_t>(retCode);
                resp_.set_code(retCode);
                onFinished();
                return;
            }
        }
    }
    schema.set_columns(std::move(columns));
    std::vector<kvstore::KV> data;
    LOG(INFO) << "Alter edge " << req.get_edge_name() << ", edgeType " << edgeType;
    data.emplace_back(MetaServiceUtils::schemaEdgeKey(spaceId, edgeType, version),
                      MetaServiceUtils::schemaEdgeVal(req.get_edge_name(), schema));
    data.emplace_back(MetaServiceUtils::schemaLatestEdgeKey(spaceId, edgeType),
                      MetaServiceUtils::schemaEdgeVal(req.get_edge_name(), schema));
    resp_.set_id(to(edgeType, EntryType::EDGE));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

