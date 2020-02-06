/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/GetEdgeProcessor.h"

namespace nebula {
namespace meta {

void GetEdgeProcessor::process(const cpp2::GetEdgeReq& req) {
    auto spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
    auto edgeTypeRet = getEdgeType(spaceId, req.get_edge_name());
    if (!edgeTypeRet.ok()) {
        resp_.set_code(to(edgeTypeRet.status()));
        onFinished();
        return;
    }
    auto edgeType = edgeTypeRet.value();

    std::string schemaValue;
    // Get the lastest version
    if (req.get_version() < 0) {
        auto edgePrefix = MetaServiceUtils::schemaEdgePrefix(spaceId, edgeType);
        auto ret = doPrefix(edgePrefix);
        if (!ret.ok()) {
            LOG(ERROR) << "Get Edge SpaceID: " << spaceId << ", edgeName: "
                       << req.get_edge_name() << ", version " << req.get_version() << " not found";
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        schemaValue = ret.value()->val().str();
    } else {
        auto edgeKey = MetaServiceUtils::schemaEdgeKey(spaceId,
                                                       edgeType,
                                                       req.get_version());
        auto ret = doGet(edgeKey);
        if (!ret.ok()) {
            LOG(ERROR) << "Get Edge SpaceID: " << spaceId << ", edgeName: "
                       << req.get_edge_name() << ", version " << req.get_version() << " not found";
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        schemaValue = ret.value();
    }

    VLOG(3) << "Get Edge SpaceID: " << spaceId << ", edgeName: "
            << req.get_edge_name() << ", version " << req.get_version();
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    auto schema = MetaServiceUtils::parseSchema(schemaValue);
    auto indexKey = getEdgeIndexKey(spaceId, edgeType);
    for (const auto &key : indexKey) {
        if (!key.get_fields().empty()) {
            const auto &colKey = key.get_fields()[0];
            for (std::size_t i = 0; i < schema.get_columns().size(); ++i) {
                if (colKey.get_name() == schema.get_columns()[i].get_name()) {
                    if (key.get_key_type() != nullptr) {
                        if (schema.columns[i].get_key_type() == nullptr) {
                            schema.columns[i].set_key_type(*key.get_key_type());
                        } else {
                            if (*schema.columns[i].get_key_type() > *key.get_key_type()) {
                                schema.columns[i].set_key_type(*key.get_key_type());
                            }
                        }
                    }
                    break;
                }
            }
        }
    }
    resp_.set_schema(schema);
    onFinished();
}

// Consider add lookup table if too many data
std::vector<::nebula::cpp2::IndexItem> GetEdgeProcessor::getEdgeIndexKey(GraphSpaceID spaceId,
    EdgeType edgeType) {
    std::vector<::nebula::cpp2::IndexItem> items;
    auto prefix = MetaServiceUtils::indexPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Edge Index Failed: SpaceID " << spaceId;
        return items;
    }

    while (iter->valid()) {
        auto val = iter->val();
        auto item = MetaServiceUtils::parseIndex(val);
        if (item.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::edge_type
            && item.get_schema_id().get_edge_type() == edgeType) {
            items.emplace_back(std::move(item));
        }
        iter->next();
    }
    return items;
}

}  // namespace meta
}  // namespace nebula

