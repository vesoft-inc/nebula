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
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto ret = getEdgeType(req.get_space_id(), req.get_edge_name());
    if (!ret.ok()) {
        resp_.set_code(to(ret.status()));
        onFinished();
        return;
    }
    auto edgeType = ret.value();

    // Check the edge belongs to the space
    std::unique_ptr<kvstore::KVIterator> iter;
    auto edgePrefix = MetaServiceUtils::schemaEdgePrefix(req.get_space_id(), edgeType);
    auto code = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, edgePrefix, &iter);
    if (code != kvstore::ResultCode::SUCCEEDED || !iter->valid()) {
        LOG(WARNING) << "Edge could not be found " << req.get_edge_name()
                     << ", spaceId " << req.get_space_id()
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
    auto& schemaOptions = req.get_schema_options();
    for (auto& schemaOption : schemaOptions) {
        auto& cols = schemaOption.get_schema().get_columns();
        for (auto& col : cols) {
            auto retCode = MetaServiceUtils::alterColumnDefs(columns, prop, col, schemaOption.type);
            if (retCode != cpp2::ErrorCode::SUCCEEDED) {
                LOG(WARNING) << "Alter edge column error " << static_cast<int32_t>(retCode);
                resp_.set_code(retCode);
                onFinished();
                return;
            }
        }
    }

    // Update schema property
    auto& schemaProps = req.get_schema_props();
    for (auto& schemaProp : schemaProps) {
        auto retCode = MetaServiceUtils::alterSchemaProp(columns, prop, schemaProp);
        if (retCode != cpp2::ErrorCode::SUCCEEDED) {
            LOG(WARNING) << "Alter edge property error " << static_cast<int32_t>(retCode);
            resp_.set_code(retCode);
            onFinished();
            return;
        }
    }

    auto retProp = MetaServiceUtils::checkSchemaTTLProp(prop);
    if (!retProp.ok()) {
        LOG(WARNING) << "Implicit ttl_col not support";
        resp_.set_code(cpp2::ErrorCode::E_UNSUPPORTED);
        onFinished();
        return;
    }

    schema.set_columns(std::move(columns));
    schema.set_schema_prop(std::move(prop));

    std::vector<kvstore::KV> data;
    LOG(INFO) << "Alter edge " << req.get_edge_name() << ", edgeTye " << edgeType;
    data.emplace_back(MetaServiceUtils::schemaEdgeKey(req.get_space_id(), edgeType, version),
                      MetaServiceUtils::schemaEdgeVal(req.get_edge_name(), schema));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(edgeType, EntryType::EDGE));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

