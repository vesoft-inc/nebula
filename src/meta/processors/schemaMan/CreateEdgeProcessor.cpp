/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/CreateEdgeProcessor.h"

namespace nebula {
namespace meta {

void CreateEdgeProcessor::process(const cpp2::CreateEdgeReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    auto edgeName = req.get_edge_name();
    {
        // if there is an edge of the same name
        // TODO: there exists race condition, we should address it in the future
        folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
        auto conflictRet = getTagId(req.get_space_id(), edgeName);
        if (conflictRet.ok()) {
            LOG(ERROR) << "Failed to create edge `" << edgeName
                       << "': some edge with the same name already exists.";
            resp_.set_id(to(conflictRet.value(), EntryType::EDGE));
            handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
            onFinished();
            return;
        }
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto ret = getEdgeType(req.get_space_id(), edgeName);
    if (ret.ok()) {
        if (req.get_if_not_exists()) {
            handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        } else {
            handleErrorCode(cpp2::ErrorCode::E_EXISTED);
        }
        resp_.set_id(to(ret.value(), EntryType::EDGE));
        onFinished();
        return;
    }

    auto edgeTypeRet = autoIncrementId();
    if (!nebula::ok(edgeTypeRet)) {
        LOG(ERROR) << "Create edge failed : Get edge type id failed";
        handleErrorCode(nebula::error(edgeTypeRet));
        onFinished();
        return;
    }
    auto edgeType = nebula::value(edgeTypeRet);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexEdgeKey(req.get_space_id(), edgeName),
                      std::string(reinterpret_cast<const char*>(&edgeType), sizeof(EdgeType)));
    data.emplace_back(MetaServiceUtils::schemaEdgeKey(req.get_space_id(), edgeType, 0),
                      MetaServiceUtils::schemaEdgeVal(edgeName, req.get_schema()));

    LOG(INFO) << "Create Edge " << edgeName << ", edgeType " << edgeType;
    auto columns = req.get_schema().get_columns();
    for (auto& column : columns) {
        if (column.__isset.default_value) {
            auto name = column.get_name();
            const auto* value = column.get_default_value();
            std::string defaultValue;
            switch (column.get_type()) {
                case cpp2::PropertyType::BOOL:
                    if (value->type() != nebula::Value::Type::BOOL) {
                        LOG(ERROR) << "Create Edge Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->getBool());
                    break;
                case cpp2::PropertyType::INT64:
                    if (value->type() != nebula::Value::Type::INT) {
                        LOG(ERROR) << "Create Edge Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->getInt());
                    break;
                case cpp2::PropertyType::DOUBLE:
                    if (value->type() != nebula::Value::Type::FLOAT) {
                        LOG(ERROR) << "Create Edge Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->getFloat());
                    break;
                case cpp2::PropertyType::STRING:
                    if (value->type() != nebula::Value::Type::STRING) {
                        LOG(ERROR) << "Create Edge Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = value->getStr();
                    break;
                default:
                    break;
            }
            VLOG(3) << "Get Edge Default value: Property Name " << name
                    << ", Value " << defaultValue;
            auto defaultKey = MetaServiceUtils::edgeDefaultKey(req.get_space_id(),
                                                               edgeType,
                                                               name);
            data.emplace_back(std::move(defaultKey), std::move(defaultValue));
        }
    }

    LOG(INFO) << "Create Edge " << edgeName << ", edgeType " << edgeType;
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(edgeType, EntryType::EDGE));
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula

