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
            resp_.set_code(cpp2::ErrorCode::E_CONFLICT);
            onFinished();
            return;
        }
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::edgeLock());
    auto ret = getEdgeType(req.get_space_id(), edgeName);
    if (ret.ok()) {
        cpp2::ErrorCode ec;
        if (req.get_if_not_exists()) {
            ec = cpp2::ErrorCode::SUCCEEDED;
        } else {
            ec = cpp2::ErrorCode::E_EXISTED;
        }
        resp_.set_id(to(ret.value(), EntryType::EDGE));
        resp_.set_code(ec);
        onFinished();
        return;
    }

    auto edgeTypeRet = autoIncrementId();
    if (!nebula::ok(edgeTypeRet)) {
        LOG(ERROR) << "Create edge failed : Get edge type id failed";
        resp_.set_code(nebula::error(edgeTypeRet));
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
            auto value = column.get_default_value();
            std::string defaultValue;
            switch (column.get_type().get_type()) {
                case nebula::cpp2::SupportedType::BOOL:
                    if (value->getType() != nebula::cpp2::Value::Type::bool_value) {
                        LOG(ERROR) << "Create Edge Failed: " << name
                                   << " type mismatch";
                        resp_.set_code(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->get_bool_value());
                    break;
                case nebula::cpp2::SupportedType::INT:
                    if (value->getType() != nebula::cpp2::Value::Type::int_value) {
                        LOG(ERROR) << "Create Edge Failed: " << name
                                   << " type mismatch";
                        resp_.set_code(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->get_int_value());
                    break;
                case nebula::cpp2::SupportedType::DOUBLE:
                    if (value->getType() != nebula::cpp2::Value::Type::double_value) {
                        LOG(ERROR) << "Create Edge Failed: " << name
                                   << " type mismatch";
                        resp_.set_code(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->get_double_value());
                    break;
                case nebula::cpp2::SupportedType::STRING:
                    if (value->getType() != nebula::cpp2::Value::Type::string_value) {
                        LOG(ERROR) << "Create Edge Failed: " << name
                                   << " type mismatch";
                        resp_.set_code(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = value->get_string_value();
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
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(edgeType, EntryType::EDGE));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula

