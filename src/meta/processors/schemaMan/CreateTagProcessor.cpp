/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/CreateTagProcessor.h"

namespace nebula {
namespace meta {

void CreateTagProcessor::process(const cpp2::CreateTagReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    auto tagName = req.get_tag_name();
    {
        // if there is an edge of the same name
        // TODO: there exists race condition, we should address it in the future
        folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
        auto conflictRet = getEdgeType(req.get_space_id(), tagName);
        if (conflictRet.ok()) {
            LOG(ERROR) << "Failed to create tag `" << tagName
                       << "': some edge with the same name already exists.";
            resp_.set_id(to(conflictRet.value(), EntryType::TAG));
            handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
            onFinished();
            return;
        }
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getTagId(req.get_space_id(), tagName);
    if (ret.ok()) {
        if (req.get_if_not_exists()) {
            handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
        } else {
            LOG(ERROR) << "Create Tag Failed :" << tagName << " has existed";
            handleErrorCode(cpp2::ErrorCode::E_EXISTED);
        }
        resp_.set_id(to(ret.value(), EntryType::TAG));
        onFinished();
        return;
    }

    auto tagRet = autoIncrementId();
    if (!nebula::ok(tagRet)) {
        LOG(ERROR) << "Create tag failed : Get tag id failed";
        handleErrorCode(nebula::error(tagRet));
        onFinished();
        return;
    }
    auto tagId = nebula::value(tagRet);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexTagKey(req.get_space_id(), tagName),
                      std::string(reinterpret_cast<const char*>(&tagId), sizeof(TagID)));
    LOG(INFO) << "Create Tag " << tagName << ", tagId " << tagId;
    data.emplace_back(MetaServiceUtils::schemaTagKey(req.get_space_id(), tagId, 0),
                      MetaServiceUtils::schemaTagVal(tagName, req.get_schema()));

    auto columns = req.get_schema().get_columns();
    for (auto& column : columns) {
        if (column.__isset.default_value) {
            auto name = column.get_name();
            auto value = column.get_default_value();
            std::string defaultValue;
            switch (column.get_type().get_type()) {
                case nebula::cpp2::SupportedType::BOOL:
                    if (value->getType() != nebula::cpp2::Value::Type::bool_value) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->get_bool_value());
                    break;
                case nebula::cpp2::SupportedType::INT:
                    if (value->getType() != nebula::cpp2::Value::Type::int_value) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->get_int_value());
                    break;
                case nebula::cpp2::SupportedType::DOUBLE:
                    if (value->getType() != nebula::cpp2::Value::Type::double_value) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->get_double_value());
                    break;
                case nebula::cpp2::SupportedType::STRING:
                    if (value->getType() != nebula::cpp2::Value::Type::string_value) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->get_string_value());
                    break;
                case nebula::cpp2::SupportedType::TIMESTAMP:
                    if (value->getType() != nebula::cpp2::Value::Type::timestamp) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->get_timestamp());
                    break;
                default:
                    LOG(ERROR) << "Unknown type " << static_cast<int>(column.get_type().get_type());
                    handleErrorCode(cpp2::ErrorCode::E_INVALID_PARM);
                    onFinished();
                    return;
            }

            LOG(INFO) << "Get Tag Default value: Property Name " << name
                    << ", Value " << defaultValue;
            auto defaultKey = MetaServiceUtils::defaultKey(req.get_space_id(),
                                                              tagId,
                                                              name);
            data.emplace_back(std::move(defaultKey), std::move(defaultValue));
        }
    }

    LOG(INFO) << "Create Tag " << tagName << ", TagID " << tagId;
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(tagId, EntryType::TAG));
    doSyncPutAndUpdate(std::move(data));
}

}  // namespace meta
}  // namespace nebula
