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
            const auto* value = column.get_default_value();
            std::string defaultValue;
            switch (column.get_type()) {
                case cpp2::PropertyType::BOOL:
                    if (value->type() != nebula::Value::Type::BOOL) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->getBool());
                    break;
                case cpp2::PropertyType::INT64:
                    if (value->type() != nebula::Value::Type::INT) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->getInt());
                    break;
                case cpp2::PropertyType::DOUBLE:
                    if (value->type() != nebula::Value::Type::FLOAT) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->getFloat());
                    break;
                case cpp2::PropertyType::STRING:
                    if (value->type() != nebula::Value::Type::STRING) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->getStr());
                    break;
                case cpp2::PropertyType::TIMESTAMP:
                    if (value->type() != nebula::Value::Type::INT) {
                        LOG(ERROR) << "Create Tag Failed: " << name
                                   << " type mismatch";
                        handleErrorCode(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->getInt());
                    break;
                default:
                    LOG(ERROR) << "Unsupported type";
                    return;
            }

            LOG(INFO) << "Get Tag Default value: Property Name " << name
                    << ", Value " << defaultValue;
            auto defaultKey = MetaServiceUtils::tagDefaultKey(req.get_space_id(),
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
