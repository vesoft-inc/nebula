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
    {
        // if there is an edge of the same name
        // TODO: there exists race condition, we should address it in the future
        folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeLock());
        auto conflictRet = getEdgeType(req.get_space_id(), req.get_tag_name());
        if (conflictRet.ok()) {
            LOG(ERROR) << "Failed to create tag `" << req.get_tag_name()
                       << "': some edge with the same name already exists.";
            resp_.set_id(to(conflictRet.value(), EntryType::TAG));
            resp_.set_code(cpp2::ErrorCode::E_CONFLICT);
            onFinished();
            return;
        }
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto ret = getTagId(req.get_space_id(), req.get_tag_name());
    if (ret.ok()) {
        LOG(ERROR) << "Create Tag Failed :" << req.get_tag_name() << " has existed";
        resp_.set_id(to(ret.value(), EntryType::TAG));
        resp_.set_code(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }

    auto tagRet = autoIncrementId();
    if (!nebula::ok(tagRet)) {
        LOG(ERROR) << "Create tag failed : Get tag id failed";
        resp_.set_code(nebula::error(tagRet));
        onFinished();
        return;
    }
    auto tagId = nebula::value(tagRet);
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::indexTagKey(req.get_space_id(), req.get_tag_name()),
                      std::string(reinterpret_cast<const char*>(&tagId), sizeof(tagId)));
    LOG(INFO) << "Create Tag " << req.get_tag_name() << ", tagId " << tagId;
    data.emplace_back(MetaServiceUtils::schemaTagKey(req.get_space_id(), tagId, 0),
                      MetaServiceUtils::schemaTagVal(req.get_tag_name(), req.get_schema()));

    auto columns = req.get_schema().get_columns();
    for (auto& column : columns) {
        if (column.__isset.default_value) {
            auto value = column.get_default_value();
            std::string defaultValue;
            switch (column.get_type().get_type()) {
                case nebula::cpp2::SupportedType::BOOL:
                    if (value->getType() != nebula::cpp2::Value::Type::bool_value) {
                        LOG(ERROR) << "Create Tag Failed: " << column.get_name()
                                   << " type mismatch";
                        resp_.set_code(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->get_bool_value());
                    break;
                case nebula::cpp2::SupportedType::INT:
                    if (value->getType() != nebula::cpp2::Value::Type::int_value) {
                        LOG(ERROR) << "Create Tag Failed: " << column.get_name()
                                   << " type mismatch";
                        resp_.set_code(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->get_int_value());
                    break;
                case nebula::cpp2::SupportedType::DOUBLE:
                    if (value->getType() != nebula::cpp2::Value::Type::double_value) {
                        LOG(ERROR) << "Create Tag Failed: " << column.get_name()
                                   << " type mismatch";
                        resp_.set_code(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->get_double_value());
                    break;
                case nebula::cpp2::SupportedType::STRING:
                    if (value->getType() != nebula::cpp2::Value::Type::string_value) {
                        LOG(ERROR) << "Create Tag Failed: " << column.get_name()
                                   << " type mismatch";
                        resp_.set_code(cpp2::ErrorCode::E_CONFLICT);
                        onFinished();
                        return;
                    }
                    defaultValue = folly::to<std::string>(value->get_string_value());
                    break;
                default:
                    LOG(ERROR) << "Unsupported type";
                    return;
            }

            LOG(INFO) << "Get Tag Default value: Property Name " << column.get_name()
                    << ", Value " << defaultValue;
            auto defaultKey = MetaServiceUtils::tagDefaultKey(req.get_space_id(),
                                                              tagId,
                                                              column.get_name());
            data.emplace_back(std::move(defaultKey), std::move(defaultValue));
        }
    }

    LOG(INFO) << "Create Tag " << req.get_tag_name() << ", TagID " << tagId;
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_id(to(tagId, EntryType::TAG));
    doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula
