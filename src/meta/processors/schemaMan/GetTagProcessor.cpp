/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/GetTagProcessor.h"

namespace nebula {
namespace meta {

void GetTagProcessor::process(const cpp2::GetTagReq& req) {
    auto spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::tagLock());
    auto tagIdRet = getTagId(spaceId, req.get_tag_name());
    if (!tagIdRet.ok()) {
        resp_.set_code(to(tagIdRet.status()));
        onFinished();
        return;
    }
    auto tagId = tagIdRet.value();

    std::string schemaValue;
    // Get the lastest version
    if (req.get_version() < 0) {
        auto tagPrefix = MetaServiceUtils::schemaTagPrefix(spaceId, tagId);
        auto ret = doPrefix(tagPrefix);
        if (!ret.ok()) {
            LOG(ERROR) << "Get Tag SpaceID: " << spaceId
                       << ", tagName: " << req.get_tag_name()
                       << ", version " << req.get_version() << " not found";
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        schemaValue = ret.value()->val().str();
    } else {
        auto tagKey = MetaServiceUtils::schemaTagKey(spaceId,
                                                     tagId,
                                                     req.get_version());
        auto ret = doGet(tagKey);
        if (!ret.ok()) {
            LOG(ERROR) << "Get Tag SpaceID: " << spaceId
                       << ", tagName: " << req.get_tag_name()
                       << ", version " << req.get_version() << " not found";
            resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
            onFinished();
            return;
        }
        schemaValue = ret.value();
    }

    VLOG(3) << "Get Tag SpaceID: " << spaceId
            << ", tagName: " << req.get_tag_name()
            << ", version " << req.get_version();

    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    auto schema = MetaServiceUtils::parseSchema(schemaValue);
    auto indexKey = getTagIndexKey(spaceId, tagId);
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
std::vector<::nebula::cpp2::IndexItem> GetTagProcessor::getTagIndexKey(GraphSpaceID spaceId,
    TagID tagId) {
    std::vector<::nebula::cpp2::IndexItem> items;
    auto prefix = MetaServiceUtils::indexPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Tag Index Failed: SpaceID " << spaceId;
        return items;
    }

    while (iter->valid()) {
        auto val = iter->val();
        auto item = MetaServiceUtils::parseIndex(val);
        if (item.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::tag_id
            && item.get_schema_id().get_tag_id() == tagId) {
            items.emplace_back(std::move(item));
        }
        iter->next();
    }
    return items;
}

}  // namespace meta
}  // namespace nebula

