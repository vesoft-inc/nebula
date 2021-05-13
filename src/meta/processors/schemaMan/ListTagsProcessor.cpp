/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/ListTagsProcessor.h"

namespace nebula {
namespace meta {

void ListTagsProcessor::process(const cpp2::ListTagsReq& req) {
    GraphSpaceID spaceId = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(spaceId);

    folly::SharedMutex::ReadHolder rHolder(LockUtils::tagLock());
    auto prefix = MetaServiceUtils::schemaTagsPrefix(spaceId);
    auto ret = doPrefix(prefix);
    if (!nebula::ok(ret)) {
        LOG(ERROR) << "List Tags failed, SpaceID: " << spaceId;
        handleErrorCode(nebula::error(ret));
        onFinished();
        return;
    }

    auto iter = nebula::value(ret).get();
    std::vector<nebula::meta::cpp2::TagItem> tags;
    while (iter->valid()) {
        auto key = iter->key();
        auto val = iter->val();
        auto tagID = *reinterpret_cast<const TagID *>(key.data() + prefix.size());
        auto version = MetaServiceUtils::parseTagVersion(key);
        auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
        auto tagName = val.subpiece(sizeof(int32_t), nameLen).str();
        auto schema = MetaServiceUtils::parseSchema(val);
        cpp2::TagItem item;
        item.set_tag_id(tagID);
        item.set_tag_name(std::move(tagName));
        item.set_version(version);
        item.set_schema(std::move(schema));
        tags.emplace_back(std::move(item));
        iter->next();
    }
    resp_.set_tags(std::move(tags));
    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
