/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/ListTagsProcessor.h"

namespace nebula {
namespace meta {

void ListTagsProcessor::process(const cpp2::ListTagsReq& req) {
    auto& tagLock = LockUtils::tagLock();
    if (!tagLock.try_lock_shared()) {
        resp_.set_code(cpp2::ErrorCode::E_TABLE_LOCKED);
        onFinished();
        return;
    }
    auto spaceId = req.get_space_id();
    auto prefix = MetaUtils::schemaTagsPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    folly::RWSpinLock::ReadHolder rHolder(LockUtils::tagLock());
    auto ret = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix, &iter);
    resp_.set_code(to(ret));
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        tagLock.unlock_shared();
        onFinished();
        return;
    }
    decltype(resp_.tags) tags;
    while (iter->valid()) {
        cpp2::TagItem tag;
        auto key = iter->key();
        auto val = iter->val();
        auto tagID = *reinterpret_cast<const TagID *>(key.data() + prefix.size());
        auto vers = *reinterpret_cast<const int64_t *>(key.data() + prefix.size() + sizeof(TagID));
        auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
        auto tagName = val.subpiece(sizeof(int32_t), nameLen).str();
        auto schema = MetaUtils::parseSchema(val);
        tag.set_tag_id(tagID);
        tag.set_tag_name(tagName.data());
        tag.set_version(vers);
        tag.set_schema(schema);
        tags.emplace_back(tag);
        iter->next();
    }
    tagLock.unlock_shared();
    resp_.set_tags(std::move(tags));
    onFinished();
}
}  // namespace meta
}  // namespace nebula
