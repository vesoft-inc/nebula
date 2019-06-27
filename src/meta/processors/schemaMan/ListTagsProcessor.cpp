/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/schemaMan/ListTagsProcessor.h"

namespace nebula {
namespace meta {

void ListTagsProcessor::process(const cpp2::ListTagsReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::ReadHolder rHolder(LockUtils::tagLock());
    auto spaceId = req.get_space_id();
    auto prefix = MetaServiceUtils::schemaTagsPrefix(spaceId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    resp_.set_code(to(ret));
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        onFinished();
        return;
    }
    decltype(resp_.tags) tags;
    while (iter->valid()) {
        auto key = iter->key();
        auto val = iter->val();
        auto tagID = *reinterpret_cast<const TagID *>(key.data() + prefix.size());
        auto vers = MetaServiceUtils::parseTagVersion(key);
        auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
        auto tagName = val.subpiece(sizeof(int32_t), nameLen).str();
        auto schema = MetaServiceUtils::parseSchema(val);
        tags.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                          tagID, tagName, vers, schema);
        iter->next();
    }
    resp_.set_tags(std::move(tags));
    onFinished();
}
}  // namespace meta
}  // namespace nebula
