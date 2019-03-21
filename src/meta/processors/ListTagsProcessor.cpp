/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/ListTagsProcessor.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/Common.h"

namespace nebula {
namespace meta {

void ListTagsProcessor::process(const cpp2::ListTagsReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::tagLock());
    auto spaceId = req.get_space_id();
    auto prefix = MetaServiceUtils::schemaTagsPrefix(spaceId);
    auto res = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix);
    if (!ok(res)) {
        resp_.set_code(to(error(res)));
        onFinished();
        return;
    } else {
        resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    }

    std::unique_ptr<kvstore::KVIterator> iter = value(std::move(res));
    decltype(resp_.tags) tags;
    while (iter->valid()) {
        cpp2::TagItem tag;
        auto key = iter->key();
        auto val = iter->val();
        auto tagID = *reinterpret_cast<const TagID *>(key.data() + prefix.size());
        auto vers = MetaServiceUtils::parseTagVersion(key);
        auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
        auto tagName = val.subpiece(sizeof(int32_t), nameLen).str();
        auto schema = MetaServiceUtils::parseSchema(val);
        cpp2::TagItem tagItem(apache::thrift::FragileConstructor::FRAGILE,
                              tagID, tagName, vers, schema);
        tags.emplace_back(std::move(tagItem));
        iter->next();
    }
    resp_.set_tags(std::move(tags));
    onFinished();
}
}  // namespace meta
}  // namespace nebula
