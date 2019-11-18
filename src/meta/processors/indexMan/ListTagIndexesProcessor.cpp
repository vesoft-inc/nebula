/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/ListTagIndexesProcessor.h"

namespace nebula {
namespace meta {

void ListTagIndexesProcessor::process(const cpp2::ListTagIndexesReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::ReadHolder rHolder(LockUtils::tagIndexLock());
    auto spaceId = req.get_space_id();
    auto prefix = MetaServiceUtils::tagIndexPrefix(spaceId);

    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    resp_.set_code(to(ret));
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Tag Index Failed: SpaceID " << req.get_space_id();
        onFinished();
        return;
    }

    decltype(resp_.items) items;
    while (iter->valid()) {
        auto key = iter->key();
        auto val = iter->val();
        auto tagIndex = *reinterpret_cast<const TagIndexID *>(key.data() + prefix.size());
        auto nameSize = *reinterpret_cast<const int32_t *>(val.data());
        auto name = val.subpiece(sizeof(int32_t), nameSize).str();
        auto properties = MetaServiceUtils::parseTagIndex(val);
        items.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                           tagIndex, name, properties);
        iter->next();
    }
    resp_.set_items(std::move(items));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

