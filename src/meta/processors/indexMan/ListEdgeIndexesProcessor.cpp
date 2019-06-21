/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/ListEdgeIndexesProcessor.h"

namespace nebula {
namespace meta {

void ListEdgeIndexesProcessor::process(const cpp2::ListEdgeIndexesReq& req) {
    CHECK_SPACE_ID_AND_RETURN(req.get_space_id());
    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeIndexLock());
    auto spaceId = req.get_space_id();
    auto prefix = MetaServiceUtils::edgeIndexPrefix(spaceId);

    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    resp_.set_code(to(ret));
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        onFinished();
        return;
    }

    decltype(resp_.items) items;
    while (iter->valid()) {
        // auto key = iter->key();
        auto val = iter->val();
        auto properties = MetaServiceUtils::parseEdgeIndex(val);
        items.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                           spaceId, properties);
        iter->next();
    }
    resp_.set_items(std::move(items));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

