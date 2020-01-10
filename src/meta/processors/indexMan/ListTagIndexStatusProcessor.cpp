/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/ListTagIndexStatusProcessor.h"

namespace nebula {
namespace meta {

void ListTagIndexStatusProcessor::process(const cpp2::ListIndexStatusReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::tagIndexLock());
    auto prefix = MetaServiceUtils::rebuildTagIndexStatusPrefix(space);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    resp_.set_code(to(ret));
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Tag Index Status Failed: SpaceID " << space;
        onFinished();
        return;
    }

    // decltype(resp_.items) items;
    while (iter->valid()) {
        iter->next();
    }
    onFinished();
}

}  // namespace meta
}  // namespace nebula
