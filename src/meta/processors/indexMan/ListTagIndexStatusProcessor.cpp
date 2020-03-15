/* Copyright (c) 2020 vesoft inc. All rights reserved.
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
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Tag Index Status Failed: SpaceID " << space;
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    decltype(resp_.statuses) statuses;
    while (iter->valid()) {
        auto key = iter->key();
        auto offset = prefix.size();
        auto indexName = key.subpiece(offset, key.size() - offset).str();
        auto val = iter->val().str();
        cpp2::IndexStatus status;
        status.set_name(std::move(indexName));
        status.set_status(std::move(val));
        statuses.emplace_back(std::move(status));
        iter->next();
    }
    resp_.set_statuses(std::move(statuses));
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula
