/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/indexMan/ListEdgeIndexStatusProcessor.h"

namespace nebula {
namespace meta {

void ListEdgeIndexStatusProcessor::process(const cpp2::ListIndexStatusReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::edgeIndexLock());
    auto prefix = MetaServiceUtils::rebuildEdgeIndexStatusPrefix(space);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Edge Index Status Failed: SpaceID " << space;
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    decltype(resp_.statuses) statuses;
    while (iter->valid()) {
        auto key = iter->key();
        auto offset = prefix.size();
        auto indexName = key.str().substr(offset, (key.size() - offset));
        auto val = iter->val().str();
        cpp2::IndexStatus status;
        status.set_name(std::move(indexName));
        status.set_status(std::move(val));
        statuses.emplace_back(std::move(status));
        iter->next();
    }
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_statuses(std::move(statuses));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
