/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/ListSpacesProcessor.h"

namespace nebula {
namespace meta {

void ListSpacesProcessor::process(const cpp2::ListSpacesReq& req) {
    UNUSED(req);
    auto& spaceLock = LockUtils::spaceLock();
    if (!spaceLock.try_lock_shared()) {
        resp_.set_code(cpp2::ErrorCode::E_TABLE_LOCKED);
        onFinished();
        return;
    }
    auto prefix = MetaUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        spaceLock.unlock_shared();
        resp_.set_code(to(ret));
        onFinished();
        return;
    }
    std::vector<cpp2::IdName> spaces;
    while (iter->valid()) {
        auto spaceId = MetaUtils::spaceId(iter->key());
        auto spaceName = MetaUtils::spaceName(iter->val());
        VLOG(3) << "List spaces " << spaceId << ", name " << spaceName.str();
        spaces.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                            to(spaceId, EntryType::SPACE),
                            spaceName.str());
        iter->next();
    }
    spaceLock.unlock_shared();
    resp_.set_spaces(std::move(spaces));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

