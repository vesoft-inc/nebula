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
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::spacePrefix();
    auto ret = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix);
    if (!ok(ret)) {
        resp_.set_code(to(error(ret)));
        onFinished();
        return;
    } else {
        resp_.set_code(to(kvstore::ResultCode::SUCCEEDED));
    }

    std::unique_ptr<kvstore::KVIterator> iter = value(std::move(ret));
    std::vector<cpp2::IdName> spaces;
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        auto spaceName = MetaServiceUtils::spaceName(iter->val());
        VLOG(3) << "List spaces " << spaceId << ", name " << spaceName.str();
        spaces.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                            to(spaceId, EntryType::SPACE),
                            spaceName.str());
        iter->next();
    }
    resp_.set_spaces(std::move(spaces));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

