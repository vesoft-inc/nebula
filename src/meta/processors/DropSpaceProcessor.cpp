/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/DropSpaceProcessor.h"

namespace nebula {
namespace meta {

void DropSpaceProcessor::process(const cpp2::DropSpaceReq& req) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    auto spaceRet = getSpaceId(req.get_space_name());

    if (!spaceRet.ok()) {
        resp_.set_code(to(std::move(spaceRet.status())));
        onFinished();
        return;;
    }

    auto spaceId = spaceRet.value();
    VLOG(3) << "Drop space " << req.get_space_name() << ", id " << spaceId;
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    std::vector<std::string> deleteKeys;

    auto prefix = MetaServiceUtils::partPrefix(spaceId);
    auto res = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix);
    if (!ok(res)) {
        resp_.set_code(to(error(res)));
        onFinished();
        return;
    }

    std::unique_ptr<kvstore::KVIterator> iter = value(std::move(res));
    while (iter->valid()) {
        deleteKeys.emplace_back(iter->key());
        iter->next();
    }

    deleteKeys.emplace_back(MetaServiceUtils::indexSpaceKey(req.get_space_name()));
    deleteKeys.emplace_back(MetaServiceUtils::spaceKey(spaceId));

    // TODO(YT) delete Tag/Edge under the space
    doMultiRemove(std::move(deleteKeys));
    // TODO(YT) delete part files of the space
}

}  // namespace meta
}  // namespace nebula
