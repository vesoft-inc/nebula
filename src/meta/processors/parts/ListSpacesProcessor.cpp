/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/ListSpacesProcessor.h"

namespace nebula {
namespace meta {

void ListSpacesProcessor::process(const cpp2::ListSpacesReq&) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    const auto& prefix = MetaServiceUtils::spacePrefix();
    auto ret = doPrefix(prefix);
     if (!nebula::ok(ret)) {
        auto retCode = nebula::error(ret);
        LOG(ERROR) << "List spaces failed, error "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }
    auto iter = nebula::value(ret).get();

    std::vector<cpp2::IdName> spaces;
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        auto spaceName = MetaServiceUtils::spaceName(iter->val());
        VLOG(3) << "List spaces " << spaceId << ", name " << spaceName;
        cpp2::IdName space;
        space.set_id(to(spaceId, EntryType::SPACE));
        space.set_name(std::move(spaceName));
        spaces.emplace_back(std::move(space));
        iter->next();
    }

    handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
    resp_.set_spaces(std::move(spaces));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

