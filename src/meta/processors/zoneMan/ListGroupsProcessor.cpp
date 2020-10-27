/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/zoneMan/ListGroupsProcessor.h"

namespace nebula {
namespace meta {

void ListGroupsProcessor::process(const cpp2::ListGroupsReq&) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::groupLock());
    auto prefix = MetaServiceUtils::groupPrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(MetaCommon::to(ret));
        onFinished();
        return;
    }

    std::vector<cpp2::Group> groups;
    while (iter->valid()) {
        auto groupName = MetaServiceUtils::parseGroupName(iter->key());
        auto zoneNames = MetaServiceUtils::parseZoneNames(iter->val());
        cpp2::Group group;
        group.set_group_name(std::move(groupName));
        group.set_zone_names(std::move(zoneNames));
        groups.emplace_back(std::move(group));
        iter->next();
    }

    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_groups(std::move(groups));
    onFinished();
}

}  // namespace meta
}  // namespace nebula
