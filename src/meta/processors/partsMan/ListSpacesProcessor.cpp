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
    auto prefix = MetaServiceUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(MetaCommon::to(ret));
        onFinished();
        return;
    }
    decltype(resp_.spaces) spaces;
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        auto properties = MetaServiceUtils::parseSpace(iter->val());
        VLOG(3) << "List spaces " << spaceId << ", name "
                << properties.get_space_name() << ", Partition Num "
                << properties.get_partition_num() << ", Replica Factor "
                << properties.get_replica_factor() << ", charset "
                << properties.get_charset_name() << ", collate "
                << properties.get_collate_name();
        cpp2::SpaceItem item;
        item.set_space_id(spaceId);
        item.set_properties(properties);
        spaces.emplace_back(std::move(item));
        iter->next();
    }
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_spaces(std::move(spaces));
    onFinished();
}

}  // namespace meta
}  // namespace nebula

