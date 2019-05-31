/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/partsMan/GetSpaceProcessor.h"

namespace nebula {
namespace meta {

void GetSpaceProcessor::process(const cpp2::GetSpaceReq& req) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto spaceId = req.get_space_id();
    std::string spaceKey = MetaServiceUtils::spaceKey(spaceId);
    auto ret = doGet(std::move(spaceKey));
    if (!ret.ok()) {
        LOG(ERROR) << "Get Space SpaceID: " << req.get_space_id() << " not found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    auto properties = MetaServiceUtils::parseSpace(ret.value());
    VLOG(3) << "Get Space SpaceID: " << req.get_space_id() << ", Name "
            << properties.get_space_name() << ", Partition Num "
            << properties.get_partition_num() << ", Replica Factor "
            << properties.get_replica_factor();

    cpp2::SpaceItem item;
    item.set_space_id(spaceId);
    item.set_properties(properties);
    resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
    resp_.set_item(item);
    onFinished();
}

}  // namespace meta
}  // namespace nebula


