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
    auto spaceRet = getSpaceId(req.get_space_name());
    if (!spaceRet.ok()) {
        handleErrorCode(MetaCommon::to(spaceRet.status()));
        onFinished();
        return;
    }

    auto spaceId = spaceRet.value();
    std::string spaceKey = MetaServiceUtils::spaceKey(spaceId);
    auto ret = doGet(spaceKey);
    if (!ret.ok()) {
        LOG(ERROR) << "Get Space SpaceName: " << req.get_space_name() << " not found";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    auto properties = MetaServiceUtils::parseSpace(ret.value());
    VLOG(3) << "Get Space SpaceName: " << req.get_space_name()
            << ", Partition Num " << properties.get_partition_num()
            << ", Replica Factor " << properties.get_replica_factor();
    if (properties.__isset.group_name) {
        LOG(INFO) << "Space " << req.get_space_name()
                  << " is bind to the group " << *properties.get_group_name();
    }

    cpp2::SpaceItem item;
    item.set_space_id(spaceId);
    item.set_properties(std::move(properties));
    resp_.set_item(std::move(item));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}

}  // namespace meta
}  // namespace nebula


