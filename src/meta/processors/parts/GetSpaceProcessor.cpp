/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/parts/GetSpaceProcessor.h"

namespace nebula {
namespace meta {

void GetSpaceProcessor::process(const cpp2::GetSpaceReq& req) {
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  const auto& spaceName = req.get_space_name();

  auto spaceRet = getSpaceId(spaceName);
  if (!nebula::ok(spaceRet)) {
    auto retCode = nebula::error(spaceRet);
    LOG(INFO) << "Get space Failed, SpaceName " << spaceName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto spaceId = nebula::value(spaceRet);
  std::string spaceKey = MetaKeyUtils::spaceKey(spaceId);
  auto ret = doGet(spaceKey);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(INFO) << "Get Space SpaceName: " << spaceName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto properties = MetaKeyUtils::parseSpace(nebula::value(ret));
  VLOG(2) << "Get Space SpaceName: " << spaceName << ", Partition Num "
          << properties.get_partition_num() << ", Replica Factor "
          << properties.get_replica_factor() << ", bind to the zones "
          << folly::join(",", properties.get_zone_names());

  cpp2::SpaceItem item;
  item.space_id_ref() = spaceId;
  item.properties_ref() = std::move(properties);
  resp_.item_ref() = std::move(item);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
