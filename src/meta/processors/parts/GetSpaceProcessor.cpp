/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/parts/GetSpaceProcessor.h"

#include <folly/SharedMutex.h>              // for SharedMutex
#include <folly/String.h>                   // for join
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <algorithm>  // for max
#include <ostream>    // for operator<<, basic_ost...
#include <string>     // for operator<<, char_traits

#include "common/base/ErrorOr.h"              // for error, ok, value
#include "common/base/Logging.h"              // for LogMessage, LOG, _LOG...
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doGet
#include "meta/processors/Common.h"           // for LockUtils

namespace nebula {
namespace meta {

void GetSpaceProcessor::process(const cpp2::GetSpaceReq& req) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
  const auto& spaceName = req.get_space_name();

  auto spaceRet = getSpaceId(spaceName);
  if (!nebula::ok(spaceRet)) {
    auto retCode = nebula::error(spaceRet);
    LOG(ERROR) << "Get space Failed, SpaceName " << spaceName
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
    LOG(ERROR) << "Get Space SpaceName: " << spaceName
               << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto properties = MetaKeyUtils::parseSpace(nebula::value(ret));
  VLOG(3) << "Get Space SpaceName: " << spaceName << ", Partition Num "
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
