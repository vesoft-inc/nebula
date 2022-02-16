/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/zone/AddHostsProcessor.h"

#include <folly/SharedMutex.h>      // for SharedMutex
#include <folly/String.h>           // for stringPrintf
#include <gflags/gflags_declare.h>  // for DECLARE_int32, DECLAR...

#include <algorithm>  // for max, unique
#include <ostream>    // for operator<<, basic_ost...
#include <string>     // for operator<<, char_traits
#include <vector>     // for vector

#include "common/base/ErrorOr.h"              // for error, ok
#include "common/base/Logging.h"              // for LOG, LogMessage, _LOG...
#include "common/datatypes/HostAddr.h"        // for HostAddr, operator<<
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "kvstore/Common.h"                   // for KV
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doPut
#include "meta/processors/Common.h"           // for LockUtils

DECLARE_uint32(expired_time_factor);
DECLARE_int32(removed_threshold_sec);

namespace nebula {
namespace meta {

void AddHostsProcessor::process(const cpp2::AddHostsReq& req) {
  folly::SharedMutex::WriteHolder zHolder(LockUtils::zoneLock());
  folly::SharedMutex::WriteHolder mHolder(LockUtils::machineLock());
  auto hosts = req.get_hosts();
  // Confirm that there are no duplicates in the parameters.
  if (std::unique(hosts.begin(), hosts.end()) != hosts.end()) {
    LOG(ERROR) << "Hosts have duplicated element";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  // Confirm that the parameter is not empty.
  if (hosts.empty()) {
    LOG(ERROR) << "Hosts is empty";
    handleErrorCode(nebula::cpp2::ErrorCode::E_INVALID_PARM);
    onFinished();
    return;
  }

  std::vector<kvstore::KV> data;
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  for (auto& host : hosts) {
    // Ensure that the node is not registered.
    auto machineKey = MetaKeyUtils::machineKey(host.host, host.port);
    if (machineExist(machineKey) == nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "The host " << host << " have existed!";
      code = nebula::cpp2::ErrorCode::E_EXISTED;
      break;
    }

    // Automatic generation zone
    auto zoneName = folly::stringPrintf("default_zone_%s_%d", host.host.c_str(), host.port);
    auto zoneIdRet = getZoneId(zoneName);
    if (nebula::ok(zoneIdRet)) {
      LOG(ERROR) << "Zone " << zoneName << " have existed";
      code = nebula::error(zoneIdRet);
      break;
    }

    auto zoneKey = MetaKeyUtils::zoneKey(zoneName);
    auto zoneVal = MetaKeyUtils::zoneVal({host});
    data.emplace_back(std::move(machineKey), "");
    data.emplace_back(std::move(zoneKey), std::move(zoneVal));
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
  }

  doPut(std::move(data));
}

}  // namespace meta
}  // namespace nebula
