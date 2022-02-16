/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/listener/ListenerProcessor.h"

#include <folly/SharedMutex.h>              // for SharedMutex
#include <gflags/gflags_declare.h>          // for DECLARE_int32, DECLAR...
#include <stddef.h>                         // for size_t
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <algorithm>  // for max, find
#include <memory>     // for unique_ptr
#include <ostream>    // for operator<<, basic_ost...
#include <string>     // for string, basic_string
#include <vector>     // for vector

#include "common/base/ErrorOr.h"              // for error, ok, value
#include "common/base/Logging.h"              // for LOG, LogMessage, _LOG...
#include "common/datatypes/HostAddr.h"        // for HostAddr
#include "common/thrift/ThriftTypes.h"        // for PartitionID
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "kvstore/Common.h"                   // for KV
#include "kvstore/KVIterator.h"               // for KVIterator
#include "meta/ActiveHostsMan.h"              // for ActiveHostsMan
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doPrefix
#include "meta/processors/Common.h"           // for LockUtils

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

void AddListenerProcessor::process(const cpp2::AddListenerReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  auto type = req.get_type();
  const auto& hosts = req.get_hosts();
  auto ret = listenerExist(space, type);
  if (ret != nebula::cpp2::ErrorCode::E_LISTENER_NOT_FOUND) {
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(INFO) << "Add listener failed, listener already exists.";
      ret = nebula::cpp2::ErrorCode::E_EXISTED;
    } else {
      LOG(INFO) << "Add listener failed, error: " << apache::thrift::util::enumNameSafe(ret);
    }
    handleErrorCode(ret);
    onFinished();
    return;
  }

  // TODO : (sky) if type is elasticsearch, need check text search service.
  folly::SharedMutex::WriteHolder wHolder(LockUtils::listenerLock());
  folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
  const auto& prefix = MetaKeyUtils::partPrefix(space);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "List parts failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  std::vector<PartitionID> parts;
  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    parts.emplace_back(MetaKeyUtils::parsePartKeyPartId(iter->key()));
    iter->next();
  }
  std::vector<kvstore::KV> data;
  for (size_t i = 0; i < parts.size(); i++) {
    data.emplace_back(MetaKeyUtils::listenerKey(space, parts[i], type),
                      MetaKeyUtils::serializeHostAddr(hosts[i % hosts.size()]));
  }
  doSyncPutAndUpdate(std::move(data));
}

void RemoveListenerProcessor::process(const cpp2::RemoveListenerReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  auto type = req.get_type();
  auto ret = listenerExist(space, type);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (ret == nebula::cpp2::ErrorCode::E_LISTENER_NOT_FOUND) {
      LOG(INFO) << "Remove listener failed, listener not exists.";
    } else {
      LOG(INFO) << "Remove listener failed, error: " << apache::thrift::util::enumNameSafe(ret);
    }
    handleErrorCode(ret);
    onFinished();
    return;
  }

  folly::SharedMutex::WriteHolder wHolder(LockUtils::listenerLock());
  std::vector<std::string> keys;
  const auto& prefix = MetaKeyUtils::listenerPrefix(space, type);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "Remove listener failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    keys.emplace_back(iter->key());
    iter->next();
  }
  doSyncMultiRemoveAndUpdate(std::move(keys));
}

void ListListenerProcessor::process(const cpp2::ListListenerReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  folly::SharedMutex::ReadHolder rHolder(LockUtils::listenerLock());
  const auto& prefix = MetaKeyUtils::listenerPrefix(space);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "List listener failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto activeHostsRet =
      ActiveHostsMan::getActiveHosts(kvstore_,
                                     FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor,
                                     cpp2::HostRole::LISTENER);
  if (!nebula::ok(activeHostsRet)) {
    handleErrorCode(nebula::error(activeHostsRet));
    onFinished();
    return;
  }

  std::vector<nebula::meta::cpp2::ListenerInfo> listeners;
  auto activeHosts = std::move(nebula::value(activeHostsRet));
  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    cpp2::ListenerInfo listener;
    listener.type_ref() = MetaKeyUtils::parseListenerType(iter->key());
    listener.host_ref() = MetaKeyUtils::deserializeHostAddr(iter->val());
    listener.part_id_ref() = MetaKeyUtils::parseListenerPart(iter->key());
    if (std::find(activeHosts.begin(), activeHosts.end(), *listener.host_ref()) !=
        activeHosts.end()) {
      listener.status_ref() = cpp2::HostStatus::ONLINE;
    } else {
      listener.status_ref() = cpp2::HostStatus::OFFLINE;
    }
    listeners.emplace_back(std::move(listener));
    iter->next();
  }
  resp_.listeners_ref() = std::move(listeners);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
