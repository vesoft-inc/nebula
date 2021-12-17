/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/listener/ListenerProcessor.h"

#include "meta/ActiveHostsMan.h"

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
      LOG(ERROR) << "Add listener failed, listener already exists.";
      ret = nebula::cpp2::ErrorCode::E_EXISTED;
    } else {
      LOG(ERROR) << "Add listener failed, error: " << apache::thrift::util::enumNameSafe(ret);
    }
    handleErrorCode(ret);
    onFinished();
    return;
  }

  // TODO : (sky) if type is elasticsearch, need check text search service.
  folly::SharedMutex::WriteHolder lHolder(LockUtils::listenerLock());
  folly::SharedMutex::WriteHolder mHolder(LockUtils::machineLock());
  folly::SharedMutex::ReadHolder sHolder(LockUtils::spaceLock());
  const auto& prefix = MetaKeyUtils::partPrefix(space);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(ERROR) << "List parts failed, error: " << apache::thrift::util::enumNameSafe(retCode);
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

  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  for (auto& host : hosts) {
    auto machineKey = MetaKeyUtils::machineKey(host.host, host.port);
    if (machineExist(machineKey) == nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "The host " << host << " have existed!";
      code = nebula::cpp2::ErrorCode::E_EXISTED;
      break;
    }
    data.emplace_back(machineKey, "");
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
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
      LOG(ERROR) << "Remove listener failed, listener not exists.";
    } else {
      LOG(ERROR) << "Remove listener failed, error: " << apache::thrift::util::enumNameSafe(ret);
    }
    handleErrorCode(ret);
    onFinished();
    return;
  }

  folly::SharedMutex::WriteHolder lHolder(LockUtils::listenerLock());
  folly::SharedMutex::WriteHolder mHolder(LockUtils::machineLock());
  std::vector<std::string> keys;
  const auto& prefix = MetaKeyUtils::listenerPrefix(space, type);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(ERROR) << "Remove listener failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  nebula::cpp2::ErrorCode code = nebula::cpp2::ErrorCode::SUCCEEDED;
  while (iter->valid()) {
    auto host = MetaKeyUtils::deserializeHostAddr(iter->val());
    auto machineKey = MetaKeyUtils::machineKey(host.host, host.port);
    if (machineExist(machineKey) != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "The host " << HostAddr(host.host, host.port) << " not existed!";
      code = nebula::cpp2::ErrorCode::E_NO_HOSTS;
      break;
    }

    keys.emplace_back(iter->key());
    keys.emplace_back(std::move(machineKey));
    iter->next();
  }

  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    handleErrorCode(code);
    onFinished();
    return;
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
    LOG(ERROR) << "List listener failed, error: " << apache::thrift::util::enumNameSafe(retCode);
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
    listener.set_type(MetaKeyUtils::parseListenerType(iter->key()));
    listener.set_host(MetaKeyUtils::deserializeHostAddr(iter->val()));
    listener.set_part_id(MetaKeyUtils::parseListenerPart(iter->key()));
    if (std::find(activeHosts.begin(), activeHosts.end(), *listener.host_ref()) !=
        activeHosts.end()) {
      listener.set_status(cpp2::HostStatus::ONLINE);
    } else {
      listener.set_status(cpp2::HostStatus::OFFLINE);
    }
    listeners.emplace_back(std::move(listener));
    iter->next();
  }
  resp_.set_listeners(std::move(listeners));
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
