/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/listener/ListenerProcessor.h"

#include "kvstore/LogEncoder.h"
#include "meta/ActiveHostsMan.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

void AddListenerProcessor::process(const cpp2::AddListenerReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
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

  auto listenerIdRet = autoIncrementIdInSpace(space);
  if (!nebula::ok(listenerIdRet)) {
    LOG(INFO) << "Add listnener failed : Get listener id failed.";
    handleErrorCode(nebula::error(listenerIdRet));
    onFinished();
    return;
  }
  auto listenerId = nebula::value(listenerIdRet);
  std::vector<kvstore::KV> data;
  for (size_t i = 0; i < parts.size(); i++) {
    data.emplace_back(MetaKeyUtils::listenerKey(space, parts[i], type, listenerId),
                      MetaKeyUtils::serializeHostAddr(hosts[i % hosts.size()]));
  }
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto result = doSyncPut(std::move(data));
  handleErrorCode(result);
  onFinished();
}

void RemoveListenerProcessor::process(const cpp2::RemoveListenerReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
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
  auto batchHolder = std::make_unique<kvstore::BatchHolder>();
  while (iter->valid()) {
    auto key = iter->key();
    batchHolder->remove(key.str());
    iter->next();
  }

  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(batchHolder.get(), timeInMilliSec);
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
}

void ListListenersProcessor::process(const cpp2::ListListenersReq& req) {
  auto space = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(space);
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
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
                                     cpp2::HostRole::STORAGE_LISTENER);
  if (!nebula::ok(activeHostsRet)) {
    handleErrorCode(nebula::error(activeHostsRet));
    onFinished();
    return;
  }
  auto activeHosts = std::move(nebula::value(activeHostsRet));

  std::vector<cpp2::ListenerInfo> listeners;
  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    cpp2::ListenerInfo listener;
    auto listenerKey = iter->key();
    listener.part_id_ref() = MetaKeyUtils::parseListenerPart(listenerKey);
    listener.type_ref() = MetaKeyUtils::parseListenerType(listenerKey);
    listener.listener_id_ref() = MetaKeyUtils::parseListenerId(listenerKey);
    listener.host_ref() = MetaKeyUtils::deserializeHostAddr(iter->val());

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
