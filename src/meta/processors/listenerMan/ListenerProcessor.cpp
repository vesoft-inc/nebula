/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/listenerMan/ListenerProcessor.h"
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
           LOG(ERROR) << "Add listener failed, error: "
                      << apache::thrift::util::enumNameSafe(ret);
        }
        handleErrorCode(ret);
        onFinished();
        return;
    }

    // TODO : (sky) if type is elasticsearch, need check text search service.
    folly::SharedMutex::WriteHolder wHolder(LockUtils::listenerLock());
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    const auto& prefix = MetaServiceUtils::partPrefix(space);
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "List parts failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    std::vector<PartitionID> parts;
    auto iter = nebula::value(iterRet).get();
    while (iter->valid()) {
        parts.emplace_back(MetaServiceUtils::parsePartKeyPartId(iter->key()));
        iter->next();
    }
    std::vector<kvstore::KV> data;
    for (size_t i = 0; i < parts.size(); i++) {
        data.emplace_back(MetaServiceUtils::listenerKey(space, parts[i], type),
                          MetaServiceUtils::serializeHostAddr(hosts[i%hosts.size()]));
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
            LOG(ERROR) << "Remove listener failed, error: "
                       << apache::thrift::util::enumNameSafe(ret);
        }
        handleErrorCode(ret);
        onFinished();
        return;
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::listenerLock());
    std::vector<std::string> keys;
    const auto& prefix = MetaServiceUtils::listenerPrefix(space, type);
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "Remove listener failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
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
    const auto& prefix = MetaServiceUtils::listenerPrefix(space);
    auto iterRet = doPrefix(prefix);
    if (!nebula::ok(iterRet)) {
        auto retCode = nebula::error(iterRet);
        LOG(ERROR) << "List listener failed, error: "
                   << apache::thrift::util::enumNameSafe(retCode);
        handleErrorCode(retCode);
        onFinished();
        return;
    }

    auto activeHostsRet = ActiveHostsMan::getActiveHosts(
        kvstore_,
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
        listener.set_type(MetaServiceUtils::parseListenerType(iter->key()));
        listener.set_host(MetaServiceUtils::deserializeHostAddr(iter->val()));
        listener.set_part_id(MetaServiceUtils::parseListenerPart(iter->key()));
        if (std::find(activeHosts.begin(), activeHosts.end(), *listener.host_ref())
                != activeHosts.end()) {
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
