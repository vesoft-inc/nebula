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
    if (ret != Status::ListenerNotFound()) {
        handleErrorCode(cpp2::ErrorCode::E_EXISTED);
        onFinished();
        return;
    }
    // TODO : (sky) if type is elasticsearch, need check text search service.
    folly::SharedMutex::WriteHolder wHolder(LockUtils::listenerLock());
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::partPrefix(space);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "List Parts Failed: No parts";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    std::vector<PartitionID> parts;
    while (iter->valid()) {
        parts.emplace_back(MetaServiceUtils::parsePartKeyPartId(iter->key()));
        iter->next();
    }
    std::vector<kvstore::KV> data;
    for (size_t i = 0; i < parts.size(); i++) {
        data.emplace_back(MetaServiceUtils::listenerKey(space, parts[i], type),
                          MetaServiceUtils::serializeHostAddr(hosts[i%hosts.size()]));
    }
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doSyncPutAndUpdate(std::move(data));
}

void RemoveListenerProcessor::process(const cpp2::RemoveListenerReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);
    auto type = req.get_type();
    auto ret = listenerExist(space, type);
    if (ret != Status::OK()) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    folly::SharedMutex::WriteHolder wHolder(LockUtils::listenerLock());
    std::vector<std::string> keys;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = MetaServiceUtils::listenerPrefix(space, type);
    auto listenerRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (listenerRet != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }
    while (iter->valid()) {
        keys.emplace_back(iter->key());
        iter->next();
    }
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    doSyncMultiRemoveAndUpdate({std::move(keys)});
}

void ListListenerProcessor::process(const cpp2::ListListenerReq& req) {
    auto space = req.get_space_id();
    CHECK_SPACE_ID_AND_RETURN(space);
    folly::SharedMutex::ReadHolder rHolder(LockUtils::listenerLock());
    std::unique_ptr<kvstore::KVIterator> iter;
    std::string prefix = MetaServiceUtils::listenerPrefix(space);
    auto ret = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Can't find any listener.";
        handleErrorCode(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto activeHosts = ActiveHostsMan::getActiveHosts(
        kvstore_,
        FLAGS_heartbeat_interval_secs * FLAGS_expired_time_factor,
        cpp2::HostRole::LISTENER);
    decltype(resp_.listeners) listeners;
    while (iter->valid()) {
        cpp2::ListenerInfo listener;
        listener.set_type(MetaServiceUtils::parseListenerType(iter->key()));
        listener.set_host(MetaServiceUtils::deserializeHostAddr(iter->val()));
        listener.set_part_id(MetaServiceUtils::parseListenerPart(iter->key()));
        if (std::find(activeHosts.begin(), activeHosts.end(), listener.host) != activeHosts.end()) {
            listener.set_status(cpp2::HostStatus::ONLINE);
        } else {
            listener.set_status(cpp2::HostStatus::OFFLINE);
        }
        listeners.emplace_back(std::move(listener));
        iter->next();
    }
    resp_.set_listeners(std::move(listeners));
    handleErrorCode(cpp2::ErrorCode::SUCCEEDED);
    onFinished();
}
}  // namespace meta
}  // namespace nebula
