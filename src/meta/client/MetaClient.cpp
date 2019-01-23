/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/client/MetaClient.h"
#include "thrift/ThriftClientManager.h"

DEFINE_int32(load_data_interval_second, 2 * 60, "Load data interval, unit: second");

namespace nebula {
namespace meta {

void MetaClient::init() {
    CHECK(loadDataThread_.start());
    size_t delayMS = 100 + folly::Random::rand32(900);
    loadDataThread_.addTimerTask(delayMS,
                                 FLAGS_load_data_interval_second * 1000,
                                 &MetaClient::loadDataThreadFunc, this);
}

void MetaClient::loadDataThreadFunc() {
    auto ret = listSpaces();
    if (!ret.ok()) {
        LOG(ERROR) << "List space failed!";
        return;
    }
    decltype(localCache_) cache;
    decltype(spaceIndexByName_) indexByName;
    for (auto space : ret.value()) {
        auto spaceId = space.first;
        auto r = getPartsAlloc(spaceId);
        if (!r.ok()) {
            LOG(ERROR) << "Get parts allocaction failed for spaceId " << spaceId;
            return;
        }
        auto spaceCache = std::make_shared<SpaceInfoCache>();
        auto partsAlloc = r.value();
        spaceCache->spaceName = space.second;
        spaceCache->partsOnHost_ = revert(partsAlloc);
        spaceCache->partsAlloc_ = std::move(partsAlloc);
        VLOG(1) << "Load space " << spaceId << ", parts num:" << spaceCache->partsAlloc_.size();
        cache.emplace(spaceId, spaceCache);
        indexByName.emplace(space.second, spaceId);
    }
    {
        folly::RWSpinLock::WriteHolder holder(localCacheLock_);
        localCache_ = std::move(cache);
        spaceIndexByName_ = std::move(indexByName);
    }
    LOG(INFO) << "Load data completed!";
}

std::unordered_map<HostAddr, std::vector<PartitionID>>
MetaClient::revert(const PartsAlloc& parts) {
    std::unordered_map<HostAddr, std::vector<PartitionID>> hosts;
    for (auto& partHost : parts) {
        for (auto& h : partHost.second) {
            hosts[h].emplace_back(partHost.first);
        }
    }
    return hosts;
}

template<typename Request, typename RemoteFunc, typename Response>
Response MetaClient::collectResponse(Request req,
                                     RemoteFunc remoteFunc) {
    folly::Promise<Response> pro;
    auto f = pro.getFuture();
    auto* evb = ioThreadPool_->getEventBase();
    auto client = thrift::ThriftClientManager<meta::cpp2::MetaServiceAsyncClient>
                         ::getClient(active_, evb);
    remoteFunc(client, std::move(req)).then(evb, [&](folly::Try<Response>&& resp) {
        pro.setValue(resp.value());
    });
    return std::move(f).get();
}

StatusOr<GraphSpaceID>
MetaClient::createSpace(std::string name, int32_t partsNum, int32_t replicaFactor) {
    cpp2::CreateSpaceReq req;
    req.set_space_name(std::move(name));
    req.set_parts_num(partsNum);
    req.set_replica_factor(replicaFactor);
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_createSpace(request);
                });
    if (resp.code != cpp2::ErrorCode::SUCCEEDED) {
        return handleResponse(resp);
    }
    return resp.get_id().get_space_id();
}

StatusOr<std::vector<SpaceIdName>> MetaClient::listSpaces() {
    cpp2::ListSpacesReq req;
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_listSpaces(request);
                });
    if (resp.code != cpp2::ErrorCode::SUCCEEDED) {
        return handleResponse(resp);
    }
    return toSpaceIdName(resp.get_spaces());
}

Status MetaClient::addHosts(const std::vector<HostAddr>& hosts) {
    std::vector<nebula::cpp2::HostAddr> thriftHosts;
    thriftHosts.resize(hosts.size());
    std::transform(hosts.begin(), hosts.end(), thriftHosts.begin(), [](const auto& h) {
        nebula::cpp2::HostAddr th;
        th.set_ip(h.first);
        th.set_port(h.second);
        return th;
    });
    cpp2::AddHostsReq req;
    req.set_hosts(std::move(thriftHosts));
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_addHosts(request);
                });
    return handleResponse(resp);
}

StatusOr<std::vector<HostAddr>> MetaClient::listHosts() {
    cpp2::ListHostsReq req;
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_listHosts(request);
                });
    if (resp.code != cpp2::ErrorCode::SUCCEEDED) {
        return handleResponse(resp);
    }
    return to(resp.hosts);
}

StatusOr<std::unordered_map<PartitionID, std::vector<HostAddr>>>
MetaClient::getPartsAlloc(GraphSpaceID spaceId) {
    cpp2::GetPartsAllocReq req;
    req.set_space_id(spaceId);
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_getPartsAlloc(request);
                });
    if (resp.code != cpp2::ErrorCode::SUCCEEDED) {
        return handleResponse(resp);
    }
    std::unordered_map<PartitionID, std::vector<HostAddr>> parts;
    for (auto it = resp.parts.begin(); it != resp.parts.end(); it++) {
        parts.emplace(it->first, to(it->second));
    }
    return parts;
}

StatusOr<std::vector<PartitionID>>
MetaClient::getPartsFromCache(GraphSpaceID spaceId, const HostAddr& host) {
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = localCache_.find(spaceId);
    if (it == localCache_.end()) {
        return Status::Error("Can't find related spaceId");
    }
    auto hostIt = it->second->partsOnHost_.find(host);
    if (hostIt != it->second->partsOnHost_.end()) {
        return hostIt->second;
    }
    return Status::Error("Can't find any parts for the host");
}

StatusOr<std::vector<HostAddr>>
MetaClient::getHostsFromCache(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = localCache_.find(spaceId);
    if (it == localCache_.end()) {
        return Status::Error("Can't find related spaceId");
    }
    auto partIt = it->second->partsAlloc_.find(partId);
    if (partIt != it->second->partsAlloc_.end()) {
        return partIt->second;
    }
    return Status::Error("Can't find any hosts for the part");
}

StatusOr<GraphSpaceID>
MetaClient::getSpaceIdByNameFromCache(const std::string& name) {
    auto it = spaceIndexByName_.find(name);
    if (it != spaceIndexByName_.end()) {
        return it->second;
    }
    return Status::SpaceNotFound();
}

std::vector<HostAddr> MetaClient::to(const std::vector<nebula::cpp2::HostAddr>& tHosts) {
    std::vector<HostAddr> hosts;
    hosts.resize(tHosts.size());
    std::transform(tHosts.begin(), tHosts.end(), hosts.begin(), [](const auto& h) {
        return HostAddr(h.get_ip(), h.get_port());
    });
    return hosts;
}

std::vector<SpaceIdName> MetaClient::toSpaceIdName(const std::vector<cpp2::IdName>& tIdNames) {
    std::vector<SpaceIdName> idNames;
    idNames.resize(tIdNames.size());
    std::transform(tIdNames.begin(), tIdNames.end(), idNames.begin(), [] (const auto& tin) {
        return SpaceIdName(tin.id.get_space_id(), tin.name);
    });
    return idNames;
}

template<typename RESP>
Status MetaClient::handleResponse(const RESP& resp) {
    switch (resp.get_code()) {
        case cpp2::ErrorCode::SUCCEEDED:
            return Status::OK();
        case cpp2::ErrorCode::E_SPACE_EXISTED:
            return Status::Error("space existed!");
        case cpp2::ErrorCode::E_LEADER_CHANGED:
            return Status::Error("Leader changed!");
        default:
            return Status::Error("Unknown code %d", static_cast<int32_t>(resp.get_code()));
    }
}

}  // namespace meta
}  // namespace nebula
