/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/client/MetaClient.h"
#include "network/NetworkUtils.h"

DEFINE_int32(load_data_interval_second, 2 * 60, "Load data interval, unit: second");
DEFINE_string(meta_server_addrs, "", "list of meta server addresses,"
                                     "the format looks like ip1:port1, ip2:port2, ip3:port3");
DEFINE_int32(meta_client_io_threads, 3, "meta client io threads");

namespace nebula {
namespace meta {

MetaClient::MetaClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                       std::vector<HostAddr> addrs)
    : ioThreadPool_(ioThreadPool)
    , addrs_(std::move(addrs)) {
    if (ioThreadPool_ == nullptr) {
        ioThreadPool_
            = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_meta_client_io_threads);
    }
    if (addrs_.empty() && !FLAGS_meta_server_addrs.empty()) {
        addrs_ = network::NetworkUtils::toHosts(FLAGS_meta_server_addrs);
    }
    CHECK(!addrs_.empty());
    clientsMan_ = std::make_shared<thrift::ThriftClientManager<
                                    meta::cpp2::MetaServiceAsyncClient>>();
    updateActiveHost();
    loadDataThreadFunc();
}

 MetaClient::~MetaClient() {
    loadDataThread_.stop();
    loadDataThread_.wait();
    VLOG(3) << "~MetaClient";
}

void MetaClient::init() {
    CHECK(loadDataThread_.start());
    size_t delayMS = FLAGS_load_data_interval_second * 1000 + folly::Random::rand32(900);
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
        spaceCache->partsOnHost_ = reverse(partsAlloc);
        spaceCache->partsAlloc_ = std::move(partsAlloc);
        VLOG(3) << "Load space " << spaceId << ", parts num:" << spaceCache->partsAlloc_.size();
        cache.emplace(spaceId, spaceCache);
        indexByName.emplace(space.second, spaceId);
    }
    diff(cache);
    {
        folly::RWSpinLock::WriteHolder holder(localCacheLock_);
        localCache_ = std::move(cache);
        spaceIndexByName_ = std::move(indexByName);
    }
    LOG(INFO) << "Load data completed!";
}

std::unordered_map<HostAddr, std::vector<PartitionID>>
MetaClient::reverse(const PartsAlloc& parts) {
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
    auto client = clientsMan_->client(active_, evb);
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

StatusOr<GraphSpaceID>
MetaClient::getSpaceIdByNameFromCache(const std::string& name) {
    auto it = spaceIndexByName_.find(name);
    if (it != spaceIndexByName_.end()) {
        return it->second;
    }
    return Status::SpaceNotFound();
}

StatusOr<bool>
MetaClient::put(const std::string& key, const std::string& value) {
    cpp2::PutReq req;
    req.set_key(std::move(key));
    req.set_value(std::move(value));
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                   return client->future_put(request);
                });
    if (resp.code != cpp2::ErrorCode::SUCCEEDED) {
        return handleResponse(resp);
    }
    return true;
}

StatusOr<bool>
MetaClient::multiPut(const std::vector<std::pair<std::string, std::string>>& pairs) {
    cpp2::MultiPutReq req;
    std::vector<cpp2::Pair> data;
    for (auto element : pairs) {
        data.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                          element.first, element.second);
    }
    req.set_pairs(std::move(data));
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_multiPut(request);
                });
    if (resp.code != cpp2::ErrorCode::SUCCEEDED) {
        return handleResponse(resp);
    }
    return true;
}

StatusOr<std::string>
MetaClient::get(const std::string& key) {
    cpp2::GetReq req;
    req.set_key(std::move(key));
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_get(request);
                });
    if (resp.code != cpp2::ErrorCode::SUCCEEDED) {
        return handleResponse(resp);
    }
    return resp.get_value();
}

StatusOr<std::vector<std::string>>
MetaClient::multiGet(const std::vector<std::string>& keys) {
    cpp2::MultiGetReq req;
    req.set_keys(keys);
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_multiGet(request);
                });
    if (resp.code != cpp2::ErrorCode::SUCCEEDED) {
        return handleResponse(resp);
    }
    return resp.get_values();
}

StatusOr<std::vector<std::string>>
MetaClient::scan(const std::string& start, const std::string& end) {
    cpp2::ScanReq req;
    req.set_start(std::move(start));
    req.set_end(std::move(end));
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_scan(request);
                });
    if (resp.code != cpp2::ErrorCode::SUCCEEDED) {
        return handleResponse(resp);
    }
    return resp.get_values();
}

StatusOr<bool>
MetaClient::remove(const std::string& key) {
    cpp2::RemoveReq req;
    req.set_key(std::move(key));
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_remove(request);
                });
    if (resp.code != cpp2::ErrorCode::SUCCEEDED) {
        return handleResponse(resp);
    }
    return true;
}

StatusOr<bool>
MetaClient::removeRange(const std::string& start, const std::string& end) {
    cpp2::RemoveRangeReq req;
    req.set_start(std::move(start));
    req.set_end(std::move(end));
    auto resp = collectResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_removeRange(request);
                });
    if (resp.code != cpp2::ErrorCode::SUCCEEDED) {
        return handleResponse(resp);
    }
    return true;
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

PartsMap MetaClient::doGetPartsMap(const HostAddr& host,
                                   const std::unordered_map<
                                                GraphSpaceID,
                                                std::shared_ptr<SpaceInfoCache>>& localCache) {
    PartsMap partMap;
    for (auto it = localCache.begin(); it != localCache.end(); it++) {
        auto spaceId = it->first;
        auto& cache = it->second;
        auto partsIt = cache->partsOnHost_.find(host);
        if (partsIt != cache->partsOnHost_.end()) {
            for (auto& partId : partsIt->second) {
                auto partAllocIter = cache->partsAlloc_.find(partId);
                CHECK(partAllocIter != cache->partsAlloc_.end());
                auto& partM = partMap[spaceId][partId];
                partM.spaceId_ = spaceId;
                partM.partId_  = partId;
                partM.peers_   = partAllocIter->second;
            }
        }
    }
    return partMap;
}
PartsMap MetaClient::getPartsMapFromCache(const HostAddr& host) {
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    return doGetPartsMap(host, localCache_);
}

PartMeta MetaClient::getPartMetaFromCache(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = localCache_.find(spaceId);
    CHECK(it != localCache_.end());
    auto& cache = it->second;
    auto partAllocIter = cache->partsAlloc_.find(partId);
    CHECK(partAllocIter != cache->partsAlloc_.end());
    PartMeta pm;
    pm.spaceId_ = spaceId;
    pm.partId_  = partId;
    pm.peers_   = partAllocIter->second;
    return pm;
}

bool MetaClient::checkPartExistInCache(const HostAddr& host,
                                       GraphSpaceID spaceId,
                                       PartitionID partId) {
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = localCache_.find(spaceId);
    if (it != localCache_.end()) {
        auto partsIt = it->second->partsOnHost_.find(host);
        if (partsIt != it->second->partsOnHost_.end()) {
            for (auto& pId : partsIt->second) {
                if (pId == partId) {
                    return true;
                }
            }
        }
    }
    return false;
}

bool MetaClient::checkSpaceExistInCache(const HostAddr& host,
                                        GraphSpaceID spaceId) {
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = localCache_.find(spaceId);
    if (it != localCache_.end()) {
        auto partsIt = it->second->partsOnHost_.find(host);
        if (partsIt != it->second->partsOnHost_.end() && !partsIt->second.empty()) {
            return true;
        }
    }
    return false;
}

int32_t MetaClient::partsNum(GraphSpaceID spaceId) {
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = localCache_.find(spaceId);
    CHECK(it != localCache_.end());
    return it->second->partsAlloc_.size();
}

void MetaClient::diff(const std::unordered_map<GraphSpaceID,
                                               std::shared_ptr<SpaceInfoCache>>& newCache) {
    if (listener_ == nullptr) {
        return;
    }
    auto localHost = listener_->getLocalHost();
    auto newPartsMap = doGetPartsMap(localHost, newCache);
    auto oldPartsMap = getPartsMapFromCache(localHost);
    VLOG(1) << "Let's check if any new parts added/updated....";
    for (auto it = newPartsMap.begin(); it != newPartsMap.end(); it++) {
        auto spaceId = it->first;
        const auto& newParts = it->second;
        auto oldIt = oldPartsMap.find(spaceId);
        if (oldIt == oldPartsMap.end()) {
            VLOG(1) << "SpaceId " << spaceId << " was added!";
            listener_->onSpaceAdded(spaceId);
            for (auto partIt = newParts.begin(); partIt != newParts.end(); partIt++) {
                listener_->onPartAdded(partIt->second);
            }
        } else {
            const auto& oldParts = oldIt->second;
            for (auto partIt = newParts.begin(); partIt != newParts.end(); partIt++) {
                auto oldPartIt = oldParts.find(partIt->first);
                if (oldPartIt == oldParts.end()) {
                    VLOG(1) << "SpaceId " << spaceId << ", partId "
                            << partIt->first << " was added!";
                    listener_->onPartAdded(partIt->second);
                } else {
                    const auto& oldPartMeta = oldPartIt->second;
                    const auto& newPartMeta = partIt->second;
                    if (oldPartMeta != newPartMeta) {
                        VLOG(1) << "SpaceId " << spaceId
                                << ", partId " << partIt->first << " was updated!";
                        listener_->onPartUpdated(newPartMeta);
                    }
                }
            }
        }
    }
    VLOG(1) << "Let's check if any old parts removed....";
    for (auto it = oldPartsMap.begin(); it != oldPartsMap.end(); it++) {
        auto spaceId = it->first;
        const auto& oldParts = it->second;
        auto newIt = newPartsMap.find(spaceId);
        if (newIt == newPartsMap.end()) {
            VLOG(1) << "SpaceId " << spaceId << " was removed!";
            for (auto partIt = oldParts.begin(); partIt != oldParts.end(); partIt++) {
                listener_->onPartRemoved(spaceId, partIt->first);
            }
            listener_->onSpaceRemoved(spaceId);
        } else {
            const auto& newParts = newIt->second;
            for (auto partIt = oldParts.begin(); partIt != oldParts.end(); partIt++) {
                auto newPartIt = newParts.find(partIt->first);
                if (newPartIt == newParts.end()) {
                    VLOG(1) << "SpaceId " << spaceId
                            << ", partId " << partIt->first << " was removed!";
                    listener_->onPartRemoved(spaceId, partIt->first);
                }
            }
        }
    }
}

}  // namespace meta
}  // namespace nebula
