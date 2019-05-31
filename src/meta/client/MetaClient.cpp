/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/common/MetaCommon.h"
#include "meta/client/MetaClient.h"
#include "network/NetworkUtils.h"
#include "meta/NebulaSchemaProvider.h"
#include "meta/ConfigManager.h"

DEFINE_int32(load_data_interval_secs, 2 * 60, "Load data interval");
DEFINE_int32(heartbeat_interval_secs, 10, "Heartbeat interval");
DEFINE_int32(load_config_interval_secs, 2 * 60, "Load config interval");

namespace nebula {
namespace meta {

MetaClient::MetaClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                       std::vector<HostAddr> addrs,
                       HostAddr localHost,
                       bool sendHeartBeat)
    : ioThreadPool_(ioThreadPool)
    , addrs_(std::move(addrs))
    , localHost_(localHost)
    , sendHeartBeat_(sendHeartBeat) {
    CHECK(ioThreadPool_ != nullptr) << "IOThreadPool is required";
    CHECK(!addrs_.empty())
        << "No meta server address is specified. Meta server is required";
    clientsMan_ = std::make_shared<
        thrift::ThriftClientManager<meta::cpp2::MetaServiceAsyncClient>
    >();
    updateHost();
    LOG(INFO) << "Create meta client to " << active_;
}


MetaClient::~MetaClient() {
    bgThread_.stop();
    bgThread_.wait();
    VLOG(3) << "~MetaClient";
}


bool MetaClient::isMetadReady() {
    if (sendHeartBeat_) {
        auto ret = heartbeat().get();
        if (!ret.ok()) {
            LOG(ERROR) << "Heartbeat failed, status:" << ret.status();
            ready_ = false;
            return ready_;
        }
    }  // end if
    loadDataThreadFunc();
    loadCfgThreadFunc();
    return ready_;
}


bool MetaClient::waitForMetadReady(int count, int retryIntervalSecs) {
    int tryCount = count;
    while (!isMetadReady() && ((count == -1) || (tryCount > 0))) {
        --tryCount;
        ::sleep(retryIntervalSecs);
    }  // end while

    CHECK(bgThread_.start());
    if (sendHeartBeat_) {
        LOG(INFO) << "Register time task for heartbeat!";
        size_t delayMS = FLAGS_heartbeat_interval_secs * 1000 + folly::Random::rand32(900);
        bgThread_.addTimerTask(delayMS,
                               FLAGS_heartbeat_interval_secs * 1000,
                               &MetaClient::heartBeatThreadFunc, this);
    }
    return true;
}


void MetaClient::heartBeatThreadFunc() {
    auto ret = heartbeat().get();
    if (!ret.ok()) {
        LOG(ERROR) << "Heartbeat failed, status:" << ret.status();
        return;
    }
}

void MetaClient::loadDataThreadFunc() {
    loadData();
    addLoadDataTask();
}

void MetaClient::loadData() {
    if (ioThreadPool_->numThreads() <= 0) {
        LOG(ERROR) << "The threads number in ioThreadPool should be greater than 0";
        return;
    }
    auto ret = listSpaces().get();
    if (!ret.ok()) {
        LOG(ERROR) << "List space failed, status:" << ret.status();
        return;
    }
    decltype(localCache_) cache;
    decltype(spaceIndexByName_) spaceIndexByName;
    decltype(spaceTagIndexByName_) spaceTagIndexByName;
    decltype(spaceEdgeIndexByName_) spaceEdgeIndexByName;
    decltype(spaceNewestTagVerMap_) spaceNewestTagVerMap;
    decltype(spaceNewestEdgeVerMap_) spaceNewestEdgeVerMap;

    for (auto space : ret.value()) {
        auto spaceId = space.first;
        auto r = getPartsAlloc(spaceId).get();
        if (!r.ok()) {
            LOG(ERROR) << "Get parts allocation failed for spaceId " << spaceId
                       << ", status " << r.status();
            return;
        }

        auto spaceCache = std::make_shared<SpaceInfoCache>();
        auto partsAlloc = r.value();
        spaceCache->spaceName = space.second;
        spaceCache->partsOnHost_ = reverse(partsAlloc);
        spaceCache->partsAlloc_ = std::move(partsAlloc);
        VLOG(2) << "Load space " << spaceId
                << ", parts num:" << spaceCache->partsAlloc_.size();

        // loadSchemas
        if (!loadSchemas(spaceId,
                         spaceCache,
                         spaceTagIndexByName,
                         spaceEdgeIndexByName,
                         spaceNewestTagVerMap,
                         spaceNewestEdgeVerMap)) {
            return;
        }

        cache.emplace(spaceId, spaceCache);
        spaceIndexByName.emplace(space.second, spaceId);
    }
    decltype(localCache_) oldCache;
    {
        folly::RWSpinLock::WriteHolder holder(localCacheLock_);
        oldCache = std::move(localCache_);
        localCache_ = std::move(cache);
        spaceIndexByName_ = std::move(spaceIndexByName);
        spaceTagIndexByName_ = std::move(spaceTagIndexByName);
        spaceEdgeIndexByName_ = std::move(spaceEdgeIndexByName);
        spaceNewestTagVerMap_ = std::move(spaceNewestTagVerMap);
        spaceNewestEdgeVerMap_ = std::move(spaceNewestEdgeVerMap);
    }
    diff(oldCache, localCache_);
    ready_ = true;
    LOG(INFO) << "Load data completed!";
}

void MetaClient::addLoadDataTask() {
    size_t delayMS = FLAGS_load_data_interval_secs * 1000;
    auto value = GET_CONFIG_INT("", cpp2::ConfigModule::META, "load_data_interval_secs");
    if (value.ok()) {
        delayMS = value.value() * 1000;
    }
    bgThread_.addDelayTask(delayMS + folly::Random::rand32(900),
                           &MetaClient::loadDataThreadFunc, this);
}


bool MetaClient::loadSchemas(GraphSpaceID spaceId,
                             std::shared_ptr<SpaceInfoCache> spaceInfoCache,
                             SpaceTagNameIdMap &tagNameIdMap,
                             SpaceEdgeNameTypeMap &edgeNameTypeMap,
                             SpaceNewestTagVerMap &newestTagVerMap,
                             SpaceNewestEdgeVerMap &newestEdgeVerMap) {
    auto tagRet = listTagSchemas(spaceId).get();
    if (!tagRet.ok()) {
        LOG(ERROR) << "Get tag schemas failed for spaceId " << spaceId << ", " << tagRet.status();
        return false;
    }

    auto edgeRet = listEdgeSchemas(spaceId).get();
    if (!edgeRet.ok()) {
        LOG(ERROR) << "Get edge schemas failed for spaceId " << spaceId << ", " << edgeRet.status();
        return false;
    }

    auto tagItemVec = tagRet.value();
    auto edgeItemVec = edgeRet.value();
    TagIDSchemas tagIdSchemas;
    EdgeTypeSchemas edgeTypeSchemas;
    for (auto& tagIt : tagItemVec) {
        std::shared_ptr<NebulaSchemaProvider> schema(new NebulaSchemaProvider(tagIt.version));
        for (auto colIt : tagIt.schema.get_columns()) {
            schema->addField(colIt.name, std::move(colIt.type));
        }
        // handle schema property
        schema->setProp(tagIt.schema.get_schema_prop());
        tagIdSchemas.emplace(std::make_pair(tagIt.tag_id, tagIt.version), schema);
        tagNameIdMap.emplace(std::make_pair(spaceId, tagIt.tag_name), tagIt.tag_id);
        // get the latest tag version
        auto it = newestTagVerMap.find(std::make_pair(spaceId, tagIt.tag_id));
        if (it != newestTagVerMap.end()) {
            if (it->second < tagIt.version) {
                it->second = tagIt.version;
            }
        } else {
            newestTagVerMap.emplace(std::make_pair(spaceId, tagIt.tag_id), tagIt.version);
        }
        VLOG(3) << "Load Tag Schema Space " << spaceId << ", ID " << tagIt.tag_id
                << ", Name " << tagIt.tag_name << ", Version " << tagIt.version << " Successfully!";
    }

    for (auto& edgeIt : edgeItemVec) {
        std::shared_ptr<NebulaSchemaProvider> schema(new NebulaSchemaProvider(edgeIt.version));
        for (auto colIt : edgeIt.schema.get_columns()) {
            schema->addField(colIt.name, std::move(colIt.type));
        }
        // handle shcem property
        schema->setProp(edgeIt.schema.get_schema_prop());
        edgeTypeSchemas.emplace(std::make_pair(edgeIt.edge_type, edgeIt.version), schema);
        edgeNameTypeMap.emplace(std::make_pair(spaceId, edgeIt.edge_name), edgeIt.edge_type);
        // get the latest edge version
        auto it = newestEdgeVerMap.find(std::make_pair(spaceId, edgeIt.edge_type));
        if (it != newestEdgeVerMap.end()) {
            if (it->second < edgeIt.version) {
                it->second = edgeIt.version;
            }
        } else {
            newestEdgeVerMap.emplace(std::make_pair(spaceId, edgeIt.edge_type), edgeIt.version);
        }
        VLOG(3) << "Load Edge Schema Space " << spaceId << ", Type " << edgeIt.edge_type
                << ", Name " << edgeIt.edge_name << ", Version " << edgeIt.version
                << " Successfully!";
    }

    spaceInfoCache->tagSchemas_ = std::move(tagIdSchemas);
    spaceInfoCache->edgeSchemas_ = std::move(edgeTypeSchemas);
    return true;
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


template<typename Request,
         typename RemoteFunc,
         typename RespGenerator,
         typename RpcResponse,
         typename Response>
folly::Future<StatusOr<Response>> MetaClient::getResponse(
                                     Request req,
                                     RemoteFunc remoteFunc,
                                     RespGenerator respGen,
                                     bool toLeader) {
    folly::Promise<StatusOr<Response>> pro;
    auto f = pro.getFuture();
    auto* evb = ioThreadPool_->getEventBase();
    HostAddr host;
    {
        folly::RWSpinLock::ReadHolder holder(&hostLock_);
        host = toLeader ? leader_ : active_;
    }
    auto client = clientsMan_->client(host, evb);
    remoteFunc(client, std::move(req))
         .then(evb, [p = std::move(pro), respGen, this] (folly::Try<RpcResponse>&& t) mutable {
        // exception occurred during RPC
        if (t.hasException()) {
            p.setValue(Status::Error(folly::stringPrintf("RPC failure in MetaClient: %s",
                                                         t.exception().what().c_str())));
            updateHost();
            return;
        }
        // errored
        auto&& resp = t.value();
        if (resp.code != cpp2::ErrorCode::SUCCEEDED) {
            p.setValue(this->handleResponse(resp));
            return;
        }
        // succeeded
        p.setValue(respGen(std::move(resp)));
    });
    return f;
}


std::vector<HostAddr> MetaClient::to(const std::vector<nebula::cpp2::HostAddr>& tHosts) {
    std::vector<HostAddr> hosts;
    hosts.resize(tHosts.size());
    std::transform(tHosts.begin(), tHosts.end(), hosts.begin(), [](const auto& h) {
        return HostAddr(h.get_ip(), h.get_port());
    });
    return hosts;
}

std::vector<HostStatus> MetaClient::toHostStatus(const std::vector<cpp2::HostItem>& tHosts) {
    std::vector<HostStatus> hosts;
    hosts.resize(tHosts.size());
    std::transform(tHosts.begin(), tHosts.end(), hosts.begin(), [](const auto& h) {
        switch (h.get_status()) {
            case cpp2::HostStatus::ONLINE:
                return HostStatus(HostAddr(h.hostAddr.get_ip(), h.hostAddr.get_port()), "online");
            case cpp2::HostStatus::OFFLINE:
                return HostStatus(HostAddr(h.hostAddr.get_ip(), h.hostAddr.get_port()), "offline");
            default:
                return HostStatus(HostAddr(h.hostAddr.get_ip(), h.hostAddr.get_port()), "unknown");
        }
    });
    return hosts;
}

std::vector<SpaceIdName> MetaClient::toSpaceIdName(const std::vector<cpp2::IdName>& tIdNames) {
    std::vector<SpaceIdName> idNames;
    idNames.resize(tIdNames.size());
    std::transform(tIdNames.begin(), tIdNames.end(), idNames.begin(), [](const auto& tin) {
        return SpaceIdName(tin.id.get_space_id(), tin.name);
    });
    return idNames;
}


template<typename RESP>
Status MetaClient::handleResponse(const RESP& resp) {
    switch (resp.get_code()) {
        case cpp2::ErrorCode::SUCCEEDED:
            return Status::OK();
        case cpp2::ErrorCode::E_EXISTED:
            return Status::Error("existed!");
        case cpp2::ErrorCode::E_NOT_FOUND:
            return Status::Error("not existed!");
        case cpp2::ErrorCode::E_NO_HOSTS:
            return Status::Error("no hosts!");
        case cpp2::ErrorCode::E_LEADER_CHANGED: {
            HostAddr leader(resp.get_leader().get_ip(), resp.get_leader().get_port());
            {
                folly::RWSpinLock::WriteHolder holder(hostLock_);
                leader_ = leader;
            }
            return Status::Error("Leader changed!");
        }
        default:
            return Status::Error("Unknown code %d", static_cast<int32_t>(resp.get_code()));
    }
}


PartsMap MetaClient::doGetPartsMap(const HostAddr& host,
                                   const LocalCache& localCache) {
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


void MetaClient::diff(const LocalCache& oldCache, const LocalCache& newCache) {
    folly::RWSpinLock::WriteHolder holder(listenerLock_);
    if (listener_ == nullptr) {
        VLOG(3) << "Listener is null!";
        return;
    }
    auto newPartsMap = doGetPartsMap(localHost_, newCache);
    auto oldPartsMap = doGetPartsMap(localHost_, oldCache);
    VLOG(1) << "Let's check if any new parts added/updated for " << localHost_;
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


/// ================================== public methods =================================

folly::Future<StatusOr<GraphSpaceID>>
MetaClient::createSpace(std::string name, int32_t partsNum, int32_t replicaFactor) {
    cpp2::SpaceProperties properties;
    properties.set_space_name(std::move(name));
    properties.set_partition_num(partsNum);
    properties.set_replica_factor(replicaFactor);
    cpp2::CreateSpaceReq req;
    req.set_properties(std::move(properties));
    return getResponse(std::move(req), [] (auto client, auto request) {
                return client->future_createSpace(request);
            }, [] (cpp2::ExecResp&& resp) -> GraphSpaceID {
                return resp.get_id().get_space_id();
            }, true);
}


folly::Future<StatusOr<std::vector<SpaceIdName>>> MetaClient::listSpaces() {
    cpp2::ListSpacesReq req;
    return getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_listSpaces(request);
                }, [this] (cpp2::ListSpacesResp&& resp) -> decltype(auto) {
                    return this->toSpaceIdName(resp.get_spaces());
                });
}

folly::Future<StatusOr<cpp2::SpaceItem>>
MetaClient::getSpace(std::string name) {
    cpp2::GetSpaceReq req;
    req.set_space_name(std::move(name));
    return  getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_getSpace(request);
                }, [] (cpp2::GetSpaceResp&& resp) -> decltype(auto) {
                    return std::move(resp).get_item();
                });
}

folly::Future<StatusOr<bool>> MetaClient::dropSpace(std::string name) {
    cpp2::DropSpaceReq req;
    req.set_space_name(std::move(name));
    return getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_dropSpace(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, true);
}


folly::Future<StatusOr<bool>> MetaClient::addHosts(const std::vector<HostAddr>& hosts) {
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
    return getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_addHosts(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, true);
}

folly::Future<StatusOr<std::vector<HostStatus>>> MetaClient::listHosts() {
    cpp2::ListHostsReq req;
    return getResponse(std::move(req), [] (auto client, auto request) {
                return client->future_listHosts(request);
            }, [this] (cpp2::ListHostsResp&& resp) -> decltype(auto) {
                return this->toHostStatus(resp.hosts);
            });
}


folly::Future<StatusOr<bool>> MetaClient::removeHosts(const std::vector<HostAddr>& hosts) {
    std::vector<nebula::cpp2::HostAddr> thriftHosts;
    thriftHosts.resize(hosts.size());
    std::transform(hosts.begin(), hosts.end(), thriftHosts.begin(), [](const auto& h) {
        nebula::cpp2::HostAddr th;
        th.set_ip(h.first);
        th.set_port(h.second);
        return th;
    });
    cpp2::RemoveHostsReq req;
    req.set_hosts(std::move(thriftHosts));
    return getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_removeHosts(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, true);
}


folly::Future<StatusOr<std::unordered_map<PartitionID, std::vector<HostAddr>>>>
MetaClient::getPartsAlloc(GraphSpaceID spaceId) {
    cpp2::GetPartsAllocReq req;
    req.set_space_id(spaceId);
    return getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_getPartsAlloc(request);
                }, [this] (cpp2::GetPartsAllocResp&& resp) -> decltype(auto) {
                    std::unordered_map<PartitionID, std::vector<HostAddr>> parts;
                    for (auto it = resp.parts.begin(); it != resp.parts.end(); it++) {
                        parts.emplace(it->first, to(it->second));
                    }
                    return parts;
                });
}


StatusOr<GraphSpaceID>
MetaClient::getSpaceIdByNameFromCache(const std::string& name) {
    if (!ready_) {
        return Status::Error("Not ready!");
    }
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = spaceIndexByName_.find(name);
    if (it != spaceIndexByName_.end()) {
        return it->second;
    }
    return Status::SpaceNotFound();
}


StatusOr<TagID> MetaClient::getTagIDByNameFromCache(const GraphSpaceID& space,
                                                    const std::string& name) {
    if (!ready_) {
        return Status::Error("Not ready!");
    }
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = spaceTagIndexByName_.find(make_pair(space, name));
    if (it == spaceTagIndexByName_.end()) {
        return Status::Error("Tag is not exist!");
    }
    return it->second;
}


StatusOr<EdgeType> MetaClient::getEdgeTypeByNameFromCache(const GraphSpaceID& space,
                                                          const std::string& name) {
    if (!ready_) {
        return Status::Error("Not ready!");
    }
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = spaceEdgeIndexByName_.find(make_pair(space, name));
    if (it == spaceEdgeIndexByName_.end()) {
        return Status::Error("Edge is no exist!");
    }
    return it->second;
}


folly::Future<StatusOr<bool>>
MetaClient::multiPut(std::string segment,
                     std::vector<std::pair<std::string, std::string>> pairs) {
    if (!nebula::meta::MetaCommon::checkSegment(segment)
            || pairs.empty()) {
        return Status::Error("arguments invalid!");
    }
    cpp2::MultiPutReq req;
    std::vector<cpp2::Pair> data;
    for (auto& element : pairs) {
        data.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                          std::move(element.first), std::move(element.second));
    }
    req.set_segment(std::move(segment));
    req.set_pairs(std::move(data));
    return getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_multiPut(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, true);
}


folly::Future<StatusOr<std::string>>
MetaClient::get(std::string segment, std::string key) {
    if (!nebula::meta::MetaCommon::checkSegment(segment)
            || key.empty()) {
        return Status::Error("arguments invalid!");
    }
    cpp2::GetReq req;
    req.set_segment(std::move(segment));
    req.set_key(std::move(key));
    return getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_get(request);
                }, [] (cpp2::GetResp&& resp) -> std::string {
                    return resp.get_value();
                });
}


folly::Future<StatusOr<std::vector<std::string>>>
MetaClient::multiGet(std::string segment, std::vector<std::string> keys) {
    if (!nebula::meta::MetaCommon::checkSegment(segment)
            || keys.empty()) {
        return Status::Error("arguments invalid!");
    }
    cpp2::MultiGetReq req;
    req.set_segment(std::move(segment));
    req.set_keys(std::move(keys));
    return getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_multiGet(request);
                }, [] (cpp2::MultiGetResp&& resp) -> std::vector<std::string> {
                    return resp.get_values();
                });
}


folly::Future<StatusOr<std::vector<std::string>>>
MetaClient::scan(std::string segment, std::string start, std::string end) {
    if (!nebula::meta::MetaCommon::checkSegment(segment)
            || start.empty() || end.empty()) {
        return Status::Error("arguments invalid!");
    }
    cpp2::ScanReq req;
    req.set_segment(std::move(segment));
    req.set_start(std::move(start));
    req.set_end(std::move(end));
    return getResponse(std::move(req), [] (auto client, auto request) {
                return client->future_scan(request);
            }, [] (cpp2::ScanResp&& resp) -> std::vector<std::string> {
                return resp.get_values();
            });
}


folly::Future<StatusOr<bool>>
MetaClient::remove(std::string segment, std::string key) {
    if (!nebula::meta::MetaCommon::checkSegment(segment)
            || key.empty()) {
        return Status::Error("arguments invalid!");
    }
    cpp2::RemoveReq req;
    req.set_segment(std::move(segment));
    req.set_key(std::move(key));
    return getResponse(std::move(req), [] (auto client, auto request) {
                return client->future_remove(request);
            }, [] (cpp2::ExecResp&& resp) -> bool {
                return resp.code == cpp2::ErrorCode::SUCCEEDED;
            }, true);
}


folly::Future<StatusOr<bool>>
MetaClient::removeRange(std::string segment, std::string start, std::string end) {
    if (!nebula::meta::MetaCommon::checkSegment(segment)
            || start.empty() || end.empty()) {
        return Status::Error("arguments invalid!");
    }
    cpp2::RemoveRangeReq req;
    req.set_segment(std::move(segment));
    req.set_start(std::move(start));
    req.set_end(std::move(end));
    return getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_removeRange(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, true);
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


folly::Future<StatusOr<TagID>>
MetaClient::createTagSchema(GraphSpaceID spaceId, std::string name, nebula::cpp2::Schema schema) {
    cpp2::CreateTagReq req;
    req.set_space_id(std::move(spaceId));
    req.set_tag_name(std::move(name));
    req.set_schema(std::move(schema));

    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_createTag(request);
    }, [] (cpp2::ExecResp&& resp) -> TagID {
        return resp.get_id().get_tag_id();
    }, true);
}


folly::Future<StatusOr<TagID>>
MetaClient::alterTagSchema(GraphSpaceID spaceId,
                           std::string name,
                           std::vector<cpp2::AlterSchemaItem> items,
                           nebula::cpp2::SchemaProp schemaProp) {
    cpp2::AlterTagReq req;
    req.set_space_id(std::move(spaceId));
    req.set_tag_name(std::move(name));
    req.set_tag_items(std::move(items));
    req.set_schema_prop(std::move(schemaProp));

    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_alterTag(request);
    }, [] (cpp2::ExecResp&& resp) -> TagID {
        return resp.get_id().get_tag_id();
    }, true);
}


folly::Future<StatusOr<std::vector<cpp2::TagItem>>>
MetaClient::listTagSchemas(GraphSpaceID spaceId) {
    cpp2::ListTagsReq req;
    req.set_space_id(std::move(spaceId));
    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_listTags(request);
    }, [] (cpp2::ListTagsResp&& resp) -> decltype(auto){
        return std::move(resp).get_tags();
    });
}


folly::Future<StatusOr<bool>>
MetaClient::dropTagSchema(int32_t spaceId, std::string tagName) {
    cpp2::DropTagReq req;
    req.set_space_id(spaceId);
    req.set_tag_name(std::move(tagName));
    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_dropTag(request);
    }, [] (cpp2::ExecResp&& resp) -> bool {
        return resp.code == cpp2::ErrorCode::SUCCEEDED;
    }, true);
}


folly::Future<StatusOr<nebula::cpp2::Schema>>
MetaClient::getTagSchema(int32_t spaceId, std::string name, int64_t version) {
    cpp2::GetTagReq req;
    req.set_space_id(spaceId);
    req.set_tag_name(std::move(name));
    req.set_version(version);
    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_getTag(request);
    }, [] (cpp2::GetTagResp&& resp) -> nebula::cpp2::Schema {
        return std::move(resp).get_schema();
    });
}


folly::Future<StatusOr<EdgeType>>
MetaClient::createEdgeSchema(GraphSpaceID spaceId, std::string name, nebula::cpp2::Schema schema) {
    cpp2::CreateEdgeReq req;
    req.set_space_id(std::move(spaceId));
    req.set_edge_name(std::move(name));
    req.set_schema(schema);
    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_createEdge(request);
    }, [] (cpp2::ExecResp&& resp) -> EdgeType {
        return resp.get_id().get_edge_type();
    }, true);
}


folly::Future<StatusOr<bool>>
MetaClient::alterEdgeSchema(GraphSpaceID spaceId,
                            std::string name,
                            std::vector<cpp2::AlterSchemaItem> items,
                            nebula::cpp2::SchemaProp schemaProp) {
    cpp2::AlterEdgeReq req;
    req.set_space_id(std::move(spaceId));
    req.set_edge_name(std::move(name));
    req.set_edge_items(std::move(items));
    req.set_schema_prop(std::move(schemaProp));

    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_alterEdge(request);
    }, [] (cpp2::ExecResp&& resp) -> bool {
        return resp.code == cpp2::ErrorCode::SUCCEEDED;
    }, true);
}


folly::Future<StatusOr<std::vector<cpp2::EdgeItem>>>
MetaClient::listEdgeSchemas(GraphSpaceID spaceId) {
    cpp2::ListEdgesReq req;
    req.set_space_id(std::move(spaceId));
    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_listEdges(request);
    }, [] (cpp2::ListEdgesResp&& resp) -> decltype(auto) {
        return std::move(resp).get_edges();
    });
}


folly::Future<StatusOr<nebula::cpp2::Schema>>
MetaClient::getEdgeSchema(GraphSpaceID spaceId, std::string name, SchemaVer version) {
    cpp2::GetEdgeReq req;
    req.set_space_id(std::move(spaceId));
    req.set_edge_name(std::move(name));
    req.set_version(version);
    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_getEdge(request);
    }, [] (cpp2::GetEdgeResp&& resp) -> nebula::cpp2::Schema {
        return std::move(resp).get_schema();
    });
}


folly::Future<StatusOr<bool>>
MetaClient::dropEdgeSchema(GraphSpaceID spaceId, std::string name) {
    cpp2::DropEdgeReq req;
    req.set_space_id(std::move(spaceId));
    req.set_edge_name(std::move(name));
    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_dropEdge(request);
    }, [] (cpp2::ExecResp&& resp) -> bool {
        return resp.code == cpp2::ErrorCode::SUCCEEDED;
    }, true);
}


StatusOr<std::shared_ptr<const SchemaProviderIf>>
MetaClient::getTagSchemaFromCache(GraphSpaceID spaceId, TagID tagID, SchemaVer ver) {
    if (!ready_) {
        return Status::Error("Not ready!");
    }
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto spaceIt = localCache_.find(spaceId);
    if (spaceIt == localCache_.end()) {
        // Not found
        return std::shared_ptr<const SchemaProviderIf>();
    } else {
        auto tagIt = spaceIt->second->tagSchemas_.find(std::make_pair(tagID, ver));
        if (tagIt == spaceIt->second->tagSchemas_.end()) {
            return std::shared_ptr<const SchemaProviderIf>();
        } else {
            return tagIt->second;
        }
    }
}


StatusOr<std::shared_ptr<const SchemaProviderIf>> MetaClient::getEdgeSchemaFromCache(
        GraphSpaceID spaceId, EdgeType edgeType, SchemaVer ver) {
    if (!ready_) {
        return Status::Error("Not ready!");
    }
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto spaceIt = localCache_.find(spaceId);
    if (spaceIt == localCache_.end()) {
        // Not found
        VLOG(3) << "Space " << spaceId << " not found!";
        return std::shared_ptr<const SchemaProviderIf>();
    } else {
        auto edgeIt = spaceIt->second->edgeSchemas_.find(std::make_pair(edgeType, ver));
        if (edgeIt == spaceIt->second->edgeSchemas_.end()) {
            VLOG(3) << "Space " << spaceId << ", EdgeType " << edgeType << ", version "
                    << ver << " not found!";
            return std::shared_ptr<const SchemaProviderIf>();
        } else {
            return edgeIt->second;
        }
    }
}

const std::vector<HostAddr>& MetaClient::getAddresses() {
    return addrs_;
}

StatusOr<SchemaVer> MetaClient::getNewestTagVerFromCache(const GraphSpaceID& space,
                                                         const TagID& tagId) {
    if (!ready_) {
        return Status::Error("Not ready!");
    }
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = spaceNewestTagVerMap_.find(std::make_pair(space, tagId));
    if (it == spaceNewestTagVerMap_.end()) {
        return -1;
    }
    return it->second;
}


StatusOr<SchemaVer> MetaClient::getNewestEdgeVerFromCache(const GraphSpaceID& space,
                                                          const EdgeType& edgeType) {
    if (!ready_) {
        return Status::Error("Not ready!");
    }
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = spaceNewestEdgeVerMap_.find(std::make_pair(space, edgeType));
    if (it == spaceNewestEdgeVerMap_.end()) {
        return -1;
    }
    return it->second;
}


folly::Future<StatusOr<bool>> MetaClient::heartbeat() {
    cpp2::HBReq req;
    nebula::cpp2::HostAddr thriftHost;
    thriftHost.set_ip(localHost_.first);
    thriftHost.set_port(localHost_.second);
    req.set_host(std::move(thriftHost));
    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_heartBeat(request);
    }, [] (cpp2::HBResp&& resp) -> bool {
        return resp.code == cpp2::ErrorCode::SUCCEEDED;
    }, true);
}

folly::Future<StatusOr<int64_t>> MetaClient::balance() {
    cpp2::BalanceReq req;
    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_balance(request);
    }, [] (cpp2::BalanceResp&& resp) -> int64_t {
        return resp.id;
    }, true);
}

folly::Future<StatusOr<ConfigItem>>
MetaClient::getConfig(const std::string& space, const cpp2::ConfigModule& module,
                      const std::string& name, const cpp2::ConfigType& type) {
    cpp2::ConfigItem item;
    item.set_space(space);
    item.set_module(module);
    item.set_name(name);
    item.set_type(type);

    cpp2::GetConfigReq req;
    req.set_item(item);
    return getResponse(std::move(req), [] (auto client, auto request) {
                return client->future_getConfig(request);
            }, [this] (cpp2::GetConfigResp&& resp) -> decltype(auto) {
                return toConfigItem(resp.get_item());
            });
}

folly::Future<StatusOr<bool>>
MetaClient::setConfig(const std::string& space, const cpp2::ConfigModule& module,
                      const std::string& name, const cpp2::ConfigType& type,
                      const std::string& value) {
    cpp2::ConfigItem item;
    item.set_space(space);
    item.set_module(module);
    item.set_name(name);
    item.set_type(type);
    item.set_value(value);

    cpp2::SetConfigReq req;
    req.set_item(item);
    return getResponse(std::move(req), [] (auto client, auto request) {
                return client->future_setConfig(request);
            }, [this] (cpp2::ExecResp&& resp) -> decltype(auto) {
                return resp.code == cpp2::ErrorCode::SUCCEEDED;
            });
}

folly::Future<StatusOr<std::vector<ConfigItem>>>
MetaClient::listConfigs(const std::string& space, const cpp2::ConfigModule& module) {
    cpp2::ListConfigsReq req;
    req.set_space(space);
    req.set_module(module);
    return getResponse(std::move(req), [] (auto client, auto request) {
                return client->future_listConfigs(request);
            }, [this] (cpp2::ListConfigsResp&& resp) -> decltype(auto) {
                std::vector<ConfigItem> configs;
                for (const auto& tItem : resp.get_items()) {
                    configs.emplace_back(toConfigItem(tItem));
                }
                return configs;
            });
}

ConfigItem MetaClient::toConfigItem(const cpp2::ConfigItem& item) {
    VariantTypeEnum eType;
    VariantType value;
    switch (item.get_type()) {
        case cpp2::ConfigType::INT64:
            eType = VariantTypeEnum::INT64;
            value = *reinterpret_cast<const int64_t*>(item.get_value().data());
            break;
        case cpp2::ConfigType::BOOL:
            eType = VariantTypeEnum::BOOL;
            value = *reinterpret_cast<const bool*>(item.get_value().data());
            break;
        case cpp2::ConfigType::DOUBLE:
            eType = VariantTypeEnum::DOUBLE;
            value = *reinterpret_cast<const double*>(item.get_value().data());
            break;
        case cpp2::ConfigType::STRING:
            eType = VariantTypeEnum::STRING;
            value = item.get_value();
            break;
    }
    return ConfigItem(item.get_space(), item.get_module(), item.get_name(), eType, value);
}

StatusOr<ConfigItem> MetaClient::getConfigFromCache(const std::string& space,
                                                    const cpp2::ConfigModule& module,
                                                    const std::string& name,
                                                    const VariantTypeEnum& type) {
    if (!configReady_) {
        return Status::Error("Config cache not ready!");
    }

    if (!space.empty()) {
        auto ret = getSpaceIdByNameFromCache(space);
        if (!ret.ok()) {
            return ret.status();
        }
    }
    {
        folly::RWSpinLock::ReadHolder holder(configCacheLock_);
        auto spaceIt = metaConfigMap_.find(space);
        if (spaceIt == metaConfigMap_.end()) {
            return Status::SpaceNotFound();
        }
        auto it = spaceIt->second.find({module, name});
        if (it != spaceIt->second.end()) {
            if (it->second.type_ != type) {
                return Status::CfgErrorType();
            }
            return it->second;
        }
        return Status::CfgNotFound();
    }
}

Status MetaClient::setConfigToCache(const std::string& space, const cpp2::ConfigModule& module,
                                            const std::string& name, const VariantType& value,
                                            const VariantTypeEnum& type) {
    if (!configReady_) {
        return Status::Error("Config cache not ready!");
    }

    if (!space.empty()) {
        auto ret = getSpaceIdByNameFromCache(space);
        if (!ret.ok()) {
            return ret.status();
        }
    }
    {
        folly::RWSpinLock::WriteHolder holder(configCacheLock_);
        std::pair<cpp2::ConfigModule, std::string> key = {module, name};
        ConfigItem item(space, module, name, type, value);
        metaConfigMap_[space][key] = item;
        return Status::OK();
    }
}

StatusOr<std::vector<ConfigItem>>
MetaClient::listConfigsFromCache(const std::string& space, const cpp2::ConfigModule& module) {
    if (!configReady_) {
        return Status::Error("Config cache not ready!");
    }
    std::vector<ConfigItem> ret;
    {
        folly::RWSpinLock::ReadHolder holder(configCacheLock_);
        if (!space.empty()) {
            auto spaceIt = metaConfigMap_.find(space);
            if (spaceIt == metaConfigMap_.end()) {
                return ret;
            }
            for (const auto& entry : spaceIt->second) {
                if (module == cpp2::ConfigModule::ALL || module == entry.first.first) {
                    ret.emplace_back(entry.second);
                }
            }
        } else {
            for (const auto& spaceEntry : metaConfigMap_) {
                for (const auto& entry : spaceEntry.second) {
                    ret.emplace_back(entry.second);
                }
            }
        }
    }
    return ret;
}

Status MetaClient::isCfgRegistered(const std::string& space, const cpp2::ConfigModule& module,
                                   const std::string& name, const VariantTypeEnum& type) {
    if (!configReady_) {
        return Status::Error("Config cache not ready!");
    }

    if (!space.empty()) {
        auto ret = getSpaceIdByNameFromCache(space);
        if (!ret.ok()) {
            return ret.status();
        }
    }

    std::pair<cpp2::ConfigModule, std::string> key = {module, name};
    {
        folly::RWSpinLock::ReadHolder holder(configCacheLock_);
        auto spaceIt = metaConfigMap_.find(space);
        if (spaceIt == metaConfigMap_.end()) {
            return Status::CfgNotFound();
        }
        auto it = spaceIt->second.find(key);
        if (it == spaceIt->second.end()) {
            return Status::CfgNotFound();
        } else if (it->second.type_ != type) {
            return Status::CfgErrorType();
        }
    }
    return Status::CfgRegistered();
}

void MetaClient::loadCfgThreadFunc() {
    if (ioThreadPool_->numThreads() <= 0) {
        LOG(ERROR) << "The threads number in ioThreadPool should be greater than 0";
        addLoadCfgTask();
        return;
    }
    auto ret = listConfigs("", cpp2::ConfigModule::ALL).get();
    MetaConfigMap metaConfigMap;
    if (ret.ok()) {
        auto items = ret.value();
        for (auto& item : items) {
            std::string space = item.space_;
            std::pair<cpp2::ConfigModule, std::string> key = {item.module_, item.name_};
            metaConfigMap[space].emplace(std::move(key), item);
        }
        {
            // we need to merge config from cache and meta
            folly::RWSpinLock::ReadHolder holder(configCacheLock_);
            for (const auto& spaceEntry : metaConfigMap_) {
                auto space = spaceEntry.first;
                for (const auto& entry : spaceEntry.second) {
                    auto key = entry.first;
                    // If configs in cache is not in meta, merge them.
                    auto spaceIt = metaConfigMap.find(space);
                    if (spaceIt == metaConfigMap.end() ||
                            spaceIt->second.find(key) == spaceIt->second.end()) {
                        metaConfigMap[space][key] = metaConfigMap_[space][key];
                    }
                }
            }
        }
        {
            folly::RWSpinLock::WriteHolder holder(configCacheLock_);
            metaConfigMap_ = std::move(metaConfigMap);
            configReady_ = true;
            LOG(INFO) << "Load configs completed";
        }
    } else {
        LOG(INFO) << "Load configs failed: " << ret.status();
    }
    addLoadCfgTask();
}

void MetaClient::addLoadCfgTask() {
    size_t delayMS = FLAGS_load_config_interval_secs * 1000;
    auto value = GET_CONFIG_INT("", cpp2::ConfigModule::META, "load_config_interval_secs");
    if (value.ok()) {
        delayMS = value.value() * 1000;
    }
    bgThread_.addDelayTask(delayMS + folly::Random::rand32(900),
                           &MetaClient::loadCfgThreadFunc, this);
}

}  // namespace meta
}  // namespace nebula
