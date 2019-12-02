/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "time/Duration.h"
#include "meta/common/MetaCommon.h"
#include "meta/client/MetaClient.h"
#include "network/NetworkUtils.h"
#include "meta/NebulaSchemaProvider.h"
#include "meta/ClusterIdMan.h"
#include "meta/GflagsManager.h"
#include "base/Configuration.h"
#include "stats/StatsManager.h"


DEFINE_int32(load_data_interval_secs, 1, "Load data interval");
DEFINE_int32(heartbeat_interval_secs, 10, "Heartbeat interval");
DEFINE_int32(meta_client_retry_times, 3, "meta client retry times, 0 means no retry");
DEFINE_int32(meta_client_retry_interval_secs, 1, "meta client sleep interval between retry");
DEFINE_string(cluster_id_path, "cluster.id", "file path saved clusterId");
DECLARE_string(gflags_mode_json);

namespace nebula {
namespace meta {

MetaClient::MetaClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                       std::vector<HostAddr> addrs,
                       HostAddr localHost,
                       ClusterID clusterId,
                       bool sendHeartBeat,
                       stats::Stats *stats)
    : ioThreadPool_(ioThreadPool)
    , addrs_(std::move(addrs))
    , localHost_(localHost)
    , clusterId_(clusterId)
    , sendHeartBeat_(sendHeartBeat)
    , stats_(stats) {
    CHECK(ioThreadPool_ != nullptr) << "IOThreadPool is required";
    CHECK(!addrs_.empty())
        << "No meta server address is specified. Meta server is required";
    clientsMan_ = std::make_shared<
        thrift::ThriftClientManager<meta::cpp2::MetaServiceAsyncClient>
    >();
    updateActive();
    updateLeader();
    bgThread_ = std::make_unique<thread::GenericWorker>();
    LOG(INFO) << "Create meta client to " << active_;
}


MetaClient::~MetaClient() {
    stop();
    VLOG(3) << "~MetaClient";
}


bool MetaClient::isMetadReady() {
    if (sendHeartBeat_) {
        auto ret = heartbeat().get();
        if (!ret.ok() && ret.status() != Status::LeaderChanged()) {
            LOG(ERROR) << "Heartbeat failed, status:" << ret.status();
            ready_ = false;
            return ready_;
        }
    }  // end if
    loadData();
    loadCfg();
    return ready_;
}

bool MetaClient::waitForMetadReady(int count, int retryIntervalSecs) {
    std::string gflagsJsonPath;
    GflagsManager::getGflagsModule(gflagsModule_);
    gflagsDeclared_ = GflagsManager::declareGflags(gflagsModule_);
    isRunning_ = true;
    int tryCount = count;
    while (!isMetadReady() && ((count == -1) || (tryCount > 0)) && isRunning_) {
        LOG(INFO) << "Waiting for the metad to be ready!";
        --tryCount;
        ::sleep(retryIntervalSecs);
    }  // end while

    if (!isRunning_) {
        LOG(ERROR) << "Connect to the MetaServer Failed";
        return false;
    }

    CHECK(bgThread_->start());
    if (sendHeartBeat_) {
        LOG(INFO) << "Register time task for heartbeat!";
        size_t delayMS = FLAGS_heartbeat_interval_secs * 1000 + folly::Random::rand32(900);
        bgThread_->addTimerTask(delayMS,
                                FLAGS_heartbeat_interval_secs * 1000,
                                &MetaClient::heartBeatThreadFunc, this);
    }
    addLoadDataTask();
    addLoadCfgTask();
    return ready_;
}

void MetaClient::stop() {
    if (bgThread_ != nullptr) {
        bgThread_->stop();
        bgThread_->wait();
        bgThread_.reset();
    }
    isRunning_ = false;
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

bool MetaClient::loadData() {
    if (ioThreadPool_->numThreads() <= 0) {
        LOG(ERROR) << "The threads number in ioThreadPool should be greater than 0";
        return false;
    }
    auto ret = listSpaces().get();
    if (!ret.ok()) {
        LOG(ERROR) << "List space failed, status:" << ret.status();
        return false;
    }
    decltype(localCache_) cache;
    decltype(spaceIndexByName_) spaceIndexByName;
    decltype(spaceTagIndexByName_) spaceTagIndexByName;
    decltype(spaceEdgeIndexByName_) spaceEdgeIndexByName;
    decltype(spaceNewestTagVerMap_) spaceNewestTagVerMap;
    decltype(spaceNewestEdgeVerMap_) spaceNewestEdgeVerMap;
    decltype(spaceEdgeIndexByType_)  spaceEdgeIndexByType;
    decltype(spaceAllEdgeMap_)      spaceAllEdgeMap;

    for (auto space : ret.value()) {
        auto spaceId = space.first;
        auto r = getPartsAlloc(spaceId).get();
        if (!r.ok()) {
            LOG(ERROR) << "Get parts allocation failed for spaceId " << spaceId
                       << ", status " << r.status();
            return false;
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
                         spaceEdgeIndexByType,
                         spaceNewestTagVerMap,
                         spaceNewestEdgeVerMap,
                         spaceAllEdgeMap)) {
            return false;
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
        spaceEdgeIndexByType_  = std::move(spaceEdgeIndexByType);
        spaceAllEdgeMap_ = std::move(spaceAllEdgeMap);
    }
    diff(oldCache, localCache_);
    ready_ = true;
    LOG(INFO) << "Load data completed!";
    return true;
}

void MetaClient::addLoadDataTask() {
    size_t delayMS = FLAGS_load_data_interval_secs * 1000 + folly::Random::rand32(900);
    bgThread_->addDelayTask(delayMS, &MetaClient::loadDataThreadFunc, this);
}


bool MetaClient::loadSchemas(GraphSpaceID spaceId,
                             std::shared_ptr<SpaceInfoCache> spaceInfoCache,
                             SpaceTagNameIdMap &tagNameIdMap,
                             SpaceEdgeNameTypeMap &edgeNameTypeMap,
                             SpaceEdgeTypeNameMap &edgeTypeNameMap,
                             SpaceNewestTagVerMap &newestTagVerMap,
                             SpaceNewestEdgeVerMap &newestEdgeVerMap,
                             SpaceAllEdgeMap &allEdgeMap) {
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
        edgeTypeNameMap.emplace(std::make_pair(spaceId, edgeIt.edge_type), edgeIt.edge_name);
        auto it = allEdgeMap.find(spaceId);
        if (it == allEdgeMap.end()) {
            std::vector<std::string> v = {edgeIt.edge_name};
            allEdgeMap.emplace(spaceId, std::move(v));
        } else {
            it->second.emplace_back(edgeIt.edge_name);
        }
        // get the latest edge version
        auto it2 = newestEdgeVerMap.find(std::make_pair(spaceId, edgeIt.edge_type));
        if (it2 != newestEdgeVerMap.end()) {
            if (it2->second < edgeIt.version) {
                it2->second = edgeIt.version;
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
void MetaClient::getResponse(Request req,
                             RemoteFunc remoteFunc,
                             RespGenerator respGen,
                             folly::Promise<StatusOr<Response>> pro,
                             bool toLeader,
                             int32_t retry,
                             int32_t retryLimit) {
    time::Duration duration;
    auto* evb = ioThreadPool_->getEventBase();
    HostAddr host;
    {
        folly::RWSpinLock::ReadHolder holder(&hostLock_);
        host = toLeader ? leader_ : active_;
    }
    folly::via(evb, [host, evb, req = std::move(req), remoteFunc = std::move(remoteFunc),
                     respGen = std::move(respGen), pro = std::move(pro),
                     toLeader, retry, retryLimit, duration, this] () mutable {
        auto client = clientsMan_->client(host, evb);
        LOG(INFO) << "Send request to meta " << host;
        remoteFunc(client, req).via(evb)
            .then([req = std::move(req), remoteFunc = std::move(remoteFunc),
                   respGen = std::move(respGen), pro = std::move(pro), toLeader, retry,
                   retryLimit, evb, duration, this] (folly::Try<RpcResponse>&& t) mutable {
            // exception occurred during RPC
            if (t.hasException()) {
                if (toLeader) {
                    updateLeader();
                } else {
                    updateActive();
                }
                if (retry < retryLimit) {
                    evb->runAfterDelay([req = std::move(req), remoteFunc = std::move(remoteFunc),
                                        respGen = std::move(respGen), pro = std::move(pro),
                                        toLeader, retry, retryLimit, this] () mutable {
                        getResponse(std::move(req),
                                    std::move(remoteFunc),
                                    std::move(respGen),
                                    std::move(pro),
                                    toLeader,
                                    retry + 1,
                                    retryLimit);
                    }, FLAGS_meta_client_retry_interval_secs * 1000);
                    return;
                } else {
                    LOG(INFO) << "Exceed retry limit";
                    pro.setValue(Status::Error(folly::stringPrintf("RPC failure in MetaClient: %s",
                                                                   t.exception().what().c_str())));
                    stats::Stats::addStatsValue(stats_, false, duration.elapsedInUSec());
                }
                return;
            }
            auto&& resp = t.value();
            if (resp.code == cpp2::ErrorCode::SUCCEEDED) {
                // succeeded
                pro.setValue(respGen(std::move(resp)));
                stats::Stats::addStatsValue(stats_, true, duration.elapsedInUSec());

                return;
            } else if (resp.code == cpp2::ErrorCode::E_LEADER_CHANGED) {
                HostAddr leader(resp.get_leader().get_ip(), resp.get_leader().get_port());
                {
                    folly::RWSpinLock::WriteHolder holder(hostLock_);
                    leader_ = leader;
                }
                if (retry < retryLimit) {
                    evb->runAfterDelay([req = std::move(req), remoteFunc = std::move(remoteFunc),
                                        respGen = std::move(respGen), pro = std::move(pro),
                                        toLeader, retry, retryLimit, this] () mutable {
                        getResponse(std::move(req),
                                    std::move(remoteFunc),
                                    std::move(respGen),
                                    std::move(pro),
                                    toLeader,
                                    retry + 1,
                                    retryLimit);
                    }, FLAGS_meta_client_retry_interval_secs * 1000);
                    return;
                }
            }
            pro.setValue(this->handleResponse(resp));
            stats::Stats::addStatsValue(stats_,
                                        resp.code == cpp2::ErrorCode::SUCCEEDED,
                                        duration.elapsedInUSec());
        });  // then
    });  // via
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
        case cpp2::ErrorCode::E_CONFIG_IMMUTABLE:
            return Status::Error("Config immutable");
        case cpp2::ErrorCode::E_CONFLICT:
            return Status::Error("conflict!");
        case cpp2::ErrorCode::E_WRONGCLUSTER:
            return Status::Error("wrong cluster!");
        case cpp2::ErrorCode::E_LEADER_CHANGED: {
            return Status::LeaderChanged("Leader changed!");
        case cpp2::ErrorCode::E_BALANCED:
            return Status::Error("The cluster is balanced!");
        case cpp2::ErrorCode::E_BALANCER_RUNNING:
            return Status::Error("The balancer is running!");
        case cpp2::ErrorCode::E_BAD_BALANCE_PLAN:
            return Status::Error("Bad balance plan!");
        case cpp2::ErrorCode::E_NO_RUNNING_BALANCE_PLAN:
            return Status::Error("No running balance plan!");
        case cpp2::ErrorCode::E_NO_VALID_HOST:
            return Status::Error("No valid host hold the partition");
        case cpp2::ErrorCode::E_CORRUPTTED_BALANCE_PLAN:
            return Status::Error("No corrupted blance plan");
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
    folly::Promise<StatusOr<GraphSpaceID>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_createSpace(request);
                }, [] (cpp2::ExecResp&& resp) -> GraphSpaceID {
                    return resp.get_id().get_space_id();
                }, std::move(promise), true);
    return future;
}


folly::Future<StatusOr<std::vector<SpaceIdName>>> MetaClient::listSpaces() {
    cpp2::ListSpacesReq req;
    folly::Promise<StatusOr<std::vector<SpaceIdName>>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_listSpaces(request);
                }, [this] (cpp2::ListSpacesResp&& resp) -> decltype(auto) {
                    return this->toSpaceIdName(resp.get_spaces());
                }, std::move(promise));
    return future;
}

folly::Future<StatusOr<cpp2::SpaceItem>>
MetaClient::getSpace(std::string name) {
    cpp2::GetSpaceReq req;
    req.set_space_name(std::move(name));
    folly::Promise<StatusOr<cpp2::SpaceItem>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_getSpace(request);
                }, [] (cpp2::GetSpaceResp&& resp) -> decltype(auto) {
                    return std::move(resp).get_item();
                }, std::move(promise));
    return future;
}

folly::Future<StatusOr<bool>> MetaClient::dropSpace(std::string name) {
    cpp2::DropSpaceReq req;
    req.set_space_name(std::move(name));
    folly::Promise<StatusOr<bool>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_dropSpace(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, std::move(promise), true);
    return future;
}

folly::Future<StatusOr<std::vector<cpp2::HostItem>>> MetaClient::listHosts() {
    cpp2::ListHostsReq req;
    folly::Promise<StatusOr<std::vector<cpp2::HostItem>>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_listHosts(request);
                }, [] (cpp2::ListHostsResp&& resp) -> decltype(auto) {
                    return resp.hosts;
                }, std::move(promise));
    return future;
}

folly::Future<StatusOr<std::vector<cpp2::PartItem>>>
MetaClient::listParts(GraphSpaceID spaceId) {
    cpp2::ListPartsReq req;
    req.set_space_id(std::move(spaceId));
    folly::Promise<StatusOr<std::vector<cpp2::PartItem>>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_listParts(request);
                }, [] (cpp2::ListPartsResp&& resp) -> decltype(auto) {
                    return resp.parts;
                }, std::move(promise));
    return future;
}

folly::Future<StatusOr<std::unordered_map<PartitionID, std::vector<HostAddr>>>>
MetaClient::getPartsAlloc(GraphSpaceID spaceId) {
    cpp2::GetPartsAllocReq req;
    req.set_space_id(spaceId);
    folly::Promise<StatusOr<std::unordered_map<PartitionID, std::vector<HostAddr>>>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_getPartsAlloc(request);
                }, [this] (cpp2::GetPartsAllocResp&& resp) -> decltype(auto) {
                    std::unordered_map<PartitionID, std::vector<HostAddr>> parts;
                    for (auto it = resp.parts.begin(); it != resp.parts.end(); it++) {
                        parts.emplace(it->first, to(it->second));
                    }
                    return parts;
                }, std::move(promise));
    return future;
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
    auto it = spaceTagIndexByName_.find(std::make_pair(space, name));
    if (it == spaceTagIndexByName_.end()) {
        std::string error = folly::stringPrintf("TagName `%s'  is nonexistent", name.c_str());
        return Status::Error(std::move(error));
    }
    return it->second;
}


StatusOr<EdgeType> MetaClient::getEdgeTypeByNameFromCache(const GraphSpaceID& space,
                                                          const std::string& name) {
    if (!ready_) {
        return Status::Error("Not ready!");
    }
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = spaceEdgeIndexByName_.find(std::make_pair(space, name));
    if (it == spaceEdgeIndexByName_.end()) {
        std::string error = folly::stringPrintf("EdgeName `%s'  is nonexistent", name.c_str());
        return Status::Error(std::move(error));
    }
    return it->second;
}

StatusOr<std::string> MetaClient::getEdgeNameByTypeFromCache(const GraphSpaceID& space,
                                                             const EdgeType edgeType) {
    if (!ready_) {
        return Status::Error("Not ready!");
    }
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = spaceEdgeIndexByType_.find(std::make_pair(space, edgeType));
    if (it == spaceEdgeIndexByType_.end()) {
        std::string error = folly::stringPrintf("EdgeType `%d'  is nonexistent", edgeType);
        return Status::Error(std::move(error));
    }
    return it->second;
}

StatusOr<std::vector<std::string>> MetaClient::getAllEdgeFromCache(const GraphSpaceID& space) {
    if (!ready_) {
        return Status::Error("Not ready!");
    }
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = spaceAllEdgeMap_.find(space);
    if (it == spaceAllEdgeMap_.end()) {
        std::string error = folly::stringPrintf("SpaceId `%d'  is nonexistent", space);
        return Status::Error(std::move(error));
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
    std::vector<nebula::cpp2::Pair> data;
    for (auto& element : pairs) {
        data.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                          std::move(element.first), std::move(element.second));
    }
    req.set_segment(std::move(segment));
    req.set_pairs(std::move(data));
    folly::Promise<StatusOr<bool>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_multiPut(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, std::move(promise), true);
    return future;
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
    folly::Promise<StatusOr<std::string>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_get(request);
                }, [] (cpp2::GetResp&& resp) -> std::string {
                    return resp.get_value();
                }, std::move(promise));
    return future;
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
    folly::Promise<StatusOr<std::vector<std::string>>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_multiGet(request);
                }, [] (cpp2::MultiGetResp&& resp) -> std::vector<std::string> {
                    return resp.get_values();
                }, std::move(promise));
    return future;
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
    folly::Promise<StatusOr<std::vector<std::string>>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_scan(request);
                }, [] (cpp2::ScanResp&& resp) -> std::vector<std::string> {
                    return resp.get_values();
                }, std::move(promise));
    return future;
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
    folly::Promise<StatusOr<bool>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_remove(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, std::move(promise), true);
    return future;
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
    folly::Promise<StatusOr<bool>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_removeRange(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, std::move(promise), true);
    return future;
}


PartsMap MetaClient::getPartsMapFromCache(const HostAddr& host) {
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    return doGetPartsMap(host, localCache_);
}


StatusOr<PartMeta> MetaClient::getPartMetaFromCache(GraphSpaceID spaceId, PartitionID partId) {
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = localCache_.find(spaceId);
    if (it == localCache_.end()) {
        return Status::Error("Space not found, spaceid: %d", spaceId);
    }
    auto& cache = it->second;
    auto partAllocIter = cache->partsAlloc_.find(partId);
    if (partAllocIter == cache->partsAlloc_.end()) {
        return Status::Error("Part not found in cache, spaceid: %d, partid: %d", spaceId, partId);
    }
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

StatusOr<int32_t> MetaClient::partsNum(GraphSpaceID spaceId) {
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = localCache_.find(spaceId);
    if (it == localCache_.end()) {
        return Status::Error("Space not found, spaceid: %d", spaceId);
    }
    return it->second->partsAlloc_.size();
}

folly::Future<StatusOr<TagID>>
MetaClient::createTagSchema(GraphSpaceID spaceId, std::string name, nebula::cpp2::Schema schema) {
    cpp2::CreateTagReq req;
    req.set_space_id(std::move(spaceId));
    req.set_tag_name(std::move(name));
    req.set_schema(std::move(schema));
    folly::Promise<StatusOr<TagID>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_createTag(request);
                }, [] (cpp2::ExecResp&& resp) -> TagID {
                    return resp.get_id().get_tag_id();
                }, std::move(promise), true);
    return future;
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
    folly::Promise<StatusOr<TagID>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_alterTag(request);
                }, [] (cpp2::ExecResp&& resp) -> TagID {
                    return resp.get_id().get_tag_id();
                }, std::move(promise), true);
    return future;
}


folly::Future<StatusOr<std::vector<cpp2::TagItem>>>
MetaClient::listTagSchemas(GraphSpaceID spaceId) {
    cpp2::ListTagsReq req;
    req.set_space_id(std::move(spaceId));
    folly::Promise<StatusOr<std::vector<cpp2::TagItem>>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_listTags(request);
                }, [] (cpp2::ListTagsResp&& resp) -> decltype(auto){
                    return std::move(resp).get_tags();
                }, std::move(promise));
    return future;
}


folly::Future<StatusOr<bool>>
MetaClient::dropTagSchema(int32_t spaceId, std::string tagName) {
    cpp2::DropTagReq req;
    req.set_space_id(spaceId);
    req.set_tag_name(std::move(tagName));
    folly::Promise<StatusOr<bool>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_dropTag(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, std::move(promise), true);
    return future;
}


folly::Future<StatusOr<nebula::cpp2::Schema>>
MetaClient::getTagSchema(int32_t spaceId, std::string name, int64_t version) {
    cpp2::GetTagReq req;
    req.set_space_id(spaceId);
    req.set_tag_name(std::move(name));
    req.set_version(version);
    folly::Promise<StatusOr<nebula::cpp2::Schema>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_getTag(request);
                }, [] (cpp2::GetTagResp&& resp) -> nebula::cpp2::Schema {
                    return std::move(resp).get_schema();
                }, std::move(promise));
    return future;
}


folly::Future<StatusOr<EdgeType>>
MetaClient::createEdgeSchema(GraphSpaceID spaceId, std::string name, nebula::cpp2::Schema schema) {
    cpp2::CreateEdgeReq req;
    req.set_space_id(std::move(spaceId));
    req.set_edge_name(std::move(name));
    req.set_schema(schema);
    folly::Promise<StatusOr<EdgeType>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_createEdge(request);
                }, [] (cpp2::ExecResp&& resp) -> EdgeType {
                    return resp.get_id().get_edge_type();
                }, std::move(promise), true);
    return future;
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
    folly::Promise<StatusOr<bool>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_alterEdge(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, std::move(promise), true);
    return future;
}


folly::Future<StatusOr<std::vector<cpp2::EdgeItem>>>
MetaClient::listEdgeSchemas(GraphSpaceID spaceId) {
    cpp2::ListEdgesReq req;
    req.set_space_id(std::move(spaceId));
    folly::Promise<StatusOr<std::vector<cpp2::EdgeItem>>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_listEdges(request);
                }, [] (cpp2::ListEdgesResp&& resp) -> decltype(auto) {
                    return std::move(resp).get_edges();
                }, std::move(promise));
    return future;
}


folly::Future<StatusOr<nebula::cpp2::Schema>>
MetaClient::getEdgeSchema(GraphSpaceID spaceId, std::string name, SchemaVer version) {
    cpp2::GetEdgeReq req;
    req.set_space_id(std::move(spaceId));
    req.set_edge_name(std::move(name));
    req.set_version(version);
    folly::Promise<StatusOr<nebula::cpp2::Schema>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_getEdge(request);
                }, [] (cpp2::GetEdgeResp&& resp) -> nebula::cpp2::Schema {
                    return std::move(resp).get_schema();
                }, std::move(promise));
    return future;
}


folly::Future<StatusOr<bool>>
MetaClient::dropEdgeSchema(GraphSpaceID spaceId, std::string name) {
    cpp2::DropEdgeReq req;
    req.set_space_id(std::move(spaceId));
    req.set_edge_name(std::move(name));
    folly::Promise<StatusOr<bool>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_dropEdge(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, std::move(promise), true);
    return future;
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
    if (clusterId_.load() == 0) {
        clusterId_ = ClusterIdMan::getClusterIdFromFile(FLAGS_cluster_id_path);
    }
    cpp2::HBReq req;
    nebula::cpp2::HostAddr thriftHost;
    thriftHost.set_ip(localHost_.first);
    thriftHost.set_port(localHost_.second);
    req.set_host(std::move(thriftHost));
    req.set_cluster_id(clusterId_.load());
    folly::Promise<StatusOr<bool>> promise;
    auto future = promise.getFuture();
    LOG(INFO) << "Send heartbeat to " << leader_ << ", clusterId " << req.get_cluster_id();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_heartBeat(request);
                }, [this] (cpp2::HBResp&& resp) -> bool {
                    if (clusterId_.load() == 0) {
                        LOG(INFO) << "Persisit the cluster Id from metad " << resp.get_cluster_id();
                        if (ClusterIdMan::persistInFile(resp.get_cluster_id(),
                                                        FLAGS_cluster_id_path)) {
                            clusterId_.store(resp.get_cluster_id());
                        } else {
                            LOG(FATAL) << "Can't persist the clusterId in file "
                                       << FLAGS_cluster_id_path;
                        }
                    }
                    return true;
                }, std::move(promise), true);
    return future;
}

folly::Future<StatusOr<int64_t>> MetaClient::balance(std::vector<HostAddr> hostDel,
                                                     bool isStop) {
    cpp2::BalanceReq req;
    if (!hostDel.empty()) {
        std::vector<nebula::cpp2::HostAddr> tHostDel;
        tHostDel.reserve(hostDel.size());
        std::transform(hostDel.begin(), hostDel.end(),
                       std::back_inserter(tHostDel), [](const auto& h) {
            nebula::cpp2::HostAddr th;
            th.set_ip(h.first);
            th.set_port(h.second);
            return th;
        });
        req.set_host_del(std::move(tHostDel));
    }
    if (isStop) {
        req.set_stop(isStop);
    }

    folly::Promise<StatusOr<int64_t>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_balance(request);
                }, [] (cpp2::BalanceResp&& resp) -> int64_t {
                    return resp.id;
                }, std::move(promise), true);
    return future;
}

folly::Future<StatusOr<std::vector<cpp2::BalanceTask>>>
MetaClient::showBalance(int64_t balanceId) {
    cpp2::BalanceReq req;
    req.set_id(balanceId);
    folly::Promise<StatusOr<std::vector<cpp2::BalanceTask>>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_balance(request);
                }, [] (cpp2::BalanceResp&& resp) -> std::vector<cpp2::BalanceTask> {
                    return resp.tasks;
                }, std::move(promise), true);
    return future;
}

folly::Future<StatusOr<bool>> MetaClient::balanceLeader() {
    cpp2::LeaderBalanceReq req;
    folly::Promise<StatusOr<bool>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_leaderBalance(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, std::move(promise), true);
    return future;
}

folly::Future<StatusOr<std::string>> MetaClient::getTagDefaultValue(GraphSpaceID spaceId,
                                                                    TagID tagId,
                                                                    const std::string& field) {
    cpp2::GetReq req;
    static std::string defaultKey = "__default__";
    req.set_segment(defaultKey);
    std::string key;
    key.reserve(64);
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    key.append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
    key.append(field);
    req.set_key(std::move(key));
    folly::Promise<StatusOr<std::string>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_get(request);
    }, [] (cpp2::GetResp&& resp) -> std::string {
        return resp.get_value();
    }, std::move(promise));
    return future;
}

folly::Future<StatusOr<std::string>> MetaClient::getEdgeDefaultValue(GraphSpaceID spaceId,
                                                                     EdgeType edgeType,
                                                                     const std::string& field) {
    cpp2::GetReq req;
    static std::string defaultKey = "__default__";
    req.set_segment(defaultKey);
    std::string key;
    key.reserve(64);
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    key.append(reinterpret_cast<const char*>(&edgeType), sizeof(EdgeType));
    key.append(field);
    req.set_key(std::move(key));
    folly::Promise<StatusOr<std::string>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_get(request);
    }, [] (cpp2::GetResp&& resp) -> std::string {
        return resp.get_value();
    },  std::move(promise));
    return future;
}

folly::Future<StatusOr<bool>>
MetaClient::regConfig(const std::vector<cpp2::ConfigItem>& items) {
    cpp2::RegConfigReq req;
    req.set_items(items);
    folly::Promise<StatusOr<int64_t>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_regConfig(request);
                }, [] (cpp2::ExecResp&& resp) -> decltype(auto) {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, std::move(promise), true);
    return future;
}

folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
MetaClient::getConfig(const cpp2::ConfigModule& module, const std::string& name) {
    if (!configReady_) {
        return Status::Error("Not ready!");
    }
    cpp2::ConfigItem item;
    item.set_module(module);
    item.set_name(name);
    cpp2::GetConfigReq req;
    req.set_item(item);
    folly::Promise<StatusOr<std::vector<cpp2::ConfigItem>>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_getConfig(request);
                }, [] (cpp2::GetConfigResp&& resp) -> decltype(auto) {
                    return std::move(resp).get_items();
                }, std::move(promise));
    return future;
}

folly::Future<StatusOr<bool>>
MetaClient::setConfig(const cpp2::ConfigModule& module, const std::string& name,
                      const cpp2::ConfigType& type, const std::string& value) {
    if (!configReady_) {
        return Status::Error("Not ready!");
    }
    cpp2::ConfigItem item;
    item.set_module(module);
    item.set_name(name);
    item.set_type(type);
    item.set_mode(cpp2::ConfigMode::MUTABLE);
    item.set_value(value);

    cpp2::SetConfigReq req;
    req.set_item(item);
    folly::Promise<StatusOr<bool>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_setConfig(request);
                }, [] (cpp2::ExecResp&& resp) -> decltype(auto) {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                }, std::move(promise), true);
    return future;
}

folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
MetaClient::listConfigs(const cpp2::ConfigModule& module) {
    if (!configReady_) {
        return Status::Error("Not ready!");
    }
    cpp2::ListConfigsReq req;
    req.set_module(module);
    folly::Promise<StatusOr<std::vector<cpp2::ConfigItem>>> promise;
    auto future = promise.getFuture();
    getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_listConfigs(request);
                }, [] (cpp2::ListConfigsResp&& resp) -> decltype(auto) {
                    return std::move(resp).get_items();
                }, std::move(promise));
    return future;
}

void MetaClient::loadCfgThreadFunc() {
    loadCfg();
    addLoadCfgTask();
}

bool MetaClient::registerCfg() {
    auto ret = regConfig(gflagsDeclared_).get();
    if (ret.ok()) {
        LOG(INFO) << "Register gflags ok " << gflagsDeclared_.size();
        configReady_ = true;
    }
    return configReady_;
}

void MetaClient::loadCfg() {
    if (!configReady_ && !registerCfg()) {
        return;
    }
    // only load current module's config is enough
    auto ret = listConfigs(gflagsModule_).get();
    if (ret.ok()) {
        // if we load config from meta server successfully, update gflags and set configReady_
        auto tItems = ret.value();
        std::vector<ConfigItem> items;
        for (const auto& tItem : tItems) {
            items.emplace_back(toConfigItem(tItem));
        }
        MetaConfigMap metaConfigMap;
        for (auto& item : items) {
            std::pair<cpp2::ConfigModule, std::string> key = {item.module_, item.name_};
            metaConfigMap.emplace(std::move(key), std::move(item));
        }
        {
            // For any configurations that is in meta, update in cache to replace previous value
            folly::RWSpinLock::WriteHolder holder(configCacheLock_);
            for (const auto& entry : metaConfigMap) {
                auto& key = entry.first;
                auto it = metaConfigMap_.find(key);
                if (it == metaConfigMap_.end() || metaConfigMap[key].value_ != it->second.value_) {
                    updateGflagsValue(entry.second);
                    metaConfigMap_[key] = entry.second;
                }
            }
        }
    } else {
        LOG(INFO) << "Load configs failed: " << ret.status();
        return;
    }
}

void MetaClient::addLoadCfgTask() {
    size_t delayMS = FLAGS_load_data_interval_secs * 1000 + folly::Random::rand32(900);
    bgThread_->addDelayTask(delayMS, &MetaClient::loadCfgThreadFunc, this);
    LOG(INFO) << "Load configs completed, call after " << delayMS << " ms";
}

void MetaClient::updateGflagsValue(const ConfigItem& item) {
    if (item.mode_ != cpp2::ConfigMode::MUTABLE) {
        return;
    }

    std::string metaValue;
    switch (item.type_) {
        case cpp2::ConfigType::INT64:
            metaValue = folly::to<std::string>(boost::get<int64_t>(item.value_));
            break;
        case cpp2::ConfigType::DOUBLE:
            metaValue = folly::to<std::string>(boost::get<double>(item.value_));
            break;
        case cpp2::ConfigType::BOOL:
            metaValue = boost::get<bool>(item.value_) ? "true" : "false";
            break;
        case cpp2::ConfigType::STRING:
        case cpp2::ConfigType::NESTED:
            metaValue = boost::get<std::string>(item.value_);
            break;
    }

    std::string curValue;
    if (!gflags::GetCommandLineOption(item.name_.c_str(), &curValue)) {
        return;
    } else if (curValue != metaValue) {
        if (item.type_ == cpp2::ConfigType::NESTED && metaValue.empty()) {
            // Be compatible with previous configuration
            metaValue = "{}";
        }
        gflags::SetCommandLineOption(item.name_.c_str(), metaValue.c_str());
        // TODO: we simply judge the rocksdb by nested type for now
        if (listener_ != nullptr && item.type_ == cpp2::ConfigType::NESTED) {
            updateNestedGflags(item.name_);
        }
        LOG(INFO) << "update " << item.name_ << " from " << curValue << " to " << metaValue;
    }
}

void MetaClient::updateNestedGflags(const std::string& name) {
    std::string json;
    gflags::GetCommandLineOption(name.c_str(), &json);
    // generate option string map
    Configuration conf;
    auto status = conf.parseFromString(json);
    if (!status.ok()) {
        LOG(ERROR) << "Parse nested gflags " << name << " failed";
        return;
    }
    std::unordered_map<std::string, std::string> optionMap;
    conf.forEachItem([&optionMap] (const std::string& key, const folly::dynamic& val) {
        optionMap.emplace(key, val.asString());
    });
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    for (const auto& spaceEntry : localCache_) {
        listener_->onSpaceOptionUpdated(spaceEntry.first, optionMap);
    }
}

ConfigItem MetaClient::toConfigItem(const cpp2::ConfigItem& item) {
    VariantType value;
    switch (item.get_type()) {
        case cpp2::ConfigType::INT64:
            value = *reinterpret_cast<const int64_t*>(item.get_value().data());
            break;
        case cpp2::ConfigType::BOOL:
            value = *reinterpret_cast<const bool*>(item.get_value().data());
            break;
        case cpp2::ConfigType::DOUBLE:
            value = *reinterpret_cast<const double*>(item.get_value().data());
            break;
        case cpp2::ConfigType::STRING:
        case cpp2::ConfigType::NESTED:
            value = item.get_value();
            break;
    }
    return ConfigItem(item.get_module(), item.get_name(), item.get_type(), item.get_mode(), value);
}

Status MetaClient::refreshCache() {
    auto ret = bgThread_->addTask(&MetaClient::loadData, this).get();
    return ret ? Status::OK() : Status::Error("Load data failed");
}

}  // namespace meta
}  // namespace nebula
