/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/common/MetaCommon.h"
#include "meta/client/MetaClient.h"
#include "network/NetworkUtils.h"
#include "meta/NebulaSchemaProvider.h"

DEFINE_int32(load_data_interval_second, 2 * 60, "Load data interval, unit: second");
DEFINE_string(meta_server_addrs, "", "list of meta server addresses,"
                                     "the format looks like ip1:port1, ip2:port2, ip3:port3");
DEFINE_int32(meta_client_io_threads, 3, "meta client io threads");

/**
 * check argument is empty
 */
#define CHECK_PARAMETER_AND_RETURN_STATUS(argument) \
    if (argument.empty()) { \
        return Status::Error("argument is invalid!"); \
    }

#define CHECK_SEGMENT_AND_RETURN_STATUS(segment) \
    if (!nebula::meta::MetaCommon::checkSegment(segment)) { \
        return Status::Error("segment is invalid!"); \
    }

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
    LOG(INFO) << "Create meta client to " << active_;
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
            LOG(ERROR) << "Get parts allocaction failed for spaceId " << spaceId
                       << ", status " << r.status();
            return;
        }

        auto spaceCache = std::make_shared<SpaceInfoCache>();
        auto partsAlloc = r.value();
        spaceCache->spaceName = space.second;
        spaceCache->partsOnHost_ = reverse(partsAlloc);
        spaceCache->partsAlloc_ = std::move(partsAlloc);
        VLOG(3) << "Load space " << spaceId << ", parts num:" << spaceCache->partsAlloc_.size();

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
    diff(cache);
    {
        folly::RWSpinLock::WriteHolder holder(localCacheLock_);
        localCache_ = std::move(cache);
        spaceIndexByName_ = std::move(spaceIndexByName);
        spaceTagIndexByName_ = std::move(spaceTagIndexByName);
        spaceEdgeIndexByName_ = std::move(spaceEdgeIndexByName);
        spaceNewestTagVerMap_ = std::move(spaceNewestTagVerMap);
        spaceNewestEdgeVerMap_ = std::move(spaceNewestEdgeVerMap);
    }
    LOG(INFO) << "Load data completed!";
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
        std::shared_ptr<NebulaSchemaProvider> schema(new NebulaSchemaProvider());
        for (auto& colIt : tagIt.schema.get_columns()) {
            schema->fields_.push_back(
                std::make_shared<NebulaSchemaProvider::SchemaField>(colIt.name, colIt.type));
            schema->fieldNameIndex_.emplace(colIt.name,
                                            static_cast<int64_t>(schema->fields_.size() - 1));
        }
        schema->ver_ = tagIt.version;
        tagIdSchemas.emplace(std::make_pair(tagIt.tag_id, tagIt.version), schema);
        tagNameIdMap.emplace(std::make_pair(spaceId, tagIt.tag_name), tagIt.tag_id);
        // get the newest version
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
        std::shared_ptr<NebulaSchemaProvider> schema(new NebulaSchemaProvider());
        for (auto& colIt : edgeIt.schema.get_columns()) {
            schema->fields_.push_back(
                std::make_shared<NebulaSchemaProvider::SchemaField>(colIt.name, colIt.type));
            schema->fieldNameIndex_.emplace(colIt.name,
                                            static_cast<int64_t>(schema->fields_.size() - 1));
        }
        schema->ver_ = edgeIt.version;
        edgeTypeSchemas.emplace(std::make_pair(edgeIt.edge_type, edgeIt.version), schema);
        edgeNameTypeMap.emplace(std::make_pair(spaceId, edgeIt.edge_name), edgeIt.edge_type);
        // get the newest version
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
                                     RespGenerator respGen) {
    folly::Promise<StatusOr<Response>> pro;
    auto f = pro.getFuture();
    auto* evb = ioThreadPool_->getEventBase();
    auto client = clientsMan_->client(active_, evb);
    remoteFunc(client, std::move(req))
         .then(evb, [p = std::move(pro), respGen, this] (folly::Try<RpcResponse>&& t) mutable {
        // exception occurred during RPC
        if (t.hasException()) {
            p.setValue(Status::Error(folly::stringPrintf("RPC failure in MetaClient: %s",
                                                         t.exception().what().c_str())));
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

folly::Future<StatusOr<GraphSpaceID>>
MetaClient::createSpace(std::string name, int32_t partsNum, int32_t replicaFactor) {
    cpp2::CreateSpaceReq req;
    req.set_space_name(std::move(name));
    req.set_parts_num(partsNum);
    req.set_replica_factor(replicaFactor);
    return getResponse(std::move(req), [] (auto client, auto request) {
                return client->future_createSpace(request);
            }, [] (cpp2::ExecResp&& resp) -> GraphSpaceID {
                return resp.get_id().get_space_id();
            });
}

folly::Future<StatusOr<std::vector<SpaceIdName>>> MetaClient::listSpaces() {
    cpp2::ListSpacesReq req;
    return getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_listSpaces(request);
                }, [this] (cpp2::ListSpacesResp&& resp) -> decltype(auto) {
                    return this->toSpaceIdName(resp.get_spaces());
                });
}


folly::Future<StatusOr<bool>> MetaClient::dropSpace(std::string name) {
    cpp2::DropSpaceReq req;
    req.set_space_name(std::move(name));
    return getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_dropSpace(request);
                }, [] (cpp2::ExecResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                });
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
                });
}

folly::Future<StatusOr<std::vector<HostAddr>>> MetaClient::listHosts() {
    cpp2::ListHostsReq req;
    return getResponse(std::move(req), [] (auto client, auto request) {
                return client->future_listHosts(request);
            }, [this] (cpp2::ListHostsResp&& resp) -> decltype(auto) {
                return this->to(resp.hosts);
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
                });
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
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = spaceIndexByName_.find(name);
    if (it != spaceIndexByName_.end()) {
        return it->second;
    }
    return Status::SpaceNotFound();
}

StatusOr<TagID> MetaClient::getTagIDByNameFromCache(const GraphSpaceID& space,
                                                    const std::string& name) {
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = spaceTagIndexByName_.find(make_pair(space, name));
    if (it == spaceTagIndexByName_.end()) {
        return Status::Error("Tag is not exist!");
    }
    return it->second;
}

StatusOr<EdgeType> MetaClient::getEdgeTypeByNameFromCache(const GraphSpaceID& space,
                                                          const std::string& name) {
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
    CHECK_SEGMENT_AND_RETURN_STATUS(segment);
    CHECK_PARAMETER_AND_RETURN_STATUS(pairs);
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
                }, [] (cpp2::MultiPutResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                });
}

folly::Future<StatusOr<std::string>>
MetaClient::get(std::string segment, std::string key) {
    CHECK_SEGMENT_AND_RETURN_STATUS(segment);
    CHECK_PARAMETER_AND_RETURN_STATUS(key);
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
    CHECK_SEGMENT_AND_RETURN_STATUS(segment);
    CHECK_PARAMETER_AND_RETURN_STATUS(keys);
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
    CHECK_SEGMENT_AND_RETURN_STATUS(segment);
    CHECK_PARAMETER_AND_RETURN_STATUS(start);
    CHECK_PARAMETER_AND_RETURN_STATUS(end);
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
    CHECK_SEGMENT_AND_RETURN_STATUS(segment);
    CHECK_PARAMETER_AND_RETURN_STATUS(key);
    cpp2::RemoveReq req;
    req.set_segment(std::move(segment));
    req.set_key(std::move(key));
    return getResponse(std::move(req), [] (auto client, auto request) {
                return client->future_remove(request);
            }, [] (cpp2::RemoveResp&& resp) -> bool {
                return resp.code == cpp2::ErrorCode::SUCCEEDED;
            });
}

folly::Future<StatusOr<bool>>
MetaClient::removeRange(std::string segment, std::string start, std::string end) {
    CHECK_SEGMENT_AND_RETURN_STATUS(segment);
    CHECK_PARAMETER_AND_RETURN_STATUS(start);
    CHECK_PARAMETER_AND_RETURN_STATUS(end);
    cpp2::RemoveRangeReq req;
    req.set_segment(std::move(segment));
    req.set_start(std::move(start));
    req.set_end(std::move(end));
    return getResponse(std::move(req), [] (auto client, auto request) {
                    return client->future_removeRange(request);
                }, [] (cpp2::RemoveRangeResp&& resp) -> bool {
                    return resp.code == cpp2::ErrorCode::SUCCEEDED;
                });
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
        case cpp2::ErrorCode::E_EXISTED:
            return Status::Error("existed!");
        case cpp2::ErrorCode::E_NOT_FOUND:
            return Status::Error("not existed!");
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
        VLOG(3) << "Listener is null!";
        return;
    }
    auto localHost = listener_->getLocalHost();
    auto newPartsMap = doGetPartsMap(localHost, newCache);
    auto oldPartsMap = getPartsMapFromCache(localHost);
    VLOG(1) << "Let's check if any new parts added/updated for " << localHost;
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
    });
}

folly::Future<StatusOr<std::vector<cpp2::TagItem>>>
MetaClient::listTagSchemas(GraphSpaceID spaceId) {
    cpp2::ListTagsReq req;
    req.set_space_id(std::move(spaceId));
    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_listTags(request);
    }, [this] (cpp2::ListTagsResp&& resp) -> decltype(auto){
        return std::move(resp).get_tags();
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
    });
}

folly::Future<StatusOr<std::vector<cpp2::EdgeItem>>>
MetaClient::listEdgeSchemas(GraphSpaceID spaceId) {
    cpp2::ListEdgesReq req;
    req.set_space_id(std::move(spaceId));
    return getResponse(std::move(req), [] (auto client, auto request) {
        return client->future_listEdges(request);
    }, [this] (cpp2::ListEdgesResp&& resp) -> decltype(auto) {
        return std::move(resp).get_edges();
    });
}

StatusOr<std::shared_ptr<const SchemaProviderIf>>
MetaClient::getTagSchemeFromCache(GraphSpaceID spaceId, TagID tagID, SchemaVer ver) {
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

StatusOr<std::shared_ptr<const SchemaProviderIf>> MetaClient::getEdgeSchemeFromCache(
        GraphSpaceID spaceId, EdgeType edgeType, SchemaVer ver) {
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

SchemaVer MetaClient::getNewestTagVerFromCache(const GraphSpaceID& space, const TagID& tagId) {
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = spaceNewestTagVerMap_.find(std::make_pair(space, tagId));
    if (it == spaceNewestTagVerMap_.end()) {
        return -1;
    }
    return it->second;
}

SchemaVer MetaClient::getNewestEdgeVerFromCache(const GraphSpaceID& space,
                                                const EdgeType& edgeType) {
    folly::RWSpinLock::ReadHolder holder(localCacheLock_);
    auto it = spaceNewestEdgeVerMap_.find(std::make_pair(space, edgeType));
    if (it == spaceNewestEdgeVerMap_.end()) {
        return -1;
    }
    return it->second;
}

}  // namespace meta
}  // namespace nebula
