/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/client/StorageClient.h"

#define ID_HASH(id, numShards) \
    ((static_cast<uint64_t>(id)) % numShards + 1)

namespace nebula {
namespace storage {

StorageClient::StorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> threadPool,
                             meta::MetaClient *client)
        : ioThreadPool_(threadPool)
        , client_(client) {
    clientsMan_
        = std::make_unique<thrift::ThriftClientManager<storage::cpp2::StorageServiceAsyncClient>>();
}


StorageClient::~StorageClient() {
    VLOG(3) << "~StorageClient";
    if (nullptr != client_) {
        client_ = nullptr;
    }
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> StorageClient::addVertices(
        GraphSpaceID space,
        std::vector<cpp2::Vertex> vertices,
        bool overwritable,
        folly::EventBase* evb) {
    auto clusters = clusterIdsToHosts(
        space,
        vertices,
        [] (const cpp2::Vertex& v) {
            return v.get_id();
        });

    std::unordered_map<HostAddr, cpp2::AddVerticesRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_overwritable(overwritable);
        req.set_parts(std::move(c.second));
    }

    VLOG(3) << "requests size " << requests.size();
    return collectResponse(
        evb, std::move(requests),
        [](cpp2::StorageServiceAsyncClient* client,
           const cpp2::AddVerticesRequest& r) {
            return client->future_addVertices(r);
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> StorageClient::addEdges(
        GraphSpaceID space,
        std::vector<storage::cpp2::Edge> edges,
        bool overwritable,
        folly::EventBase* evb) {
    auto clusters = clusterIdsToHosts(
        space,
        edges,
        [] (const cpp2::Edge& e) {
            return e.get_key().get_src();
        });

    std::unordered_map<HostAddr, cpp2::AddEdgesRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_overwritable(overwritable);
        req.set_parts(std::move(c.second));
    }

    return collectResponse(
        evb, std::move(requests),
        [](cpp2::StorageServiceAsyncClient* client,
           const cpp2::AddEdgesRequest& r) {
            return client->future_addEdges(r);
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::QueryResponse>> StorageClient::getNeighbors(
        GraphSpaceID space,
        const std::vector<VertexID> &vertices,
        const std::vector<EdgeType> &edgeTypes,
        std::string filter,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto clusters = clusterIdsToHosts(
        space,
        vertices,
        [] (const VertexID& v) {
            return v;
        });

    std::unordered_map<HostAddr, cpp2::GetNeighborsRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
        req.set_edge_types(edgeTypes);
        req.set_filter(filter);
        req.set_return_columns(returnCols);
    }

    return collectResponse(
        evb, std::move(requests),
        [](cpp2::StorageServiceAsyncClient* client, const cpp2::GetNeighborsRequest& r) {
            return client->future_getBound(r);
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::QueryStatsResponse>> StorageClient::neighborStats(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        std::vector<EdgeType> edgeTypes,
        std::string filter,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto clusters = clusterIdsToHosts(
        space,
        vertices,
        [] (const VertexID& v) {
            return v;
        });

    std::unordered_map<HostAddr, cpp2::GetNeighborsRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
        // Make edge type a negative number when query in-bound
        req.set_edge_types(edgeTypes);
        req.set_filter(filter);
        req.set_return_columns(returnCols);
    }

    return collectResponse(
        evb, std::move(requests),
        [](cpp2::StorageServiceAsyncClient* client, const cpp2::GetNeighborsRequest& r) {
            return client->future_boundStats(r);
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::QueryResponse>> StorageClient::getVertexProps(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto clusters = clusterIdsToHosts(
        space,
        vertices,
        [] (const VertexID& v) {
            return v;
        });

    std::unordered_map<HostAddr, cpp2::VertexPropRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
        req.set_return_columns(returnCols);
    }

    return collectResponse(
        evb, std::move(requests),
        [](cpp2::StorageServiceAsyncClient* client,
           const cpp2::VertexPropRequest& r) {
            return client->future_getProps(r);
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::EdgePropResponse>> StorageClient::getEdgeProps(
        GraphSpaceID space,
        std::vector<cpp2::EdgeKey> edges,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto clusters = clusterIdsToHosts(
        space,
        edges,
        [] (const cpp2::EdgeKey& v) {
            return v.get_src();
        });

    std::unordered_map<HostAddr, cpp2::EdgePropRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        for (auto& p : c.second) {
            req.set_edge_type((p.second[0].edge_type));
            break;
        }
        req.set_parts(std::move(c.second));
        req.set_return_columns(returnCols);
    }

    return collectResponse(
        evb, std::move(requests),
        [](cpp2::StorageServiceAsyncClient* client,
           const cpp2::EdgePropRequest& r) {
            return client->future_getEdgeProps(r);
        });
}


folly::Future<StatusOr<cpp2::EdgeKeyResponse>> StorageClient::getEdgeKeys(
    GraphSpaceID space,
    VertexID vid,
    folly::EventBase* evb) {
    std::pair<HostAddr, cpp2::EdgeKeyRequest> request;
    PartitionID part = partId(space, vid);
    auto partMeta = getPartMeta(space, part);
    CHECK_GT(partMeta.peers_.size(), 0U);
    const auto& leader = this->leader(partMeta);
    request.first = leader;

    cpp2::EdgeKeyRequest req;
    req.set_space_id(space);
    req.set_part_id(part);
    req.set_vid(vid);
    request.second = std::move(req);

    return getResponse(
        evb,
        std::move(request),
        [] (cpp2::StorageServiceAsyncClient* client,
            const cpp2::EdgeKeyRequest& r) {
            return client->future_getEdgeKeys(r);
    });
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> StorageClient::deleteEdges(
    GraphSpaceID space,
    std::vector<storage::cpp2::EdgeKey> edges,
    folly::EventBase* evb) {
    auto clusters = clusterIdsToHosts(
        space,
        edges,
        [] (const cpp2::EdgeKey& v) {
            return v.get_src();
        });

    std::unordered_map<HostAddr, cpp2::DeleteEdgesRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
    }

    return collectResponse(
        evb, std::move(requests),
        [](cpp2::StorageServiceAsyncClient* client,
           const cpp2::DeleteEdgesRequest& r) {
            return client->future_deleteEdges(r);
        });
}


folly::Future<StatusOr<cpp2::ExecResponse>> StorageClient::deleteVertex(
    GraphSpaceID space,
    VertexID vid,
    folly::EventBase* evb) {
    std::pair<HostAddr, cpp2::DeleteVertexRequest> request;
    PartitionID part = partId(space, vid);
    auto partMeta = getPartMeta(space, part);
    CHECK_GT(partMeta.peers_.size(), 0U);
    const auto& leader = this->leader(partMeta);
    request.first = leader;

    cpp2::DeleteVertexRequest req;
    req.set_space_id(space);
    req.set_part_id(part);
    req.set_vid(vid);
    request.second = std::move(req);

    return getResponse(
        evb,
        std::move(request),
        [] (cpp2::StorageServiceAsyncClient* client,
            const cpp2::DeleteVertexRequest& r) {
            return client->future_deleteVertex(r);
    });
}


folly::Future<StatusOr<storage::cpp2::UpdateResponse>> StorageClient::updateVertex(
        GraphSpaceID space,
        VertexID vertexId,
        std::string filter,
        std::vector<storage::cpp2::UpdateItem> updateItems,
        std::vector<std::string> returnCols,
        bool insertable,
        folly::EventBase* evb) {
    std::pair<HostAddr, cpp2::UpdateVertexRequest> request;
    PartitionID part = this->partId(space, vertexId);
    auto partMeta = this->getPartMeta(space, part);
    CHECK_GT(partMeta.peers_.size(), 0U);
    const auto& host = this->leader(partMeta);
    request.first = std::move(host);
    cpp2::UpdateVertexRequest req;
    req.set_space_id(space);
    req.set_vertex_id(vertexId);
    req.set_part_id(part);
    req.set_filter(filter);
    req.set_update_items(std::move(updateItems));
    req.set_return_columns(returnCols);
    req.set_insertable(insertable);
    request.second = std::move(req);

    return getResponse(
        evb, std::move(request),
        [] (cpp2::StorageServiceAsyncClient* client,
           const cpp2::UpdateVertexRequest& r) {
            return client->future_updateVertex(r);
        });
}


folly::Future<StatusOr<storage::cpp2::UpdateResponse>> StorageClient::updateEdge(
        GraphSpaceID space,
        storage::cpp2::EdgeKey edgeKey,
        std::string filter,
        std::vector<storage::cpp2::UpdateItem> updateItems,
        std::vector<std::string> returnCols,
        bool insertable,
        folly::EventBase* evb) {
    std::pair<HostAddr, cpp2::UpdateEdgeRequest> request;
    PartitionID part = this->partId(space, edgeKey.get_src());
    auto partMeta = this->getPartMeta(space, part);
    CHECK_GT(partMeta.peers_.size(), 0U);
    const auto& host = this->leader(partMeta);
    request.first = std::move(host);
    cpp2::UpdateEdgeRequest req;
    req.set_space_id(space);
    req.set_edge_key(edgeKey);
    req.set_part_id(part);
    req.set_filter(filter);
    req.set_update_items(std::move(updateItems));
    req.set_return_columns(returnCols);
    req.set_insertable(insertable);
    request.second = std::move(req);

    return getResponse(
        evb, std::move(request),
        [] (cpp2::StorageServiceAsyncClient* client,
           const cpp2::UpdateEdgeRequest& r) {
            return client->future_updateEdge(r);
        });
}


folly::Future<StatusOr<cpp2::GetUUIDResp>> StorageClient::getUUID(
        GraphSpaceID space,
        const std::string& name,
        folly::EventBase* evb) {
    std::pair<HostAddr, cpp2::GetUUIDReq> request;
    std::hash<std::string> hashFunc;
    auto hashValue = hashFunc(name);
    PartitionID part = partId(space, hashValue);
    auto partMeta = getPartMeta(space, part);
    CHECK_GT(partMeta.peers_.size(), 0U);
    const auto& leader = this->leader(partMeta);
    request.first = leader;

    cpp2::GetUUIDReq req;
    req.set_space_id(space);
    req.set_part_id(part);
    req.set_name(name);
    request.second = std::move(req);

    return getResponse(
        evb,
        std::move(request),
        [] (cpp2::StorageServiceAsyncClient* client,
            const cpp2::GetUUIDReq& r) {
            return client->future_getUUID(r);
    });
}


// Get All leaders about these hosts
folly::SemiFuture<StorageRpcResponse<cpp2::GetLeaderResp>>
StorageClient::getLeaders(
        const std::vector<HostAddr>& hosts,
        folly::EventBase* evb) {
    if (UNLIKELY(hosts.empty())) {
        return folly::makeSemiFuture(StorageRpcResponse<cpp2::GetLeaderResp>::fastFailed());
    }
    std::vector<std::pair<HostAddr, cpp2::GetLeaderReq>> requests;
    requests.reserve(hosts.size());
    for (auto& host : hosts) {
        requests.emplace_back(
            std::make_pair(host, cpp2::GetLeaderReq()));
    }

    return collectResponseWithoutLeader(evb, std::move(requests),
                        [](cpp2::StorageServiceAsyncClient* client,
                            const cpp2::GetLeaderReq& r) {
                                return client->future_getLeaderPart(r);
                            });
}


PartitionID StorageClient::partId(GraphSpaceID spaceId, int64_t id) const {
    auto parts = partsNum(spaceId);
    auto s = ID_HASH(id, parts);
    CHECK_GE(s, 0U);
    return s;
}

folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
StorageClient::put(GraphSpaceID space,
                   std::vector<nebula::cpp2::Pair> values,
                   folly::EventBase* evb) {
    auto clusters = clusterIdsToHosts(space, values,
                                      [] (const nebula::cpp2::Pair& v) {
                                          return std::hash<std::string>{}(v.get_key());
                                      });

    std::unordered_map<HostAddr, cpp2::PutRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
    }

    return collectResponse(evb, std::move(requests),
                           [](cpp2::StorageServiceAsyncClient* client,
                              const cpp2::PutRequest& r) {
                                  return client->future_put(r);
                              });
}

folly::SemiFuture<StorageRpcResponse<storage::cpp2::GeneralResponse>>
StorageClient::get(GraphSpaceID space,
                   const std::vector<std::string>& keys,
                   folly::EventBase* evb) {
    auto clusters = clusterIdsToHosts(space, keys,
                                      [] (const std::string& v) {
                                          return std::hash<std::string>{}(v);
                                      });

    std::unordered_map<HostAddr, cpp2::GetRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
    }

    return collectResponse(evb, std::move(requests),
                           [](cpp2::StorageServiceAsyncClient* client,
                              const cpp2::GetRequest& r) {
                                  return client->future_get(r);
                              });
}

StatusOr<std::unordered_map<std::pair<GraphSpaceID, PartitionID>, HostAddr>>
StorageClient::preHeatLeaders() {
    static constexpr char host_err[] = "Get hosts failed!";
    auto hosts = client_->listHosts().thenValue([](auto&& hosts) {
        std::vector<HostAddr> req_hosts;
        if (UNLIKELY(!hosts.ok())) {
            LOG(WARNING) << host_err;
            return req_hosts;
        }
        for (auto& host : hosts.value()) {
            if (host.get_status() == meta::cpp2::HostStatus::ONLINE) {
                req_hosts.emplace_back(
                    std::make_pair(host.get_hostAddr().get_ip(), host.get_hostAddr().get_port()));
            } else {
                LOG(WARNING) << "Invalid host at " << std::make_pair(
                    host.get_hostAddr().get_ip(), host.get_hostAddr().get_port());
            }
        }
        return req_hosts;
    }).thenError([](auto&& e) {
        LOG(ERROR) << host_err << e.what();
        return std::vector<HostAddr>();
    }).get();
    return getLeaders(hosts).via(ioThreadPool_->getEventBase()).thenValue([&hosts](auto&& leaders){
        if (UNLIKELY(!leaders.succeeded())) {
            constexpr char err[] = "At least one RPC call failed";
            // Require complete successded for the holes in responses
            LOG(WARNING) << err;
            return decltype(preHeatLeaders())(Status::Error(err));
        }
        if (UNLIKELY(hosts.size() != leaders.responses().size())) {
            constexpr char err[] = "Impossible the req/resp count not equal!";
            LOG(ERROR) << err;
            return decltype(preHeatLeaders())(Status::Error(err));
        }
        std::unordered_map<std::pair<GraphSpaceID, PartitionID>, HostAddr> r;
        for (std::size_t i = 0; i < leaders.responses().size(); ++i) {
            for (auto& parts : leaders.responses()[i].get_leader_parts()) {
                for (auto& p : parts.second) {
                    r.emplace(std::make_pair(parts.first, p), hosts[i]);
                }
            }
        }
        return decltype(preHeatLeaders())(std::move(r));
    }).thenError([](auto&& e){
        std::string err = "Get leaders failed!";
        LOG(ERROR) << err << e.what();
        return decltype(preHeatLeaders())(Status::Error(err));
    }).get();
}


}   // namespace storage
}   // namespace nebula
