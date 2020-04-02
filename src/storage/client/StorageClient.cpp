/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/client/StorageClient.h"

DEFINE_int32(storage_client_timeout_ms, 60 * 1000, "storage client timeout");

namespace nebula {
namespace storage {

StorageClient::StorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> threadPool,
                             meta::MetaClient *client,
                             const std::string &serviceName)
        : ioThreadPool_(threadPool)
        , client_(client) {
    clientsMan_
        = std::make_unique<thrift::ThriftClientManager<storage::cpp2::StorageServiceAsyncClient>>();
    stats_ = std::make_unique<stats::Stats>(serviceName, "storageClient");
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
    auto status =
        clusterIdsToHosts(space, vertices, [](const cpp2::Vertex& v) { return v.get_id(); });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();
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
            return client->future_addVertices(r); },
        [](const std::pair<const PartitionID, std::vector<cpp2::Vertex>>& p) {
            return p.first;
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> StorageClient::addEdges(
        GraphSpaceID space,
        std::vector<storage::cpp2::Edge> edges,
        bool overwritable,
        folly::EventBase* evb) {
    auto status =
        clusterIdsToHosts(space, edges, [](const cpp2::Edge& e) { return e.get_key().get_src(); });
    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();

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
            return client->future_addEdges(r); },
        [](const std::pair<const PartitionID,
                           std::vector<cpp2::Edge>>& p) {
            return p.first;
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::QueryResponse>> StorageClient::getNeighbors(
        GraphSpaceID space,
        const std::vector<VertexID> &vertices,
        const std::vector<EdgeType> &edgeTypes,
        std::string filter,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space, vertices, [](const VertexID& v) { return v; });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::QueryResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();

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
            return client->future_getBound(r); },
        [](const std::pair<const PartitionID,
                           std::vector<VertexID>>& p) {
            return p.first;
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::QueryStatsResponse>> StorageClient::neighborStats(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        std::vector<EdgeType> edgeTypes,
        std::string filter,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space, vertices, [](const VertexID& v) { return v; });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::QueryStatsResponse>>(
            std::runtime_error(status.status().toString()));
    }
    auto& clusters = status.value();

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
            return client->future_boundStats(r); },
        [](const std::pair<const PartitionID,
                           std::vector<VertexID>>& p) {
            return p.first;
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::QueryResponse>> StorageClient::getVertexProps(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space, vertices, [](const VertexID& v) { return v; });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::QueryResponse>>(
            std::runtime_error(status.status().toString()));
    }
    auto& clusters = status.value();

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
            return client->future_getProps(r); },
        [](const std::pair<const PartitionID,
                           std::vector<VertexID>>& p) {
            return p.first;
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::EdgePropResponse>> StorageClient::getEdgeProps(
        GraphSpaceID space,
        std::vector<cpp2::EdgeKey> edges,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto status =
        clusterIdsToHosts(space, edges, [](const cpp2::EdgeKey& v) { return v.get_src(); });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::EdgePropResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();
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
            return client->future_getEdgeProps(r); },
        [](const std::pair<const PartitionID, std::vector<cpp2::EdgeKey>>& p) {
            return p.first;
        });
}

folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> StorageClient::deleteEdges(
    GraphSpaceID space,
    std::vector<storage::cpp2::EdgeKey> edges,
    folly::EventBase* evb) {
    auto status =
        clusterIdsToHosts(space, edges, [](const cpp2::EdgeKey& v) { return v.get_src(); });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(status.status().toString()));
    }
    auto& clusters = status.value();

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
            return client->future_deleteEdges(r); },
        [](const std::pair<const PartitionID,
                           std::vector<cpp2::EdgeKey>>& p) {
            return p.first;
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> StorageClient::deleteVertices(
    GraphSpaceID space,
    std::vector<VertexID> vids,
    folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space, vids, [] (const VertexID& v) { return v; });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();
    std::unordered_map<HostAddr, cpp2::DeleteVerticesRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
    }

    return collectResponse(
        evb,
        std::move(requests),
        [] (cpp2::StorageServiceAsyncClient* client,
            const cpp2::DeleteVerticesRequest& r) {
            return client->future_deleteVertices(r);
        },
        [](const std::pair<const PartitionID, std::vector<VertexID>>& p) {
            return p.first;
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

    auto status = partId(space, vertexId);
    if (!status.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(status.status());
    }

    auto part = status.value();
    auto metaStatus = getPartMeta(space, part);
    if (!metaStatus.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(metaStatus.status());
    }
    auto partMeta = metaStatus.value();
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
    auto status = partId(space, edgeKey.get_src());
    if (!status.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(status.status());
    }

    auto part = status.value();
    auto metaStatus = getPartMeta(space, part);
    if (!metaStatus.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(metaStatus.status());
    }
    auto partMeta = metaStatus.value();
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
    auto status = partId(space, hashValue);
    if (!status.ok()) {
        return folly::makeFuture<StatusOr<cpp2::GetUUIDResp>>(status.status());
    }

    auto part = status.value();
    auto metaStatus = getPartMeta(space, part);
    if (!metaStatus.ok()) {
        return folly::makeFuture<StatusOr<cpp2::GetUUIDResp>>(metaStatus.status());
    }
    auto partMeta = metaStatus.value();
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

StatusOr<PartitionID> StorageClient::partId(GraphSpaceID spaceId, int64_t id) const {
    auto status = partsNum(spaceId);
    if (!status.ok()) {
        return Status::Error("Space not found, spaceid: %d", spaceId);
    }

    auto parts = status.value();
    auto s = ID_HASH(id, parts);
    CHECK_GE(s, 0U);
    return s;
}

folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
StorageClient::put(GraphSpaceID space,
                   std::vector<nebula::cpp2::Pair> values,
                   folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space, values, [](const nebula::cpp2::Pair& v) {
        return std::hash<std::string>{}(v.get_key());
    });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();
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
                               return client->future_put(r); },
                           [](const std::pair<const PartitionID,
                                              std::vector<::nebula::cpp2::Pair>>& p) {
                               return p.first;
                           });
}

folly::SemiFuture<StorageRpcResponse<storage::cpp2::GeneralResponse>>
StorageClient::get(GraphSpaceID space,
                   const std::vector<std::string>& keys,
                   bool returnPartly,
                   folly::EventBase* evb) {
    auto status = clusterIdsToHosts(
        space, keys, [](const std::string& v) { return std::hash<std::string>{}(v); });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<storage::cpp2::GeneralResponse>>(
            std::runtime_error(status.status().toString()));
    }
    auto& clusters = status.value();

    std::unordered_map<HostAddr, cpp2::GetRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
        req.set_return_partly(returnPartly);
    }

    return collectResponse(evb, std::move(requests),
                           [](cpp2::StorageServiceAsyncClient* client,
                              const cpp2::GetRequest& r) {
                               return client->future_get(r); },
                           [](const std::pair<const PartitionID,
                                               std::vector<std::string>>& p) {
                               return p.first;
                           });
}

folly::SemiFuture<StorageRpcResponse<storage::cpp2::LookUpIndexResp>>
StorageClient::lookUpIndex(GraphSpaceID space,
                           IndexID indexId,
                           std::string filter,
                           std::vector<std::string> returnCols,
                           bool isEdge,
                           folly::EventBase *evb) {
    auto status = getHostParts(space);
    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<storage::cpp2::LookUpIndexResp>>(
            std::runtime_error(status.status().toString()));
    }
    auto& clusters = status.value();
    std::unordered_map<HostAddr, cpp2::LookUpIndexRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
        req.set_index_id(indexId);
        req.set_filter(filter);
        req.set_return_columns(returnCols);
        req.set_is_edge(isEdge);
    }
    return collectResponse(evb, std::move(requests),
                           [](cpp2::StorageServiceAsyncClient* client,
                              const cpp2::LookUpIndexRequest& r) {
                              return client->future_lookUpIndex(r);
                           },
                           [](const PartitionID& part) {
                               return part;
                           });
}

}   // namespace storage
}   // namespace nebula
