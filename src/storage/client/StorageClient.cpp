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

static thread_local thrift::ThriftClientManager<storage::cpp2::StorageServiceAsyncClient>
    storageClients;

StorageClient::StorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> threadPool,
                             meta::MetaClient *client,
                             const std::string &serviceName)
        : ioThreadPool_(threadPool)
        , client_(client) {
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

template<class Request, class RemoteFunc, class GetPartIDFunc, class Response>
folly::SemiFuture<StorageRpcResponse<Response>> StorageClient::collectResponse(
        folly::EventBase* evb,
        std::unordered_map<HostAddr, Request> requests,
        RemoteFunc&& remoteFunc,
        GetPartIDFunc getPartIDFunc) {
    auto context = std::make_shared<ResponseContext<Request, RemoteFunc, Response>>(
        requests.size(), std::move(remoteFunc));

    if (evb == nullptr) {
        DCHECK(!!ioThreadPool_);
        evb = ioThreadPool_->getEventBase();
    }

    time::Duration duration;
    for (auto& req : requests) {
        auto& host = req.first;
        auto spaceId = req.second.get_space_id();
        auto res = context->insertRequest(host, std::move(req.second));
        DCHECK(res.second);
        // Invoke the remote method
        folly::via(evb, [this,
                         evb,
                         context,
                         host,
                         spaceId,
                         res,
                         duration,
                         getPartIDFunc] () mutable {
            auto client = storageClients.client(host, evb, false, FLAGS_storage_client_timeout_ms);
            // Result is a pair of <Request&, bool>
            auto start = time::WallClock::fastNowInMicroSec();
            context->serverMethod(client.get(), *res.first)
            // Future process code will be executed on the IO thread
            // Since all requests are sent using the same eventbase, all then-callback
            // will be executed on the same IO thread
            .via(evb).then([this,
                            context,
                            host,
                            spaceId,
                            duration,
                            getPartIDFunc,
                            start] (folly::Try<Response>&& val) {
                auto& r = context->findRequest(host);
                if (val.hasException()) {
                    LOG(ERROR) << "Request to " << host << " failed: " << val.exception().what();
                    for (auto& part : r.parts) {
                        auto partId = getPartIDFunc(part);
                        VLOG(3) << "Exception! Failed part " << partId;
                        context->resp.failedParts().emplace(
                            partId,
                            storage::cpp2::ErrorCode::E_RPC_FAILURE);
                        invalidLeader(spaceId, partId);
                    }
                    context->resp.markFailure();
                } else {
                    auto resp = std::move(val.value());
                    auto& result = resp.get_result();
                    bool hasFailure{false};
                    for (auto& code : result.get_failed_codes()) {
                        VLOG(3) << "Failure! Failed part " << code.get_part_id()
                                << ", failed code " << static_cast<int32_t>(code.get_code());
                        hasFailure = true;
                        if (code.get_code() == storage::cpp2::ErrorCode::E_LEADER_CHANGED) {
                            auto* leader = code.get_leader();
                            if (leader != nullptr
                                    && leader->get_ip() != 0
                                    && leader->get_port() != 0) {
                                updateLeader(spaceId,
                                             code.get_part_id(),
                                             HostAddr(leader->get_ip(), leader->get_port()));
                            } else {
                                invalidLeader(spaceId, code.get_part_id());
                            }
                        } else if (code.get_code() == storage::cpp2::ErrorCode::E_PART_NOT_FOUND
                                || code.get_code() == storage::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
                            invalidLeader(spaceId, code.get_part_id());
                        } else {
                            // Simply keep the result
                            context->resp.failedParts().emplace(code.get_part_id(),
                                                                code.get_code());
                        }
                    }
                    if (hasFailure) {
                        context->resp.markFailure();
                    }

                    // Adjust the latency
                    auto latency = result.get_latency_in_us();
                    context->resp.setLatency(host,
                                             latency,
                                             time::WallClock::fastNowInMicroSec() - start);

                    // Keep the response
                    context->resp.responses().emplace_back(std::move(resp));
                }

                if (context->removeRequest(host)) {
                    // Received all responses
                    stats::Stats::addStatsValue(stats_.get(),
                                                context->resp.succeeded(),
                                                duration.elapsedInUSec());
                    context->promise.setValue(std::move(context->resp));
                }
            });
        });  // via
    }  // for

    if (context->finishSending()) {
        // Received all responses, most likely, all rpc failed
        context->promise.setValue(std::move(context->resp));
        stats::Stats::addStatsValue(stats_.get(),
                                    context->resp.succeeded(),
                                    duration.elapsedInUSec());
    }

    return context->promise.getSemiFuture();
}


template<class Request, class RemoteFunc, class Response>
folly::Future<StatusOr<Response>> StorageClient::getResponse(
        folly::EventBase* evb,
        std::pair<HostAddr, Request> request,
        RemoteFunc remoteFunc) {
    time::Duration duration;
    if (evb == nullptr) {
        DCHECK(!!ioThreadPool_);
        evb = ioThreadPool_->getEventBase();
    }
    folly::Promise<StatusOr<Response>> pro;
    auto f = pro.getFuture();
    folly::via(evb, [evb, request = std::move(request), remoteFunc = std::move(remoteFunc),
                     pro = std::move(pro), duration, this] () mutable {
        auto host = request.first;
        auto client = storageClients.client(host, evb, false, FLAGS_storage_client_timeout_ms);
        auto spaceId = request.second.get_space_id();
        auto partId = request.second.get_part_id();
        LOG(INFO) << "Send request to storage " << host;
        remoteFunc(client.get(), std::move(request.second)).via(evb)
             .then([spaceId, partId, p = std::move(pro),
                    duration, this] (folly::Try<Response>&& t) mutable {
            // exception occurred during RPC
            if (t.hasException()) {
                stats::Stats::addStatsValue(stats_.get(), false, duration.elapsedInUSec());
                p.setValue(Status::Error(folly::stringPrintf("RPC failure in StorageClient: %s",
                                                             t.exception().what().c_str())));
                invalidLeader(spaceId, partId);
                return;
            }
            auto&& resp = std::move(t.value());
            // leader changed
            auto& result = resp.get_result();
            for (auto& code : result.get_failed_codes()) {
                VLOG(3) << "Failure! Failed part " << code.get_part_id()
                        << ", failed code " << static_cast<int32_t>(code.get_code());
                if (code.get_code() == storage::cpp2::ErrorCode::E_LEADER_CHANGED) {
                    auto* leader = code.get_leader();
                    if (leader != nullptr && leader->get_ip() != 0 && leader->get_port() != 0) {
                        updateLeader(spaceId, code.get_part_id(),
                                     HostAddr(leader->get_ip(), leader->get_port()));
                    } else {
                        invalidLeader(spaceId, code.get_part_id());
                    }
                } else if (code.get_code() == storage::cpp2::ErrorCode::E_PART_NOT_FOUND ||
                           code.get_code() == storage::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
                    invalidLeader(spaceId, code.get_part_id());
                }
            }
            stats::Stats::addStatsValue(stats_.get(),
                                        result.get_failed_codes().empty(),
                                        duration.elapsedInUSec());
            p.setValue(std::move(resp));
        });
    });  // via
    return f;
}

}   // namespace storage
}   // namespace nebula
