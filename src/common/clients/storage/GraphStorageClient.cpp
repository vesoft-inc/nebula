/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/clients/storage/GraphStorageClient.h"

namespace nebula {
namespace storage {

folly::SemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>
GraphStorageClient::getNeighbors(GraphSpaceID space,
                                 std::vector<std::string> colNames,
                                 const std::vector<Row>& vertices,
                                 const std::vector<EdgeType>& edgeTypes,
                                 cpp2::EdgeDirection edgeDirection,
                                 const std::vector<cpp2::StatProp>* statProps,
                                 const std::vector<cpp2::VertexProp>* vertexProps,
                                 const std::vector<cpp2::EdgeProp>* edgeProps,
                                 const std::vector<cpp2::Expr>* expressions,
                                 bool dedup,
                                 bool random,
                                 const std::vector<cpp2::OrderBy>& orderBy,
                                 int64_t limit,
                                 std::string filter,
                                 folly::EventBase* evb) {
    auto status = clusterIdsToHosts(
        space, vertices, [](const Row& r) -> const VertexID& {
            // The first column has to be the vid
            DCHECK_EQ(Value::Type::STRING, r.values[0].type());
            return r.values[0].getStr();
        });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();
    std::unordered_map<HostAddr, cpp2::GetNeighborsRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_column_names(std::move(colNames));
        req.set_parts(std::move(c.second));
        req.set_edge_types(edgeTypes);
        req.set_edge_direction(edgeDirection);
        req.set_dedup(dedup);
        req.set_random(random);
        if (statProps != nullptr) {
            req.set_stat_props(*statProps);
        }
        if (vertexProps != nullptr) {
            req.set_vertex_props(*vertexProps);
        }
        if (edgeProps != nullptr) {
            req.set_edge_props(*edgeProps);
        }
        if (expressions != nullptr) {
            req.set_expressions(*expressions);
        }
        if (!orderBy.empty()) {
            req.set_order_by(orderBy);
        }
        if (limit < std::numeric_limits<int64_t>::max()) {
            req.set_limit(limit);
        }
        if (filter.size() > 0) {
            req.set_filter(std::move(filter));
        }
    }

    return collectResponse(
        evb, std::move(requests),
        [] (cpp2::GraphStorageServiceAsyncClient* client,
            const cpp2::GetNeighborsRequest& r) {
            return client->future_getNeighbors(r);
        },
        [] (const std::pair<const PartitionID, std::vector<Row>>& p) {
            return p.first;
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
GraphStorageClient::addVertices(GraphSpaceID space,
                                std::vector<cpp2::NewVertex> vertices,
                                std::unordered_map<TagID, std::vector<std::string>> propNames,
                                bool overwritable,
                                folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space,
                                    std::move(vertices),
                                    [](const cpp2::NewVertex& v) -> const VertexID& {
        return v.get_id();
    });

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
        req.set_prop_names(std::move(propNames));
    }

    VLOG(3) << "requests size " << requests.size();
    return collectResponse(
        evb,
        std::move(requests),
        [] (cpp2::GraphStorageServiceAsyncClient* client,
            const cpp2::AddVerticesRequest& r) {
            return client->future_addVertices(r);
        },
        [] (const std::pair<const PartitionID, std::vector<cpp2::NewVertex>>& p) {
            return p.first;
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
GraphStorageClient::addEdges(GraphSpaceID space,
                             std::vector<cpp2::NewEdge> edges,
                             std::vector<std::string> propNames,
                             bool overwritable,
                             folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space,
                                    std::move(edges),
                                    [](const cpp2::NewEdge& e) -> const VertexID& {
        return e.get_key().get_src();
    });

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
        req.set_prop_names(std::move(propNames));
    }

    return collectResponse(
        evb,
        std::move(requests),
        [] (cpp2::GraphStorageServiceAsyncClient* client,
            const cpp2::AddEdgesRequest& r) {
            return client->future_addEdges(r);
        },
        [] (const std::pair<const PartitionID, std::vector<cpp2::NewEdge>>& p) {
            return p.first;
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::GetPropResponse>>
GraphStorageClient::getProps(GraphSpaceID space,
                             const DataSet& input,
                             const std::vector<cpp2::VertexProp>* vertexProps,
                             const std::vector<cpp2::EdgeProp>* edgeProps,
                             const std::vector<cpp2::Expr>* expressions,
                             bool dedup,
                             const std::vector<cpp2::OrderBy>& orderBy,
                             int64_t limit,
                             std::string filter,
                             folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space,
                                    input.rows,
                                    [](const Row& r) -> const VertexID& {
        DCHECK_EQ(Value::Type::STRING, r.values[0].type());
        return r.values[0].getStr();
    });

    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::GetPropResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();
    std::unordered_map<HostAddr, cpp2::GetPropRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_column_names(std::move(input.colNames));
        req.set_parts(std::move(c.second));
        req.set_dedup(dedup);
        if (vertexProps != nullptr) {
            req.set_vertex_props(*vertexProps);
        }
        if (edgeProps != nullptr) {
            req.set_edge_props(*edgeProps);
        }
        if (expressions != nullptr) {
            req.set_expressions(*expressions);
        }
        if (!orderBy.empty()) {
            req.set_order_by(orderBy);
        }
        if (limit < std::numeric_limits<int64_t>::max()) {
            req.set_limit(limit);
        }
        if (filter.size() > 0) {
            req.set_filter(std::move(filter));
        }
    }

    return collectResponse(
        evb,
        std::move(requests),
        [] (cpp2::GraphStorageServiceAsyncClient* client,
            const cpp2::GetPropRequest& r) {
            return client->future_getProps(r);
        },
        [] (const std::pair<const PartitionID, std::vector<Row>>& p) {
            return p.first;
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
GraphStorageClient::deleteEdges(GraphSpaceID space,
                                std::vector<cpp2::EdgeKey> edges,
                                folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space,
                                    std::move(edges),
                                    [](const cpp2::EdgeKey& v) -> const VertexID& {
        return v.get_src();
    });

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
        evb,
        std::move(requests),
        [] (cpp2::GraphStorageServiceAsyncClient* client,
            const cpp2::DeleteEdgesRequest& r) {
            return client->future_deleteEdges(r);
        },
        [] (const std::pair<const PartitionID, std::vector<cpp2::EdgeKey>>& p) {
            return p.first;
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
GraphStorageClient::deleteVertices(GraphSpaceID space,
                                   std::vector<VertexID> ids,
                                   folly::EventBase* evb) {
    auto status = clusterIdsToHosts(space,
                                    std::move(ids),
                                    [](const VertexID& v) -> const VertexID& {
        return v;
    });

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
        [] (cpp2::GraphStorageServiceAsyncClient* client,
            const cpp2::DeleteVerticesRequest& r) {
            return client->future_deleteVertices(r);
        },
        [] (const std::pair<const PartitionID, std::vector<VertexID>>& p) {
            return p.first;
        });
}


folly::Future<StatusOr<storage::cpp2::UpdateResponse>>
GraphStorageClient::updateVertex(GraphSpaceID space,
                                 VertexID vertexId,
                                 TagID tagId,
                                 std::vector<cpp2::UpdatedProp> updatedProps,
                                 bool insertable,
                                 std::vector<std::string> returnProps,
                                 std::string condition,
                                 folly::EventBase* evb) {
    std::pair<HostAddr, cpp2::UpdateVertexRequest> request;

    DCHECK(!!metaClient_);
    auto status = metaClient_->partId(space, vertexId);
    if (!status.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(status.status());
    }

    auto part = status.value();
    auto hStatus = getPartHosts(space, part);
    if (!hStatus.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(hStatus.status());
    }

    auto partHosts = hStatus.value();
    CHECK_GT(partHosts.hosts_.size(), 0U);
    const auto& host = this->getLeader(partHosts);
    request.first = std::move(host);
    cpp2::UpdateVertexRequest req;
    req.set_space_id(space);
    req.set_vertex_id(vertexId);
    req.set_tag_id(tagId);
    req.set_part_id(part);
    req.set_updated_props(std::move(updatedProps));
    req.set_return_props(std::move(returnProps));
    req.set_insertable(insertable);
    if (condition.size() > 0) {
        req.set_condition(std::move(condition));
    }
    request.second = std::move(req);

    return getResponse(
        evb, std::move(request),
        [] (cpp2::GraphStorageServiceAsyncClient* client,
            const cpp2::UpdateVertexRequest& r) {
            return client->future_updateVertex(r);
        });
}


folly::Future<StatusOr<storage::cpp2::UpdateResponse>>
GraphStorageClient::updateEdge(GraphSpaceID space,
                               storage::cpp2::EdgeKey edgeKey,
                               std::vector<cpp2::UpdatedProp> updatedProps,
                               bool insertable,
                               std::vector<std::string> returnProps,
                               std::string condition,
                               folly::EventBase* evb) {
    std::pair<HostAddr, cpp2::UpdateEdgeRequest> request;

    DCHECK(!!metaClient_);
    auto status = metaClient_->partId(space, edgeKey.get_src());
    if (!status.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(status.status());
    }

    auto part = status.value();
    auto hStatus = getPartHosts(space, part);
    if (!hStatus.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(hStatus.status());
    }
    auto partHosts = hStatus.value();
    CHECK_GT(partHosts.hosts_.size(), 0U);

    const auto& host = this->getLeader(partHosts);
    request.first = std::move(host);
    cpp2::UpdateEdgeRequest req;
    req.set_space_id(space);
    req.set_edge_key(edgeKey);
    req.set_part_id(part);
    req.set_updated_props(std::move(updatedProps));
    req.set_return_props(std::move(returnProps));
    req.set_insertable(insertable);
    if (condition.size() > 0) {
        req.set_condition(std::move(condition));
    }
    request.second = std::move(req);

    return getResponse(
        evb, std::move(request),
        [] (cpp2::GraphStorageServiceAsyncClient* client,
            const cpp2::UpdateEdgeRequest& r) {
            return client->future_updateEdge(r);
        });
}


folly::Future<StatusOr<cpp2::GetUUIDResp>>
GraphStorageClient::getUUID(GraphSpaceID space,
                            const std::string& name,
                            folly::EventBase* evb) {
    std::pair<HostAddr, cpp2::GetUUIDReq> request;
    DCHECK(!!metaClient_);
    auto status = metaClient_->partId(space, name);
    if (!status.ok()) {
        return folly::makeFuture<StatusOr<cpp2::GetUUIDResp>>(status.status());
    }

    auto part = status.value();
    auto hStatus = getPartHosts(space, part);
    if (!hStatus.ok()) {
        return folly::makeFuture<StatusOr<cpp2::GetUUIDResp>>(hStatus.status());
    }
    auto partHosts = hStatus.value();
    CHECK_GT(partHosts.hosts_.size(), 0U);

    const auto& leader = this->getLeader(partHosts);
    request.first = leader;
    cpp2::GetUUIDReq req;
    req.set_space_id(space);
    req.set_part_id(part);
    req.set_name(name);
    request.second = std::move(req);

    return getResponse(
        evb,
        std::move(request),
        [] (cpp2::GraphStorageServiceAsyncClient* client,
            const cpp2::GetUUIDReq& r) {
            return client->future_getUUID(r);
    });
}

folly::SemiFuture<StorageRpcResponse<cpp2::LookupIndexResp>>
GraphStorageClient::lookupIndex(GraphSpaceID space,
                                std::vector<storage::cpp2::IndexQueryContext> contexts,
                                bool isEdge,
                                int32_t tagOrEdge,
                                std::vector<std::string> returnCols,
                                folly::EventBase *evb) {
    auto status = getHostParts(space);
    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::LookupIndexResp>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();
    std::unordered_map<HostAddr, cpp2::LookupIndexRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
        req.set_contexts(std::move(contexts));
        req.set_is_edge(isEdge);
        req.set_tag_or_edge_id(tagOrEdge);
        req.set_return_columns(returnCols);
    }
    return collectResponse(evb,
                           std::move(requests),
                           [] (cpp2::GraphStorageServiceAsyncClient* client,
                               const cpp2::LookupIndexRequest& r) {
                               return client->future_lookupIndex(r); },
                           [] (const PartitionID& part) {
                               return part;
                           });
}

}   // namespace storage
}   // namespace nebula
