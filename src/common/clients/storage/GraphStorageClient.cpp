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
    auto cbStatus = getIdFromRow(space, false);
    if (!cbStatus.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>(
            std::runtime_error(cbStatus.status().toString()));
    }

    auto status = clusterIdsToHosts(space, vertices, std::move(cbStatus).value());
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
        req.set_column_names(colNames);
        req.set_parts(std::move(c.second));

        cpp2::TraverseSpec spec;
        spec.set_edge_types(edgeTypes);
        spec.set_edge_direction(edgeDirection);
        spec.set_dedup(dedup);
        spec.set_random(random);
        if (statProps != nullptr) {
            spec.set_stat_props(*statProps);
        }
        if (vertexProps != nullptr) {
            spec.set_vertex_props(*vertexProps);
        }
        if (edgeProps != nullptr) {
            spec.set_edge_props(*edgeProps);
        }
        if (expressions != nullptr) {
            spec.set_expressions(*expressions);
        }
        if (!orderBy.empty()) {
            spec.set_order_by(orderBy);
        }
        spec.set_limit(limit);
        if (filter.size() > 0) {
            spec.set_filter(filter);
        }
        req.set_traverse_spec(std::move(spec));
    }

    return collectResponse(
        evb, std::move(requests),
        [] (cpp2::GraphStorageServiceAsyncClient* client,
            const cpp2::GetNeighborsRequest& r) {
            return client->future_getNeighbors(r);
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
GraphStorageClient::addVertices(GraphSpaceID space,
                                std::vector<cpp2::NewVertex> vertices,
                                std::unordered_map<TagID, std::vector<std::string>> propNames,
                                bool ifNotExists,
                                folly::EventBase* evb) {
    auto cbStatus = getIdFromNewVertex(space);
    if (!cbStatus.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(cbStatus.status().toString()));
    }

    auto status = clusterIdsToHosts(space, std::move(vertices), std::move(cbStatus).value());
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
        req.set_if_not_exists(ifNotExists);
        req.set_parts(std::move(c.second));
        req.set_prop_names(propNames);
    }

    VLOG(3) << "requests size " << requests.size();
    return collectResponse(
        evb,
        std::move(requests),
        [] (cpp2::GraphStorageServiceAsyncClient* client,
            const cpp2::AddVerticesRequest& r) {
            return client->future_addVertices(r);
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
GraphStorageClient::addEdges(GraphSpaceID space,
                             std::vector<cpp2::NewEdge> edges,
                             std::vector<std::string> propNames,
                             bool ifNotExists,
                             folly::EventBase* evb,
                             bool useToss) {
    auto cbStatus = getIdFromNewEdge(space);
    if (!cbStatus.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(cbStatus.status().toString()));
    }

    auto status = clusterIdsToHosts(space, std::move(edges), std::move(cbStatus).value());
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
        req.set_if_not_exists(ifNotExists);
        req.set_parts(std::move(c.second));
        req.set_prop_names(propNames);
    }
    return collectResponse(
        evb,
        std::move(requests),
        [=](cpp2::GraphStorageServiceAsyncClient* client, const cpp2::AddEdgesRequest& r) {
            return useToss ? client->future_addEdgesAtomic(r)
                           : client->future_addEdges(r);
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
    auto cbStatus = getIdFromRow(space, edgeProps != nullptr);
    if (!cbStatus.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::GetPropResponse>>(
            std::runtime_error(cbStatus.status().toString()));
    }

    auto status = clusterIdsToHosts(space, input.rows, std::move(cbStatus).value());
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
        req.set_limit(limit);
        if (filter.size() > 0) {
            req.set_filter(filter);
        }
    }

    return collectResponse(
        evb,
        std::move(requests),
        [] (cpp2::GraphStorageServiceAsyncClient* client,
            const cpp2::GetPropRequest& r) {
            return client->future_getProps(r);
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
GraphStorageClient::deleteEdges(GraphSpaceID space,
                                std::vector<cpp2::EdgeKey> edges,
                                folly::EventBase* evb) {
    auto cbStatus = getIdFromEdgeKey(space);
    if (!cbStatus.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(cbStatus.status().toString()));
    }

    auto status = clusterIdsToHosts(space, std::move(edges), std::move(cbStatus).value());
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
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>>
GraphStorageClient::deleteVertices(GraphSpaceID space,
                                   std::vector<Value> ids,
                                   folly::EventBase* evb) {
    auto cbStatus = getIdFromValue(space);
    if (!cbStatus.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
            std::runtime_error(cbStatus.status().toString()));
    }

    auto status = clusterIdsToHosts(space, std::move(ids), std::move(cbStatus).value());
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
        });
}


folly::Future<StatusOr<storage::cpp2::UpdateResponse>>
GraphStorageClient::updateVertex(GraphSpaceID space,
                                 Value vertexId,
                                 TagID tagId,
                                 std::vector<cpp2::UpdatedProp> updatedProps,
                                 bool insertable,
                                 std::vector<std::string> returnProps,
                                 std::string condition,
                                 folly::EventBase* evb) {
    auto cbStatus = getIdFromValue(space);
    if (!cbStatus.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(cbStatus.status());
    }

    std::pair<HostAddr, cpp2::UpdateVertexRequest> request;

    DCHECK(!!metaClient_);
    auto status = metaClient_->partsNum(space);
    if (!status.ok()) {
        return Status::Error("Space not found, spaceid: %d", space);
    }
    auto numParts = status.value();
    status = metaClient_->partId(numParts, std::move(cbStatus).value()(vertexId));
    if (!status.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(status.status());
    }

    auto part = status.value();
    auto host = this->getLeader(space, part);
    if (!host.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(host.status());
    }
    request.first = std::move(host).value();
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
    auto cbStatus = getIdFromEdgeKey(space);
    if (!cbStatus.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(cbStatus.status());
    }

    std::pair<HostAddr, cpp2::UpdateEdgeRequest> request;

    DCHECK(!!metaClient_);
    auto status = metaClient_->partsNum(space);
    if (!status.ok()) {
        return Status::Error("Space not found, spaceid: %d", space);
    }
    auto numParts = status.value();
    status = metaClient_->partId(numParts, std::move(cbStatus).value()(edgeKey));
    if (!status.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(status.status());
    }

    auto part = status.value();
    auto host = this->getLeader(space, part);
    if (!host.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(host.status());
    }
    request.first = std::move(host).value();
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
    auto status = metaClient_->partsNum(space);
    if (!status.ok()) {
        return Status::Error("Space not found, spaceid: %d", space);
    }
    auto numParts = status.value();
    status = metaClient_->partId(numParts, name);
    if (!status.ok()) {
        return folly::makeFuture<StatusOr<cpp2::GetUUIDResp>>(status.status());
    }

    auto part = status.value();
    auto host = this->getLeader(space, part);
    if (!host.ok()) {
        return folly::makeFuture<StatusOr<storage::cpp2::GetUUIDResp>>(host.status());
    }
    request.first = std::move(host).value();
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
                                const std::vector<storage::cpp2::IndexQueryContext>& contexts,
                                bool isEdge,
                                int32_t tagOrEdge,
                                const std::vector<std::string>& returnCols,
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
        req.set_return_columns(returnCols);

        cpp2::IndexSpec spec;
        spec.set_contexts(contexts);
        spec.set_is_edge(isEdge);
        spec.set_tag_or_edge_id(tagOrEdge);

        req.set_indices(spec);
    }

    return collectResponse(evb,
                           std::move(requests),
                           [] (cpp2::GraphStorageServiceAsyncClient* client,
                               const cpp2::LookupIndexRequest& r) {
                               return client->future_lookupIndex(r); });
}


folly::SemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>
GraphStorageClient::lookupAndTraverse(GraphSpaceID space,
                                      cpp2::IndexSpec indexSpec,
                                      cpp2::TraverseSpec traverseSpec,
                                      folly::EventBase* evb) {
    auto status = getHostParts(space);
    if (!status.ok()) {
        return folly::makeFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>(
            std::runtime_error(status.status().toString()));
    }

    auto& clusters = status.value();
    std::unordered_map<HostAddr, cpp2::LookupAndTraverseRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        auto& req = requests[host];
        req.set_space_id(space);
        req.set_parts(std::move(c.second));
        req.set_indices(indexSpec);
        req.set_traverse_spec(traverseSpec);
    }

    return collectResponse(evb,
                           std::move(requests),
                           [] (cpp2::GraphStorageServiceAsyncClient* client,
                               const cpp2::LookupAndTraverseRequest& r) {
                               return client->future_lookupAndTraverse(r); });
}

folly::Future<StatusOr<cpp2::ScanEdgeResponse>>
GraphStorageClient::scanEdge(cpp2::ScanEdgeRequest req,
                             folly::EventBase* evb) {
    std::pair<HostAddr, cpp2::ScanEdgeRequest> request;
    auto host = this->getLeader(req.get_space_id(), req.get_part_id());
    if (!host.ok()) {
        return folly::makeFuture<StatusOr<cpp2::ScanEdgeResponse>>(host.status());
    }
    request.first = std::move(host).value();
    request.second = std::move(req);

    return getResponse(
        evb, std::move(request),
        [] (cpp2::GraphStorageServiceAsyncClient* client,
            const cpp2::ScanEdgeRequest& r) {
            return client->future_scanEdge(r);
        });
}

folly::Future<StatusOr<cpp2::ScanVertexResponse>>
GraphStorageClient::scanVertex(cpp2::ScanVertexRequest req,
                               folly::EventBase* evb) {
    std::pair<HostAddr, cpp2::ScanVertexRequest> request;
    auto host = this->getLeader(req.get_space_id(), req.get_part_id());
    if (!host.ok()) {
        return folly::makeFuture<StatusOr<cpp2::ScanVertexResponse>>(host.status());
    }
    request.first = std::move(host).value();
    request.second = std::move(req);

    return getResponse(
        evb,
        std::move(request),
        [](cpp2::GraphStorageServiceAsyncClient* client, const cpp2::ScanVertexRequest& r) {
            return client->future_scanVertex(r);
        });
}

StatusOr<std::function<const VertexID&(const Row&)>> GraphStorageClient::getIdFromRow(
    GraphSpaceID space, bool isEdgeProps) const {
    auto vidTypeStatus = metaClient_->getSpaceVidType(space);
    if (!vidTypeStatus) {
        return vidTypeStatus.status();
    }
    auto vidType = std::move(vidTypeStatus).value();

    std::function<const VertexID&(const Row&)> cb;
    if (vidType == meta::cpp2::PropertyType::INT64) {
        if (isEdgeProps) {
            cb = [](const Row& r) -> const VertexID& {
                // The first column has to be the src, the thrid column has to be the dst
                DCHECK_EQ(Value::Type::INT, r.values[0].type());
                DCHECK_EQ(Value::Type::INT, r.values[3].type());
                auto& mutableR = const_cast<Row&>(r);
                mutableR.values[0] = Value(
                        std::string(reinterpret_cast<const char*>(&r.values[0].getInt()), 8));
                mutableR.values[3] = Value(
                        std::string(reinterpret_cast<const char*>(&r.values[3].getInt()), 8));
                return mutableR.values[0].getStr();
            };
        } else {
            cb = [](const Row& r) -> const VertexID& {
                // The first column has to be the vid
                DCHECK_EQ(Value::Type::INT, r.values[0].type());
                auto& mutableR = const_cast<Row&>(r);
                mutableR.values[0] = Value(
                        std::string(reinterpret_cast<const char*>(&r.values[0].getInt()), 8));
                return mutableR.values[0].getStr();
            };
        }
    } else if (vidType == meta::cpp2::PropertyType::FIXED_STRING) {
        cb = [](const Row& r) -> const VertexID& {
                // The first column has to be the vid
                DCHECK_EQ(Value::Type::STRING, r.values[0].type());
                return r.values[0].getStr();
            };
    } else {
        return Status::Error("Only support integer/string type vid.");
    }

    return cb;
}

StatusOr<std::function<const VertexID&(const cpp2::NewVertex&)>>
GraphStorageClient::getIdFromNewVertex(GraphSpaceID space) const {
    auto vidTypeStatus = metaClient_->getSpaceVidType(space);
    if (!vidTypeStatus) {
        return vidTypeStatus.status();
    }
    auto vidType = std::move(vidTypeStatus).value();

    std::function<const VertexID&(const cpp2::NewVertex&)> cb;
    if (vidType == meta::cpp2::PropertyType::INT64) {
        cb = [](const cpp2::NewVertex& v) -> const VertexID& {
                DCHECK_EQ(Value::Type::INT, v.get_id().type());
                auto& mutableV = const_cast<cpp2::NewVertex&>(v);
                mutableV.set_id(Value(
                        std::string(reinterpret_cast<const char*>(&v.get_id().getInt()), 8)));
                return mutableV.get_id().getStr();
            };
    } else if (vidType == meta::cpp2::PropertyType::FIXED_STRING) {
        cb = [](const cpp2::NewVertex& v) -> const VertexID& {
                DCHECK_EQ(Value::Type::STRING, v.get_id().type());
                return v.get_id().getStr();
            };
    } else {
        return Status::Error("Only support integer/string type vid.");
    }
    return cb;
}

StatusOr<std::function<const VertexID&(const cpp2::NewEdge&)>> GraphStorageClient::getIdFromNewEdge(
    GraphSpaceID space) const {
    auto vidTypeStatus = metaClient_->getSpaceVidType(space);
    if (!vidTypeStatus) {
        return vidTypeStatus.status();
    }
    auto vidType = std::move(vidTypeStatus).value();

    std::function<const VertexID&(const cpp2::NewEdge&)> cb;
    if (vidType == meta::cpp2::PropertyType::INT64) {
        cb = [](const cpp2::NewEdge& e) -> const VertexID& {
                DCHECK_EQ(Value::Type::INT, e.get_key().get_src().type());
                DCHECK_EQ(Value::Type::INT, e.get_key().get_dst().type());
                auto& mutableE = const_cast<cpp2::NewEdge&>(e);
                (*mutableE.key_ref()).src_ref().emplace(
                        Value(std::string(
                                reinterpret_cast<const char*>(&e.get_key().get_src().getInt()),
                                8)));
                (*mutableE.key_ref()).dst_ref().emplace(
                        Value(std::string(
                                reinterpret_cast<const char*>(&e.get_key().get_dst().getInt()),
                                8)));
                return mutableE.get_key().get_src().getStr();
            };
    } else if (vidType == meta::cpp2::PropertyType::FIXED_STRING) {
        cb = [](const cpp2::NewEdge& e) -> const VertexID& {
                DCHECK_EQ(Value::Type::STRING, e.get_key().get_src().type());
                DCHECK_EQ(Value::Type::STRING, e.get_key().get_dst().type());
                return e.get_key().get_src().getStr();
            };
    } else {
        return Status::Error("Only support integer/string type vid.");
    }
    return cb;
}

StatusOr<std::function<const VertexID&(const cpp2::EdgeKey&)>> GraphStorageClient::getIdFromEdgeKey(
    GraphSpaceID space) const {
    auto vidTypeStatus = metaClient_->getSpaceVidType(space);
    if (!vidTypeStatus) {
        return vidTypeStatus.status();
    }
    auto vidType = std::move(vidTypeStatus).value();

    std::function<const VertexID&(const cpp2::EdgeKey&)> cb;
    if (vidType == meta::cpp2::PropertyType::INT64) {
        cb = [](const cpp2::EdgeKey& eKey) -> const VertexID& {
                DCHECK_EQ(Value::Type::INT, eKey.get_src().type());
                DCHECK_EQ(Value::Type::INT, eKey.get_dst().type());
                auto& mutableEK = const_cast<cpp2::EdgeKey&>(eKey);
                mutableEK.set_src(Value(
                        std::string(reinterpret_cast<const char*>(&eKey.get_src().getInt()), 8)));
                mutableEK.set_dst(Value(
                        std::string(reinterpret_cast<const char*>(&eKey.get_dst().getInt()), 8)));
                return mutableEK.get_src().getStr();
            };
    } else if (vidType == meta::cpp2::PropertyType::FIXED_STRING) {
        cb = [](const cpp2::EdgeKey& eKey) -> const VertexID& {
                DCHECK_EQ(Value::Type::STRING, eKey.get_src().type());
                DCHECK_EQ(Value::Type::STRING, eKey.get_dst().type());
                return eKey.get_src().getStr();
            };
    } else {
        return Status::Error("Only support integer/string type vid.");
    }
    return cb;
}

StatusOr<std::function<const VertexID&(const Value&)>> GraphStorageClient::getIdFromValue(
    GraphSpaceID space) const {
    auto vidTypeStatus = metaClient_->getSpaceVidType(space);
    if (!vidTypeStatus) {
        return vidTypeStatus.status();
    }
    auto vidType = std::move(vidTypeStatus).value();

    std::function<const VertexID&(const Value&)> cb;
    if (vidType == meta::cpp2::PropertyType::INT64) {
        cb = [](const Value& v) -> const VertexID& {
                DCHECK_EQ(Value::Type::INT, v.type());
                auto& mutableV = const_cast<Value&>(v);
                mutableV = Value(
                        std::string(reinterpret_cast<const char*>(&v.getInt()), 8));
                return mutableV.getStr();
            };
    } else if (vidType == meta::cpp2::PropertyType::FIXED_STRING) {
        cb = [](const Value& v) -> const VertexID& {
                DCHECK_EQ(Value::Type::STRING, v.type());
                return v.getStr();
            };
    } else {
        return Status::Error("Only support integer/string type vid.");
    }
    return cb;
}
}   // namespace storage
}   // namespace nebula
