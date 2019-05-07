/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "storage/client/StorageClient.h"

#define ID_HASH(id, numShards) \
    ((static_cast<uint64_t>(id)) % numShards + 1)

namespace nebula {
namespace storage {

StorageClient::StorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> threadPool,
                             meta::MetaClient *client)
        : ioThreadPool_(threadPool) {
    if (nullptr == client) {
        LOG(INFO) << "MetaClient is nullptr, create new one";
        static auto clientPtr = std::make_unique<meta::MetaClient>();
        static std::once_flag flag;
        std::call_once(flag, std::bind(&meta::MetaClient::init, clientPtr.get()));
        client_ = clientPtr.get();
    } else {
        client_ = client;
    }
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
        std::vector<VertexID> vertices,
        EdgeType edgeType,
        bool isOutBound,
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
        req.set_edge_type(isOutBound ? edgeType : -edgeType);
        req.set_filter(filter);
        req.set_return_columns(returnCols);
    }

    return collectResponse(
        evb, std::move(requests),
        [isOutBound](cpp2::StorageServiceAsyncClient* client,
                     const cpp2::GetNeighborsRequest& r) {
            if (isOutBound) {
                return client->future_getOutBound(r);
            } else {
                return client->future_getInBound(r);
            }
        });
}


folly::SemiFuture<StorageRpcResponse<cpp2::QueryStatsResponse>> StorageClient::neighborStats(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        EdgeType edgeType,
        bool isOutBound,
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
        req.set_edge_type(isOutBound ? edgeType : -edgeType);
        req.set_filter(filter);
        req.set_return_columns(returnCols);
    }

    return collectResponse(
        evb, std::move(requests),
        [isOutBound](cpp2::StorageServiceAsyncClient* client,
                     const cpp2::GetNeighborsRequest& r) {
            if (isOutBound) {
                return client->future_outBoundStats(r);
            } else {
                return client->future_inBoundStats(r);
            }
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


PartitionID StorageClient::partId(GraphSpaceID spaceId, int64_t id) const {
    CHECK(client_);
    auto parts = client_->partsNum(spaceId);
    auto s = ID_HASH(id, parts);
    CHECK_GE(s, 0U);
    return s;
}
}   // namespace storage
}   // namespace nebula
