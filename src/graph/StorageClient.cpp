/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/StorageClient.h"
#include "meta/HostManager.h"

namespace nebula {
namespace graph {

using namespace storage;
using namespace meta;


StorageClient::StorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> threadPool)
        : ioThreadPool_(threadPool) {}


folly::SemiFuture<cpp2::ExecResponse> StorageClient::addVertices(
        GraphSpaceID space,
        std::vector<storage::cpp2::Vertex> vertices,
        bool overwritable,
        folly::EventBase* evb) {
    auto clusters = HostManager::get(space)->clusterIdsToHosts(
        vertices,
        [] (const cpp2::Vertex& v) {
            return v.get_id();
        });

    std::unordered_map<HostAddr, cpp2::AddVerticesRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        cpp2::AddVerticesRequest req;
        req.set_space_id(space);
        req.set_overwritable(overwritable);
        req.set_vertices(std::move(c.second));
        requests.emplace(host, std::move(req));
    }

    return collectExecResponse(
        evb, std::move(requests),
        [](std::shared_ptr<cpp2::StorageServiceAsyncClient> client,
           const cpp2::AddVerticesRequest& r) {
            return client->future_addVertices(r);
        },
        [](HostAddr h,
           const cpp2::AddVerticesRequest& r,
           std::vector<cpp2::ResultCode>& results) {
            UNUSED(h);
            for (auto& part : r.get_vertices()) {
                cpp2::ResultCode result;
                result.set_code(cpp2::ErrorCode::E_RPC_FAILURE);
                result.set_part_id(part.first);
                results.emplace_back(std::move(result));
            }
        });
}


folly::SemiFuture<cpp2::ExecResponse> StorageClient::addEdges(
        GraphSpaceID space,
        std::vector<storage::cpp2::Edge> edges,
        bool overwritable,
        folly::EventBase* evb) {
    auto clusters = HostManager::get(space)->clusterIdsToHosts(
        edges,
        [] (const cpp2::Edge& e) {
            return e.get_key().get_src();
        });

    std::unordered_map<HostAddr, cpp2::AddEdgesRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        cpp2::AddEdgesRequest req;
        req.set_space_id(space);
        req.set_overwritable(overwritable);
        req.set_edges(std::move(c.second));
        requests.emplace(host, std::move(req));
    }

    return collectExecResponse(
        evb, std::move(requests),
        [](std::shared_ptr<cpp2::StorageServiceAsyncClient> client,
           const cpp2::AddEdgesRequest& r) {
            return client->future_addEdges(r);
        },
        [](HostAddr h,
           const cpp2::AddEdgesRequest& r,
           std::vector<cpp2::ResultCode>& results) {
            UNUSED(h);
            for (auto& edge : r.get_edges()) {
                cpp2::ResultCode result;
                result.set_code(cpp2::ErrorCode::E_RPC_FAILURE);
                result.set_part_id(edge.first);
                results.emplace_back(std::move(result));
            }
        });
}


folly::SemiFuture<storage::cpp2::QueryResponse> StorageClient::getNeighbors(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        EdgeType edgeType,
        bool isOutBound,
        std::string filter,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto clusters = HostManager::get(space)->clusterIdsToHosts(
        vertices,
        [] (const VertexID& v) {
            return v;
        });

    std::unordered_map<HostAddr, cpp2::GetNeighborsRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        cpp2::GetNeighborsRequest req;
        req.set_space_id(space);
        req.set_ids(std::move(c.second));
        // Make edge type a negative number when query in-bound
        req.set_edge_type(isOutBound ? edgeType : -edgeType);
        req.set_filter(filter);
        req.set_return_columns(returnCols);
        requests.emplace(host, std::move(req));
    }

    return collectQueryResponse(
        evb, std::move(requests),
        [isOutBound](std::shared_ptr<cpp2::StorageServiceAsyncClient> client,
           const cpp2::GetNeighborsRequest& r) {
            if (isOutBound) {
                return client->future_getOutBound(r);
            } else {
                return client->future_getInBound(r);
            }
        },
        [](HostAddr h,
           const cpp2::GetNeighborsRequest& r,
           std::vector<cpp2::ResultCode>& results) {
            UNUSED(h);
            for (auto& id : r.get_ids()) {
                cpp2::ResultCode result;
                result.set_code(cpp2::ErrorCode::E_RPC_FAILURE);
                result.set_part_id(id.first);
                results.emplace_back(std::move(result));
            }
        });
}


folly::SemiFuture<storage::cpp2::QueryResponse> StorageClient::neighborStats(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        EdgeType edgeType,
        bool isOutBound,
        std::string filter,
        std::vector<cpp2::PropStat> returnCols,
        folly::EventBase* evb) {
    auto clusters = HostManager::get(space)->clusterIdsToHosts(
        vertices,
        [] (const VertexID& v) {
            return v;
        });

    std::unordered_map<HostAddr, cpp2::NeighborsStatsRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        cpp2::NeighborsStatsRequest req;
        req.set_space_id(space);
        req.set_ids(std::move(c.second));
        // Make edge type a negative number when query in-bound
        req.set_edge_type(isOutBound ? edgeType : -edgeType);
        req.set_filter(filter);
        req.set_return_columns(returnCols);
        requests.emplace(host, std::move(req));
    }

    return collectQueryResponse(
        evb, std::move(requests),
        [isOutBound](std::shared_ptr<cpp2::StorageServiceAsyncClient> client,
           const cpp2::NeighborsStatsRequest& r) {
            if (isOutBound) {
                return client->future_outBoundStats(r);
            } else {
                return client->future_inBoundStats(r);
            }
        },
        [](HostAddr h,
           const cpp2::NeighborsStatsRequest& r,
           std::vector<cpp2::ResultCode>& results) {
            UNUSED(h);
            for (auto& id : r.get_ids()) {
                cpp2::ResultCode result;
                result.set_code(cpp2::ErrorCode::E_RPC_FAILURE);
                result.set_part_id(id.first);
                results.emplace_back(std::move(result));
            }
        });
}


folly::SemiFuture<storage::cpp2::QueryResponse> StorageClient::getVertexProps(
        GraphSpaceID space,
        std::vector<VertexID> vertices,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto clusters = HostManager::get(space)->clusterIdsToHosts(
        vertices,
        [] (const VertexID& v) {
            return v;
        });

    std::unordered_map<HostAddr, cpp2::VertexPropRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        cpp2::VertexPropRequest req;
        req.set_space_id(space);
        req.set_ids(std::move(c.second));
        req.set_return_columns(returnCols);
        requests.emplace(host, std::move(req));
    }

    return collectQueryResponse(
        evb, std::move(requests),
        [](std::shared_ptr<cpp2::StorageServiceAsyncClient> client,
           const cpp2::VertexPropRequest& r) {
            return client->future_getProps(r);
        },
        [](HostAddr h,
           const cpp2::VertexPropRequest& r,
           std::vector<cpp2::ResultCode>& results) {
            UNUSED(h);
            for (auto& id : r.get_ids()) {
                cpp2::ResultCode result;
                result.set_code(cpp2::ErrorCode::E_RPC_FAILURE);
                result.set_part_id(id.first);
                results.emplace_back(std::move(result));
            }
        });
}


folly::SemiFuture<storage::cpp2::QueryResponse> StorageClient::getEdgeProps(
        GraphSpaceID space,
        std::vector<cpp2::EdgeKey> edges,
        std::vector<cpp2::PropDef> returnCols,
        folly::EventBase* evb) {
    auto clusters = HostManager::get(space)->clusterIdsToHosts(
        edges,
        [] (const cpp2::EdgeKey& v) {
            return v.get_src();
        });

    std::unordered_map<HostAddr, cpp2::EdgePropRequest> requests;
    for (auto& c : clusters) {
        auto& host = c.first;
        cpp2::EdgePropRequest req;
        req.set_space_id(space);
        req.set_edges(std::move(c.second));
        req.set_return_columns(returnCols);
        requests.emplace(host, std::move(req));
    }

    return collectQueryResponse(
        evb, std::move(requests),
        [](std::shared_ptr<cpp2::StorageServiceAsyncClient> client,
           const cpp2::EdgePropRequest& r) {
            return client->future_getEdgeProps(r);
        },
        [](HostAddr h,
           const cpp2::EdgePropRequest& r,
           std::vector<cpp2::ResultCode>& results) {
            UNUSED(h);
            for (auto& e : r.get_edges()) {
                cpp2::ResultCode result;
                result.set_code(cpp2::ErrorCode::E_RPC_FAILURE);
                result.set_part_id(e.first);
                results.emplace_back(std::move(result));
            }
        });
}


}   // namespace graph
}   // namespace nebula
