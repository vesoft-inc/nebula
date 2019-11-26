/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_STORAGESERVICEHANDLER_H_
#define STORAGE_STORAGESERVICEHANDLER_H_

#include <gtest/gtest_prod.h>
#include "base/Base.h"
#include "interface/gen-cpp2/StorageService.h"
#include "kvstore/KVStore.h"
#include "meta/SchemaManager.h"
#include "stats/StatsManager.h"
#include "storage/StorageStats.h"
#include "storage/CommonUtils.h"

DECLARE_int32(vertex_cache_num);
DECLARE_int32(vertex_cache_bucket_exp);

namespace nebula {
namespace storage {

class StorageServiceHandler final : public cpp2::StorageServiceSvIf {
    FRIEND_TEST(StorageServiceHandlerTest, FutureAddVerticesTest);

public:
    StorageServiceHandler(kvstore::KVStore* kvstore,
                          meta::SchemaManager* schemaMan,
                          meta::MetaClient* client)
        : kvstore_(kvstore)
        , schemaMan_(schemaMan)
        , metaClient_(client)
        , vertexCache_(FLAGS_vertex_cache_num, FLAGS_vertex_cache_bucket_exp) {
        getBoundQpsStat_ = StorageStats("get_bound");
        boundStatsQpsStat_ = StorageStats("bound_stats");
        vertexPropsQpsStat_ = StorageStats("vertex_props");
        edgePropsQpsStat_ = StorageStats("edge_props");
        addVertexQpsStat_ = StorageStats("add_vertex");
        addEdgeQpsStat_ = StorageStats("add_edge");
        delVertexQpsStat_ = StorageStats("del_vertex");
        updateVertexQpsStat_ = StorageStats("update_vertex");
        updateEdgeQpsStat_ = StorageStats("update_edge");
        getKvQpsStat_ = StorageStats("get_kv");
        putKvQpsStat_ = StorageStats("put_kv");
    }

    folly::Future<cpp2::QueryResponse>
    future_getBound(const cpp2::GetNeighborsRequest& req) override;

    folly::Future<cpp2::QueryStatsResponse>
    future_boundStats(const cpp2::GetNeighborsRequest& req) override;

    folly::Future<cpp2::QueryResponse>
    future_getProps(const cpp2::VertexPropRequest& req) override;

    folly::Future<cpp2::EdgePropResponse>
    future_getEdgeProps(const cpp2::EdgePropRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_addVertices(const cpp2::AddVerticesRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_addEdges(const cpp2::AddEdgesRequest& req) override;

    folly::Future<cpp2::EdgeKeyResponse>
    future_getEdgeKeys(const cpp2::EdgeKeyRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_deleteEdges(const cpp2::DeleteEdgesRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_deleteVertex(const cpp2::DeleteVertexRequest& req) override;

    folly::Future<cpp2::UpdateResponse>
    future_updateVertex(const cpp2::UpdateVertexRequest& req) override;

    folly::Future<cpp2::UpdateResponse>
    future_updateEdge(const cpp2::UpdateEdgeRequest& req) override;

    // Admin operations
    folly::Future<cpp2::AdminExecResp>
    future_transLeader(const cpp2::TransLeaderReq& req) override;

    folly::Future<cpp2::AdminExecResp>
    future_addPart(const cpp2::AddPartReq& req) override;

    folly::Future<cpp2::AdminExecResp>
    future_addLearner(const cpp2::AddLearnerReq& req) override;

    folly::Future<cpp2::AdminExecResp>
    future_waitingForCatchUpData(const cpp2::CatchUpDataReq& req) override;

    folly::Future<cpp2::AdminExecResp>
    future_removePart(const cpp2::RemovePartReq& req) override;

    folly::Future<cpp2::AdminExecResp>
    future_memberChange(const cpp2::MemberChangeReq& req) override;

    folly::Future<cpp2::GetLeaderResp>
    future_getLeaderPart(const cpp2::GetLeaderReq& req) override;

    folly::Future<cpp2::ExecResponse>
    future_put(const cpp2::PutRequest& req) override;

    folly::Future<cpp2::GeneralResponse>
    future_get(const cpp2::GetRequest& req) override;

    folly::Future<cpp2::GetUUIDResp>
    future_getUUID(const cpp2::GetUUIDReq& req) override;

private:
    kvstore::KVStore* kvstore_ = nullptr;
    meta::SchemaManager* schemaMan_ = nullptr;
    meta::MetaClient* metaClient_ = nullptr;
    VertexCache vertexCache_;

    StorageStats getBoundQpsStat_;
    StorageStats boundStatsQpsStat_;
    StorageStats vertexPropsQpsStat_;
    StorageStats edgePropsQpsStat_;
    StorageStats addVertexQpsStat_;
    StorageStats addEdgeQpsStat_;
    StorageStats delVertexQpsStat_;
    StorageStats updateVertexQpsStat_;
    StorageStats updateEdgeQpsStat_;
    StorageStats getKvQpsStat_;
    StorageStats putKvQpsStat_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_STORAGESERVICEHANDLER_H_
