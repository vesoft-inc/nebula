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
#include "meta/IndexManager.h"
#include "stats/StatsManager.h"
#include "storage/CommonUtils.h"
#include "stats/Stats.h"

DECLARE_int32(vertex_cache_num);
DECLARE_int32(vertex_cache_bucket_exp);
DECLARE_int32(reader_handlers);
DECLARE_string(reader_handlers_type);

namespace nebula {
namespace storage {

class StorageServiceHandler final : public cpp2::StorageServiceSvIf {
    FRIEND_TEST(StorageServiceHandlerTest, FutureAddVerticesTest);

public:
    StorageServiceHandler(kvstore::KVStore* kvstore,
                          meta::SchemaManager* schemaMan,
                          meta::IndexManager* indexMan,
                          meta::MetaClient* client)
        : kvstore_(kvstore)
        , schemaMan_(schemaMan)
        , indexMan_(indexMan)
        , metaClient_(client)
        , vertexCache_(FLAGS_vertex_cache_num, FLAGS_vertex_cache_bucket_exp) {
        if (FLAGS_reader_handlers_type == "io") {
            auto tf = std::make_shared<folly::NamedThreadFactory>("reader-pool");
            readerPool_ = std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_reader_handlers,
                                                                        std::move(tf));
        } else {
            if (FLAGS_reader_handlers_type != "cpu") {
                LOG(WARNING) << "Unknown value for --reader_handlers_type, using `cpu'";
            }
            using TM = apache::thrift::concurrency::PriorityThreadManager;
            auto pool = TM::newPriorityThreadManager(FLAGS_reader_handlers, true);
            pool->setNamePrefix("reader-pool");
            pool->start();
            readerPool_ = std::move(pool);
        }
        getBoundQpsStat_ = stats::Stats("storage", "get_bound");
        boundStatsQpsStat_ = stats::Stats("storage", "bound_stats");
        vertexPropsQpsStat_ = stats::Stats("storage", "vertex_props");
        edgePropsQpsStat_ = stats::Stats("storage", "edge_props");
        addVertexQpsStat_ = stats::Stats("storage", "add_vertex");
        addEdgeQpsStat_ = stats::Stats("storage", "add_edge");
        delVertexQpsStat_ = stats::Stats("storage", "del_vertex");
        updateVertexQpsStat_ = stats::Stats("storage", "update_vertex");
        updateEdgeQpsStat_ = stats::Stats("storage", "update_edge");
        scanEdgeQpsStat_ = stats::Stats("storage", "scan_edge");
        scanVertexQpsStat_ = stats::Stats("storage", "scan_vertex");
        getKvQpsStat_ = stats::Stats("storage", "get_kv");
        putKvQpsStat_ = stats::Stats("storage", "put_kv");
        lookupVerticesQpsStat_ = stats::Stats("storage", "lookup_vertices");
        lookupEdgesQpsStat_ = stats::Stats("storage", "lookup_edges");
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

    folly::Future<cpp2::ExecResponse>
    future_deleteEdges(const cpp2::DeleteEdgesRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_deleteVertices(const cpp2::DeleteVerticesRequest& req) override;

    folly::Future<cpp2::UpdateResponse>
    future_updateVertex(const cpp2::UpdateVertexRequest& req) override;

    folly::Future<cpp2::UpdateResponse>
    future_updateEdge(const cpp2::UpdateEdgeRequest& req) override;

    folly::Future<cpp2::ScanEdgeResponse>
    future_scanEdge(const cpp2::ScanEdgeRequest& req) override;

    folly::Future<cpp2::ScanVertexResponse>
    future_scanVertex(const cpp2::ScanVertexRequest& req) override;

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

    folly::Future<cpp2::AdminExecResp>
    future_checkPeers(const cpp2::CheckPeersReq& req) override;

    folly::Future<cpp2::GetLeaderResp>
    future_getLeaderPart(const cpp2::GetLeaderReq& req) override;

    folly::Future<cpp2::ExecResponse>
    future_put(const cpp2::PutRequest& req) override;

    folly::Future<cpp2::GeneralResponse>
    future_get(const cpp2::GetRequest& req) override;

    folly::Future<cpp2::GetUUIDResp>
    future_getUUID(const cpp2::GetUUIDReq& req) override;

    folly::Future<cpp2::AdminExecResp>
    future_createCheckpoint(const cpp2::CreateCPRequest& req) override;

    folly::Future<cpp2::AdminExecResp>
    future_dropCheckpoint(const cpp2::DropCPRequest& req) override;

    folly::Future<cpp2::AdminExecResp>
    future_blockingWrites(const cpp2::BlockingSignRequest& req) override;

    folly::Future<cpp2::AdminExecResp>
    future_rebuildTagIndex(const cpp2::RebuildIndexRequest& req) override;

    folly::Future<cpp2::AdminExecResp>
    future_rebuildEdgeIndex(const cpp2::RebuildIndexRequest& req) override;

    folly::Future<cpp2::LookUpIndexResp>
    future_lookUpIndex(const cpp2::LookUpIndexRequest& req) override;

private:
    kvstore::KVStore* kvstore_{nullptr};
    meta::SchemaManager* schemaMan_{nullptr};
    meta::IndexManager* indexMan_{nullptr};
    meta::MetaClient* metaClient_{nullptr};
    VertexCache vertexCache_;
    std::shared_ptr<folly::Executor> readerPool_;

    stats::Stats getBoundQpsStat_;
    stats::Stats boundStatsQpsStat_;
    stats::Stats vertexPropsQpsStat_;
    stats::Stats edgePropsQpsStat_;
    stats::Stats addVertexQpsStat_;
    stats::Stats addEdgeQpsStat_;
    stats::Stats delVertexQpsStat_;
    stats::Stats updateVertexQpsStat_;
    stats::Stats updateEdgeQpsStat_;
    stats::Stats scanEdgeQpsStat_;
    stats::Stats scanVertexQpsStat_;
    stats::Stats getKvQpsStat_;
    stats::Stats putKvQpsStat_;
    stats::Stats lookupVerticesQpsStat_;
    stats::Stats lookupEdgesQpsStat_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_STORAGESERVICEHANDLER_H_
