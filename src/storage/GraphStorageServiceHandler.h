/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_GRAPHSTORAGESERVICEHANDLER_H_
#define STORAGE_GRAPHSTORAGESERVICEHANDLER_H_

#include "common/base/Base.h"
#include "common/stats/Stats.h"
#include "common/stats/StatsManager.h"
#include "common/interface/gen-cpp2/GraphStorageService.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class StorageEnv;

class GraphStorageServiceHandler final : public cpp2::GraphStorageServiceSvIf {
public:
    explicit GraphStorageServiceHandler(StorageEnv* env)
        : env_(env)
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
        addVerticesQpsStat_ = stats::Stats("storage", "add_vertices");
        addEdgesQpsStat_ = stats::Stats("storage", "add_edges");
        delVerticesQpsStat_ = stats::Stats("storage", "del_vertices");
        delEdgesQpsStat_ = stats::Stats("storage", "del_edges");
        updateVertexQpsStat_ = stats::Stats("storage", "update_vertex");
        updateEdgeQpsStat_ = stats::Stats("storage", "update_edge");
        getNeighborsQpsStat_ = stats::Stats("storage", "get_neighbors");
        getPropQpsStat_ = stats::Stats("storage", "get_prop");
        scanVertexQpsStat_ = stats::Stats("storage", "scan_vertex");
        scanEdgeQpsStat_ = stats::Stats("storage", "scan_edge");
    }

    // Vertice section
    folly::Future<cpp2::ExecResponse>
    future_addVertices(const cpp2::AddVerticesRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_deleteVertices(const cpp2::DeleteVerticesRequest& req) override;

    folly::Future<cpp2::UpdateResponse>
    future_updateVertex(const cpp2::UpdateVertexRequest& req) override;

    // Edge section
    folly::Future<cpp2::ExecResponse>
    future_addEdges(const cpp2::AddEdgesRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_deleteEdges(const cpp2::DeleteEdgesRequest& req) override;

    folly::Future<cpp2::UpdateResponse>
    future_updateEdge(const cpp2::UpdateEdgeRequest& req) override;

    folly::Future<cpp2::GetNeighborsResponse>
    future_getNeighbors(const cpp2::GetNeighborsRequest& req) override;

    folly::Future<cpp2::GetPropResponse>
    future_getProps(const cpp2::GetPropRequest& req) override;

    folly::Future<cpp2::LookupIndexResp>
    future_lookupIndex(const cpp2::LookupIndexRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_addEdgesAtomic(const cpp2::AddEdgesRequest& req) override;

    folly::Future<cpp2::ScanVertexResponse>
    future_scanVertex(const cpp2::ScanVertexRequest& req) override;

    folly::Future<cpp2::ScanEdgeResponse>
    future_scanEdge(const cpp2::ScanEdgeRequest& req) override;

    folly::Future<cpp2::GetUUIDResp>
    future_getUUID(const cpp2::GetUUIDReq& req) override;

private:
    StorageEnv*                                     env_{nullptr};
    VertexCache                                     vertexCache_;
    std::shared_ptr<folly::Executor>                readerPool_;

    stats::Stats                                    addVerticesQpsStat_;
    stats::Stats                                    addEdgesQpsStat_;
    stats::Stats                                    delVerticesQpsStat_;
    stats::Stats                                    delEdgesQpsStat_;
    stats::Stats                                    updateVertexQpsStat_;
    stats::Stats                                    updateEdgeQpsStat_;
    stats::Stats                                    getNeighborsQpsStat_;
    stats::Stats                                    getPropQpsStat_;
    stats::Stats                                    lookupQpsStat_;
    stats::Stats                                    scanVertexQpsStat_;
    stats::Stats                                    scanEdgeQpsStat_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_GRAPHSTORAGESERVICEHANDLER_H_
