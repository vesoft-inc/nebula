/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_GRAPHSTORAGESERVICEHANDLER_H_
#define STORAGE_GRAPHSTORAGESERVICEHANDLER_H_

#include "base/Base.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include "interface/gen-cpp2/GraphStorageService.h"
#include "stats/Stats.h"
#include "storage/CommonUtils.h"
#include "stats/StatsManager.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class StorageEnv;

class GraphStorageServiceHandler final : public cpp2::GraphStorageServiceSvIf {
public:
    explicit GraphStorageServiceHandler(StorageEnv* env)
        : env_(env)
        , vertexCache_(FLAGS_vertex_cache_num, FLAGS_vertex_cache_bucket_exp)
        , readerPool_(std::make_unique<folly::IOThreadPoolExecutor>(FLAGS_reader_handlers)) {
        addVerticesQpsStat_ = stats::Stats("storage", "add_vertices");
        addEdgesQpsStat_ = stats::Stats("storage", "add_edges");
        delVerticesQpsStat_ = stats::Stats("storage", "del_vertices");
        delEdgesQpsStat_ = stats::Stats("storage", "del_edges");
    }

    // Vertice section
    folly::Future<cpp2::ExecResponse>
    future_addVertices(const cpp2::AddVerticesRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_deleteVertices(const cpp2::DeleteVerticesRequest& req) override;

    // Edge section
    folly::Future<cpp2::ExecResponse>
    future_addEdges(const cpp2::AddEdgesRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_deleteEdges(const cpp2::DeleteEdgesRequest& req) override;

    folly::Future<cpp2::GetNeighborsResponse>
    future_getNeighbors(const cpp2::GetNeighborsRequest& req) override;

private:
    StorageEnv*                                     env_{nullptr};
    VertexCache                                     vertexCache_;
    std::unique_ptr<folly::IOThreadPoolExecutor>    readerPool_;

    stats::Stats                                    addVerticesQpsStat_;
    stats::Stats                                    addEdgesQpsStat_;
    stats::Stats                                    delVerticesQpsStat_;
    stats::Stats                                    delEdgesQpsStat_;
    stats::Stats                                    getNeighborsQpsStat_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_GRAPHSTORAGESERVICEHANDLER_H_
