/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_STORAGESERVICEHANDLER_H_
#define STORAGE_STORAGESERVICEHANDLER_H_

#include "base/Base.h"
#include "interface/gen-cpp2/StorageService.h"
#include "kvstore/include/KVStore.h"

namespace nebula {
namespace storage {

class StorageServiceHandler final : public cpp2::StorageServiceSvIf {
public:
    folly::Future<cpp2::QueryResponse>
    future_getOutBound(const cpp2::GetNeighborsRequest& req) override;

    folly::Future<cpp2::QueryResponse>
    future_getInBound(const cpp2::GetNeighborsRequest& req) override;

    folly::Future<cpp2::QueryResponse>
    future_outBoundStats(const cpp2::NeighborsStatsRequest& req) override;

    folly::Future<cpp2::QueryResponse>
    future_inBoundStats(const cpp2::NeighborsStatsRequest& req) override;

    folly::Future<cpp2::QueryResponse>
    future_getProps(const cpp2::VertexPropRequest& req) override;

    folly::Future<cpp2::QueryResponse>
    future_getEdgeProps(const cpp2::EdgePropRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_addVertices(const cpp2::AddVerticesRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_addEdges(const cpp2::AddEdgesRequest& req) override;

private:
    kvstore::KVStore* kvstore_ = nullptr;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_STORAGESERVICEHANDLER_H_
