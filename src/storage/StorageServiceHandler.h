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

namespace nebula {
namespace storage {

class StorageServiceHandler final : public cpp2::StorageServiceSvIf {
    FRIEND_TEST(StorageServiceHandlerTest, FutureAddVerticesTest);

public:
    StorageServiceHandler(kvstore::KVStore* kvstore,
                          meta::SchemaManager* schemaMan)
        : kvstore_(kvstore)
        , schemaMan_(schemaMan) {}

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

private:
    kvstore::KVStore* kvstore_ = nullptr;
    meta::SchemaManager* schemaMan_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_STORAGESERVICEHANDLER_H_
