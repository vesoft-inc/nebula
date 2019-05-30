/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageServiceHandler.h"
#include "base/Base.h"
#include "storage/AddVerticesProcessor.h"
#include "storage/AddEdgesProcessor.h"
#include "storage/DeleteEdgesProcessor.h"
#include "storage/QueryBoundProcessor.h"
#include "storage/QueryVertexPropsProcessor.h"
#include "storage/QueryEdgePropsProcessor.h"
#include "storage/QueryStatsProcessor.h"
#include "storage/AdminProcessor.h"

#define RETURN_FUTURE(processor) \
    auto f = processor->getFuture(); \
    processor->process(req); \
    return f;

namespace nebula {
namespace storage {

folly::Future<cpp2::QueryResponse>
StorageServiceHandler::future_getOutBound(const cpp2::GetNeighborsRequest& req) {
    auto* processor = QueryBoundProcessor::instance(kvstore_, schemaMan_, getThreadManager());
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::QueryResponse>
StorageServiceHandler::future_getInBound(const cpp2::GetNeighborsRequest& req) {
    auto* processor = QueryBoundProcessor::instance(kvstore_,
                                                    schemaMan_,
                                                    getThreadManager(),
                                                    BoundType::IN_BOUND);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::QueryStatsResponse>
StorageServiceHandler::future_outBoundStats(const cpp2::GetNeighborsRequest& req) {
    auto* processor = QueryStatsProcessor::instance(kvstore_, schemaMan_, getThreadManager());
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::QueryStatsResponse>
StorageServiceHandler::future_inBoundStats(const cpp2::GetNeighborsRequest& req) {
    auto* processor = QueryStatsProcessor::instance(kvstore_,
                                                    schemaMan_,
                                                    getThreadManager(),
                                                    BoundType::IN_BOUND);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::QueryResponse>
StorageServiceHandler::future_getProps(const cpp2::VertexPropRequest& req) {
    auto* processor = QueryVertexPropsProcessor::instance(kvstore_,
                                                          schemaMan_,
                                                          getThreadManager());
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::EdgePropResponse>
StorageServiceHandler::future_getEdgeProps(const cpp2::EdgePropRequest& req) {
    auto* processor = QueryEdgePropsProcessor::instance(kvstore_, schemaMan_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
StorageServiceHandler::future_addVertices(const cpp2::AddVerticesRequest& req) {
    auto* processor = AddVerticesProcessor::instance(kvstore_, schemaMan_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
StorageServiceHandler::future_addEdges(const cpp2::AddEdgesRequest& req) {
    auto* processor = AddEdgesProcessor::instance(kvstore_, schemaMan_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_transLeader(const cpp2::TransLeaderReq& req) {
    auto* processor = TransLeaderProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_addPart(const cpp2::AddPartReq& req) {
    auto* processor = AddPartProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_addLearner(const cpp2::AddLearnerReq& req) {
    auto* processor = AddLearnerProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_waitingForCatchUpData(const cpp2::CatchUpDataReq& req) {
    auto* processor = WaitingForCatchUpDataProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_removePart(const cpp2::RemovePartReq& req) {
    auto* processor = RemovePartProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::AdminExecResp>
StorageServiceHandler::future_memberChange(const cpp2::MemberChangeReq& req) {
    auto* processor = MemberChangeProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
StorageServiceHandler::future_deleteEdges(const cpp2::DeleteEdgesRequest& req) {
    auto* processor = DeleteEdgesProcessor::instance(kvstore_, schemaMan_);
    RETURN_FUTURE(processor);
}

}  // namespace storage
}  // namespace nebula

