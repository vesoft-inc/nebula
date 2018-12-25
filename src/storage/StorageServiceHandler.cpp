/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/StorageServiceHandler.h"
#include "base/Base.h"
#include "storage/AddVerticesProcessor.h"
#include "storage/AddEdgesProcessor.h"
#include "storage/QueryProcessor.h"
#include "storage/QueryBoundProcessor.h"
#include "storage/QueryVertexPropsProcessor.h"
#include "storage/QueryEdgePropsProcessor.h"

#define RETURN_FUTURE(processor) \
    auto f = processor->getFuture(); \
    processor->process(req); \
    return f;

namespace nebula {
namespace storage {

folly::Future<cpp2::QueryResponse>
StorageServiceHandler::future_getOutBound(const cpp2::GetNeighborsRequest& req) {
    auto* processor = QueryBoundProcessor::instance(kvstore_, schemaMan_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::QueryResponse>
StorageServiceHandler::future_getInBound(const cpp2::GetNeighborsRequest& req) {
    auto* processor = QueryBoundProcessor::instance(kvstore_, schemaMan_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::QueryResponse>
StorageServiceHandler::future_outBoundStats(const cpp2::NeighborsStatsRequest& req) {
    auto* processor = QueryStatsProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::QueryResponse>
StorageServiceHandler::future_inBoundStats(const cpp2::NeighborsStatsRequest& req) {
    auto* processor = QueryStatsProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::QueryResponse>
StorageServiceHandler::future_getProps(const cpp2::VertexPropRequest& req) {
    auto* processor = QueryVertexPropsProcessor::instance(kvstore_, schemaMan_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::EdgePropResponse>
StorageServiceHandler::future_getEdgeProps(const cpp2::EdgePropRequest& req) {
    auto* processor = QueryEdgePropsProcessor::instance(kvstore_, schemaMan_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
StorageServiceHandler::future_addVertices(const cpp2::AddVerticesRequest& req) {
    auto* processor = AddVerticesProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
StorageServiceHandler::future_addEdges(const cpp2::AddEdgesRequest& req) {
    auto* processor = AddEdgesProcessor::instance(kvstore_);
    RETURN_FUTURE(processor);
}
}  // namespace storage
}  // namespace nebula

