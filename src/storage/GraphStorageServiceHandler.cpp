/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/GraphStorageServiceHandler.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/AddEdgesAtomicProcessor.h"
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/mutate/UpdateVertexProcessor.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "storage/query/GetNeighborsProcessor.h"
#include "storage/query/GetPropProcessor.h"
#include "storage/query/ScanVertexProcessor.h"
#include "storage/query/ScanEdgeProcessor.h"
#include "storage/query/GetUUIDProcessor.h"
#include "storage/index/LookupProcessor.h"
#include "storage/transaction/TransactionProcessor.h"

#define RETURN_FUTURE(processor) \
    auto f = processor->getFuture(); \
    processor->process(req); \
    return f;

namespace nebula {
namespace storage {

// Vertice section
folly::Future<cpp2::ExecResponse>
GraphStorageServiceHandler::future_addVertices(const cpp2::AddVerticesRequest& req) {
    auto* processor = AddVerticesProcessor::instance(env_, &addVerticesQpsStat_, &vertexCache_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
GraphStorageServiceHandler::future_deleteVertices(const cpp2::DeleteVerticesRequest& req) {
    auto* processor = DeleteVerticesProcessor::instance(env_, &delVerticesQpsStat_, &vertexCache_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::UpdateResponse>
GraphStorageServiceHandler::future_updateVertex(const cpp2::UpdateVertexRequest& req) {
    auto* processor = UpdateVertexProcessor::instance(env_, &updateVertexQpsStat_, &vertexCache_);
    RETURN_FUTURE(processor);
}

// Edge section
folly::Future<cpp2::ExecResponse>
GraphStorageServiceHandler::future_addEdges(const cpp2::AddEdgesRequest& req) {
    auto* processor = AddEdgesProcessor::instance(env_, &addEdgesQpsStat_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
GraphStorageServiceHandler::future_deleteEdges(const cpp2::DeleteEdgesRequest& req) {
    auto* processor = DeleteEdgesProcessor::instance(env_, &delEdgesQpsStat_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::UpdateResponse>
GraphStorageServiceHandler::future_updateEdge(const cpp2::UpdateEdgeRequest& req) {
    auto* processor = UpdateEdgeProcessor::instance(env_, &updateEdgeQpsStat_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetNeighborsResponse>
GraphStorageServiceHandler::future_getNeighbors(const cpp2::GetNeighborsRequest& req) {
    auto* processor = GetNeighborsProcessor::instance(env_,
                                                      &getNeighborsQpsStat_,
                                                      readerPool_.get(),
                                                      &vertexCache_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetPropResponse>
GraphStorageServiceHandler::future_getProps(const cpp2::GetPropRequest& req) {
    auto* processor = GetPropProcessor::instance(env_,
                                                 &getPropQpsStat_,
                                                 readerPool_.get(),
                                                 &vertexCache_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::LookupIndexResp>
GraphStorageServiceHandler::future_lookupIndex(const cpp2::LookupIndexRequest& req) {
    auto* processor = LookupProcessor::instance(env_,
                                                &lookupQpsStat_,
                                                readerPool_.get(),
                                                &vertexCache_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ScanVertexResponse>
GraphStorageServiceHandler::future_scanVertex(const cpp2::ScanVertexRequest& req) {
    auto* processor = ScanVertexProcessor::instance(env_,
                                                    &scanVertexQpsStat_,
                                                    readerPool_.get());
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ScanEdgeResponse>
GraphStorageServiceHandler::future_scanEdge(const cpp2::ScanEdgeRequest& req) {
    auto* processor = ScanEdgeProcessor::instance(env_,
                                                  &scanEdgeQpsStat_,
                                                  readerPool_.get());
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetUUIDResp>
GraphStorageServiceHandler::future_getUUID(const cpp2::GetUUIDReq& req) {
    auto* processor = GetUUIDProcessor::instance(env_);
    RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse>
GraphStorageServiceHandler::future_addEdgesAtomic(const cpp2::AddEdgesRequest& req) {
    auto* processor = AddEdgesAtomicProcessor::instance(env_, &addEdgesQpsStat_);
    RETURN_FUTURE(processor);
}


}  // namespace storage
}  // namespace nebula
