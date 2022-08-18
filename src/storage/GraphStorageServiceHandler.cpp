/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/GraphStorageServiceHandler.h"

#include "storage/index/LookupProcessor.h"
#include "storage/kv/GetProcessor.h"
#include "storage/kv/PutProcessor.h"
#include "storage/kv/RemoveProcessor.h"
#include "storage/mutate/AddEdgesProcessor.h"
#include "storage/mutate/AddVerticesProcessor.h"
#include "storage/mutate/DeleteEdgesProcessor.h"
#include "storage/mutate/DeleteTagsProcessor.h"
#include "storage/mutate/DeleteVerticesProcessor.h"
#include "storage/mutate/UpdateEdgeProcessor.h"
#include "storage/mutate/UpdateVertexProcessor.h"
#include "storage/query/GetDstBySrcProcessor.h"
#include "storage/query/GetNeighborsProcessor.h"
#include "storage/query/GetPropProcessor.h"
#include "storage/query/ScanEdgeProcessor.h"
#include "storage/query/ScanVertexProcessor.h"
#include "storage/transaction/ChainAddEdgesGroupProcessor.h"
#include "storage/transaction/ChainDeleteEdgesGroupProcessor.h"
#include "storage/transaction/ChainUpdateEdgeLocalProcessor.h"

#define RETURN_FUTURE(processor)   \
  auto f = processor->getFuture(); \
  processor->process(req);         \
  return f;

namespace nebula {
namespace storage {

GraphStorageServiceHandler::GraphStorageServiceHandler(StorageEnv* env) : env_(env) {
  if (FLAGS_reader_handlers_type == "io") {
    auto tf = std::make_shared<folly::NamedThreadFactory>("reader-pool");
    readerPool_ =
        std::make_shared<folly::IOThreadPoolExecutor>(FLAGS_reader_handlers, std::move(tf));
  } else {
    if (FLAGS_reader_handlers_type != "cpu") {
      LOG(WARNING) << "Unknown value for --reader_handlers_type, using `cpu'";
    }
    using TM = apache::thrift::concurrency::PriorityThreadManager;
    auto pool = TM::newPriorityThreadManager(FLAGS_reader_handlers);
    pool->setNamePrefix("reader-pool");
    pool->start();
    readerPool_ = std::move(pool);
  }

  // Initialize all counters
  kAddVerticesCounters.init("add_vertices");
  kAddEdgesCounters.init("add_edges");
  kDelVerticesCounters.init("delete_vertices");
  kDelTagsCounters.init("delete_tags");
  kDelEdgesCounters.init("delete_edges");
  kUpdateVertexCounters.init("update_vertex");
  kUpdateEdgeCounters.init("update_edge");
  kGetNeighborsCounters.init("get_neighbors");
  kGetDstBySrcCounters.init("get_dst_by_src");
  kGetPropCounters.init("get_prop");
  kLookupCounters.init("lookup");
  kScanVertexCounters.init("scan_vertex");
  kScanEdgeCounters.init("scan_edge");
  kPutCounters.init("kv_put");
  kGetCounters.init("kv_get");
  kRemoveCounters.init("kv_remove");
}

// Vertice section
folly::Future<cpp2::ExecResponse> GraphStorageServiceHandler::future_addVertices(
    const cpp2::AddVerticesRequest& req) {
  auto* processor = AddVerticesProcessor::instance(env_, &kAddVerticesCounters);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse> GraphStorageServiceHandler::future_deleteVertices(
    const cpp2::DeleteVerticesRequest& req) {
  auto* processor = DeleteVerticesProcessor::instance(env_, &kDelVerticesCounters);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse> GraphStorageServiceHandler::future_deleteTags(
    const cpp2::DeleteTagsRequest& req) {
  auto* processor = DeleteTagsProcessor::instance(env_, &kDelTagsCounters);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::UpdateResponse> GraphStorageServiceHandler::future_updateVertex(
    const cpp2::UpdateVertexRequest& req) {
  auto* processor =
      UpdateVertexProcessor::instance(env_, &kUpdateVertexCounters, readerPool_.get());
  RETURN_FUTURE(processor);
}

// Edge section
folly::Future<cpp2::ExecResponse> GraphStorageServiceHandler::future_addEdges(
    const cpp2::AddEdgesRequest& req) {
  auto* processor = AddEdgesProcessor::instance(env_, &kAddEdgesCounters);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse> GraphStorageServiceHandler::future_deleteEdges(
    const cpp2::DeleteEdgesRequest& req) {
  auto* processor = DeleteEdgesProcessor::instance(env_, &kDelEdgesCounters);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::UpdateResponse> GraphStorageServiceHandler::future_updateEdge(
    const cpp2::UpdateEdgeRequest& req) {
  auto* processor = UpdateEdgeProcessor::instance(env_, &kUpdateEdgeCounters, readerPool_.get());
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::UpdateResponse> GraphStorageServiceHandler::future_chainUpdateEdge(
    const cpp2::UpdateEdgeRequest& req) {
  auto* proc = ChainUpdateEdgeLocalProcessor::instance(env_);
  RETURN_FUTURE(proc);
}

folly::Future<cpp2::GetNeighborsResponse> GraphStorageServiceHandler::future_getNeighbors(
    const cpp2::GetNeighborsRequest& req) {
  auto* processor =
      GetNeighborsProcessor::instance(env_, &kGetNeighborsCounters, readerPool_.get());
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetDstBySrcResponse> GraphStorageServiceHandler::future_getDstBySrc(
    const cpp2::GetDstBySrcRequest& req) {
  auto* processor = GetDstBySrcProcessor::instance(env_, &kGetDstBySrcCounters, readerPool_.get());
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetPropResponse> GraphStorageServiceHandler::future_getProps(
    const cpp2::GetPropRequest& req) {
  auto* processor = GetPropProcessor::instance(env_, &kGetPropCounters, readerPool_.get());
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::LookupIndexResp> GraphStorageServiceHandler::future_lookupIndex(
    const cpp2::LookupIndexRequest& req) {
  auto* processor = LookupProcessor::instance(env_, &kLookupCounters, readerPool_.get());
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ScanResponse> GraphStorageServiceHandler::future_scanVertex(
    const cpp2::ScanVertexRequest& req) {
  auto* processor = ScanVertexProcessor::instance(env_, &kScanVertexCounters, readerPool_.get());
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ScanResponse> GraphStorageServiceHandler::future_scanEdge(
    const cpp2::ScanEdgeRequest& req) {
  auto* processor = ScanEdgeProcessor::instance(env_, &kScanEdgeCounters, readerPool_.get());
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::GetUUIDResp> GraphStorageServiceHandler::future_getUUID(
    const cpp2::GetUUIDReq&) {
  LOG(FATAL) << "Unsupported in version 2.0";
  cpp2::GetUUIDResp ret;
  return ret;
}

folly::Future<cpp2::ExecResponse> GraphStorageServiceHandler::future_chainAddEdges(
    const cpp2::AddEdgesRequest& req) {
  auto* processor = ChainAddEdgesGroupProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse> GraphStorageServiceHandler::future_chainDeleteEdges(
    const cpp2::DeleteEdgesRequest& req) {
  auto* processor = ChainDeleteEdgesGroupProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse> GraphStorageServiceHandler::future_put(
    const cpp2::KVPutRequest& req) {
  auto* processor = PutProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::KVGetResponse> GraphStorageServiceHandler::future_get(
    const cpp2::KVGetRequest& req) {
  auto* processor = GetProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse> GraphStorageServiceHandler::future_remove(
    const cpp2::KVRemoveRequest& req) {
  auto* processor = RemoveProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

}  // namespace storage
}  // namespace nebula
