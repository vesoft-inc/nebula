/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/InternalStorageServiceHandler.h"

#include "storage/transaction/ChainAddEdgesRemoteProcessor.h"
#include "storage/transaction/ChainDeleteEdgesRemoteProcessor.h"
#include "storage/transaction/ChainUpdateEdgeRemoteProcessor.h"

#define RETURN_FUTURE(processor)   \
  auto f = processor->getFuture(); \
  processor->process(req);         \
  return f;

namespace nebula {
namespace storage {

InternalStorageServiceHandler::InternalStorageServiceHandler(StorageEnv* env) : env_(env) {}

folly::Future<cpp2::ExecResponse> InternalStorageServiceHandler::future_chainAddEdges(
    const cpp2::ChainAddEdgesRequest& req) {
  auto* processor = ChainAddEdgesRemoteProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::UpdateResponse> InternalStorageServiceHandler::future_chainUpdateEdge(
    const cpp2::ChainUpdateEdgeRequest& req) {
  auto* processor = ChainUpdateEdgeRemoteProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

folly::Future<cpp2::ExecResponse> InternalStorageServiceHandler::future_chainDeleteEdges(
    const cpp2::ChainDeleteEdgesRequest& req) {
  auto* processor = ChainDeleteEdgesRemoteProcessor::instance(env_);
  RETURN_FUTURE(processor);
}

}  // namespace storage
}  // namespace nebula
