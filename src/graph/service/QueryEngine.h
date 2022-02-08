/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SERVICE_QUERYENGINE_H_
#define GRAPH_SERVICE_QUERYENGINE_H_

#include <folly/executors/IOThreadPoolExecutor.h>

#include <boost/core/noncopyable.hpp>

#include "clients/meta/MetaClient.h"
#include "clients/storage/StorageClient.h"
#include "common/charset/Charset.h"
#include "common/cpp/helpers.h"
#include "common/meta/IndexManager.h"
#include "common/meta/SchemaManager.h"
#include "common/network/NetworkUtils.h"
#include "graph/optimizer/Optimizer.h"
#include "graph/service/RequestContext.h"
#include "interface/gen-cpp2/GraphService.h"

namespace nebula {
namespace graph {

/**
 * QueryEngine is responsible to create and manage ExecutionPlan.
 * For the time being, we don't have the execution plan cache support,
 * instead we create a plan for each query, and destroy it upon finish.
 */
class QueryEngine final : public boost::noncopyable, public cpp::NonMovable {
 public:
  QueryEngine() = default;
  ~QueryEngine() = default;

  Status init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor,
              meta::MetaClient* metaClient);

  using RequestContextPtr = std::unique_ptr<RequestContext<ExecutionResponse>>;
  void execute(RequestContextPtr rctx);

  meta::MetaClient* metaClient() {
    return metaClient_;
  }

 private:
  Status setupMemoryMonitorThread();

  std::unique_ptr<meta::SchemaManager> schemaManager_;
  std::unique_ptr<meta::IndexManager> indexManager_;
  std::unique_ptr<storage::StorageClient> storage_;
  std::unique_ptr<opt::Optimizer> optimizer_;
  std::unique_ptr<thread::GenericWorker> memoryMonitorThread_;
  meta::MetaClient* metaClient_{nullptr};
  CharsetInfo* charsetInfo_{nullptr};
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTION_ENGINE_H_
