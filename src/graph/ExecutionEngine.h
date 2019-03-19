/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_EXECUTION_ENGINE_H_
#define GRAPH_EXECUTION_ENGINE_H_

#include "base/Base.h"
#include "cpp/helpers.h"
#include "graph/RequestContext.h"
#include "gen-cpp2/GraphService.h"
#include "graph/mock/SchemaManager.h"

/**
 * ExecutinoEngine is responsible to create and manage ExecutionPlan.
 * For the time being, we don't have the execution plan cache support,
 * instead we create a plan for each query, and destroy it upon finish.
 */

namespace folly {
class IOThreadPoolExecutor;
}   // namespace folly

namespace nebula {
namespace storage {
class StorageClient;
}   // namespace storage

namespace graph {

class ExecutionEngine final : public cpp::NonCopyable, public cpp::NonMovable {
public:
    explicit ExecutionEngine(std::unique_ptr<storage::StorageClient> storage);
    ~ExecutionEngine();

    using RequestContextPtr = std::unique_ptr<RequestContext<cpp2::ExecutionResponse>>;
    void execute(RequestContextPtr rctx);

private:
    std::unique_ptr<SchemaManager>              schemaManager_;
    std::unique_ptr<storage::StorageClient>     storage_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_EXECUTION_ENGINE_H_
