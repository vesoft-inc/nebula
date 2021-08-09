/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SERVICE_QUERYENGINE_H_
#define SERVICE_QUERYENGINE_H_

#include "common/cpp/helpers.h"
#include "service/RequestContext.h"
#include "common/interface/gen-cpp2/GraphService.h"
#include "common/meta/SchemaManager.h"
#include "common/meta/IndexManager.h"
#include "common/clients/meta/MetaClient.h"
#include "common/clients/storage/GraphStorageClient.h"
#include "common/network/NetworkUtils.h"
#include "common/charset/Charset.h"
#include "optimizer/Optimizer.h"
#include <folly/executors/IOThreadPoolExecutor.h>

/**
 * QueryEngine is responsible to create and manage ExecutionPlan.
 * For the time being, we don't have the execution plan cache support,
 * instead we create a plan for each query, and destroy it upon finish.
 */

namespace nebula {
namespace graph {

class QueryEngine final : public cpp::NonCopyable, public cpp::NonMovable {
public:
    QueryEngine() = default;
    ~QueryEngine() = default;

    Status init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor,
                meta::MetaClient* metaClient);

    using RequestContextPtr = std::unique_ptr<RequestContext<ExecutionResponse>>;
    void execute(RequestContextPtr rctx);

    const meta::MetaClient* metaClient() const {
        return metaClient_;
    }

private:
    std::unique_ptr<meta::SchemaManager>              schemaManager_;
    std::unique_ptr<meta::IndexManager>               indexManager_;
    std::unique_ptr<storage::GraphStorageClient>      storage_;
    std::unique_ptr<opt::Optimizer>                   optimizer_;
    meta::MetaClient                                 *metaClient_;
    CharsetInfo*                                      charsetInfo_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_EXECUTION_ENGINE_H_
