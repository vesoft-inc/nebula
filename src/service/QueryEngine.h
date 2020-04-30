/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef SERVICE_QUERYENGINE_H_
#define SERVICE_QUERYENGINE_H_

#include "base/Base.h"
#include "cpp/helpers.h"
#include "service/RequestContext.h"
#include "gen-cpp2/GraphService.h"
#include "meta/SchemaManager.h"
// #include "meta/ClientBasedGflagsManager.h"
#include "clients/meta/MetaClient.h"
#include "clients/storage/GraphStorageClient.h"
#include "network/NetworkUtils.h"
#include "charset/Charset.h"
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
    QueryEngine();
    ~QueryEngine();

    Status init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor);

    using RequestContextPtr = std::unique_ptr<RequestContext<cpp2::ExecutionResponse>>;
    void execute(RequestContextPtr rctx);

private:
    std::unique_ptr<meta::SchemaManager>              schemaManager_;
    // std::unique_ptr<meta::ClientBasedGflagsManager>   gflagsManager_;
    std::unique_ptr<storage::GraphStorageClient>      storage_;
    std::unique_ptr<meta::MetaClient>                 metaClient_;
    CharsetInfo*                                      charsetInfo_{nullptr};
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_EXECUTION_ENGINE_H_
