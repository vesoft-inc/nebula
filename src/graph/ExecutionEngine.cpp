/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/ExecutionEngine.h"
#include "graph/ExecutionContext.h"
#include "graph/ExecutionPlan.h"
#include "storage/client/StorageClient.h"


namespace nebula {
namespace graph {

ExecutionEngine::ExecutionEngine(meta::MetaClient* client) {
    metaClient_ = client;
}


ExecutionEngine::~ExecutionEngine() {
}


Status ExecutionEngine::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor) {
    schemaManager_ = meta::SchemaManager::create();
    schemaManager_->init(metaClient_);

    gflagsManager_ = std::make_unique<meta::ClientBasedGflagsManager>(metaClient_);

    storage_ = std::make_unique<storage::StorageClient>(ioExecutor,
                                                        metaClient_,
                                                        "graph");
    charsetInfo_ = CharsetInfo::instance();

    return Status::OK();
}

void ExecutionEngine::execute(RequestContextPtr rctx) {
    auto ectx = std::make_unique<ExecutionContext>(std::move(rctx),
                                                   schemaManager_.get(),
                                                   gflagsManager_.get(),
                                                   storage_.get(),
                                                   metaClient_,
                                                   charsetInfo_);
    // TODO(dutor) add support to plan cache
    auto plan = new ExecutionPlan(std::move(ectx));

    plan->execute();
}

}   // namespace graph
}   // namespace nebula
