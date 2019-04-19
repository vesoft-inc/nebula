/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/ExecutionEngine.h"
#include "graph/ExecutionContext.h"
#include "graph/ExecutionPlan.h"
#include "storage/client/StorageClient.h"

namespace nebula {
namespace graph {

ExecutionEngine::ExecutionEngine(std::unique_ptr<storage::StorageClient> storage) {
    // TODO(YT) schemaManager and StorageClient should share one meta client instance
    auto threadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
    metaClient_ = std::make_unique<meta::MetaClient>(threadPool);
    metaClient_->init();

    schemaManager_ = meta::SchemaManager::create();
    schemaManager_->init();
    schemaManager_->setMetaClient(metaClient_.get());
    storage_ = std::move(storage);
}


ExecutionEngine::~ExecutionEngine() {
}


void ExecutionEngine::execute(RequestContextPtr rctx) {
    auto ectx = std::make_unique<ExecutionContext>(std::move(rctx),
                                                   schemaManager_.get(),
                                                   storage_.get(),
                                                   metaClient_.get());
    // TODO(dutor) add support to plan cache
    auto plan = new ExecutionPlan(std::move(ectx));

    plan->execute();
}

}   // namespace graph
}   // namespace nebula
