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

ExecutionEngine::ExecutionEngine() {
}


ExecutionEngine::~ExecutionEngine() {
}


Status ExecutionEngine::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor) {
    metaClient_ = std::make_unique<meta::MetaClient>();
    metaClient_->init();

    schemaManager_ = meta::SchemaManager::create();
    schemaManager_->init(metaClient_.get());
    storage_ = std::make_unique<storage::StorageClient>(ioExecutor, metaClient_.get());
    return Status::OK();
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
