/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "vgraphd/ExecutionEngine.h"
#include "vgraphd/ExecutionContext.h"
#include "vgraphd/ExecutionPlan.h"

namespace vesoft {
namespace vgraph {

ExecutionEngine::ExecutionEngine() {
    schemaManager_ = std::make_unique<SchemaManager>();
    storage_ = std::make_unique<StorageService>(schemaManager_.get());
}


ExecutionEngine::~ExecutionEngine() {
}


void ExecutionEngine::execute(RequestContextPtr rctx) {
    auto ectx = std::make_unique<ExecutionContext>(std::move(rctx),
                                                   schemaManager_.get(),
                                                   storage_.get());
    // TODO(dutor) add support to execution plan
    auto plan = new ExecutionPlan(std::move(ectx));

    plan->execute();
}

}   // namespace vgraph
}   // namespace vesoft
