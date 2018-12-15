/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_EXECUTION_ENGINE_H_
#define VGRAPHD_EXECUTION_ENGINE_H_

#include "base/Base.h"
#include "cpp/helpers.h"
#include "vgraphd/RequestContext.h"
#include "interface/gen-cpp2/GraphDbService.h"
#include "vgraphd/mock/SchemaManager.h"
#include "vgraphd/mock/StorageService.h"

/**
 * ExecutinoEngine is responsible to create and manage ExecutionPlan.
 * For the time being, we don't have the execution plan cache support,
 * instead we create a plan for each query, and destroy it upon finish.
 */

namespace vesoft {
namespace vgraph {

class ExecutionEngine final : public cpp::NonCopyable, public cpp::NonMovable {
public:
    ExecutionEngine();
    ~ExecutionEngine();

    using RequestContextPtr = std::unique_ptr<RequestContext<cpp2::ExecutionResponse>>;
    void execute(RequestContextPtr rctx);

private:
    std::unique_ptr<SchemaManager>              schemaManager_;
    std::unique_ptr<StorageService>             storage_;
};

}   // namespace vgraph
}   // namespace vesoft

#endif  // VGRAPHD_EXECUTION_ENGINE_H_
