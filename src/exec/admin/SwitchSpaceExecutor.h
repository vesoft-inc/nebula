/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_SWITCHSPACEEXECUTOR_H_
#define EXEC_QUERY_SWITCHSPACEEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class SwitchSpaceExecutor final : public Executor {
public:
    SwitchSpaceExecutor(const PlanNode *node, ExecutionContext *ectx)
        : Executor("SwitchSpaceExecutor", node, ectx) {}

    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_SWITCHSPACEEXECUTOR_H_
