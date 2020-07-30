/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_QUERY_INDEXSCANEXECUTOR_H_
#define EXEC_QUERY_INDEXSCANEXECUTOR_H_

#include "exec/Executor.h"

namespace nebula {
namespace graph {

class IndexScanExecutor final : public Executor {
public:
    IndexScanExecutor(const PlanNode *node, QueryContext *qctx)
        : Executor("IndexScanExecutor", node, qctx) {}

private:
    folly::Future<Status> execute() override;
};

}   // namespace graph
}   // namespace nebula

#endif   // EXEC_QUERY_INDEXSCANEXECUTOR_H_
