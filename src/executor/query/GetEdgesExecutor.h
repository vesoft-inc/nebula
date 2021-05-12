/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_QUERY_GETEDGESEXECUTOR_H_
#define EXECUTOR_QUERY_GETEDGESEXECUTOR_H_

#include "executor/query/GetPropExecutor.h"

namespace nebula {
namespace graph {
class GetEdges;
class GetEdgesExecutor final : public GetPropExecutor {
public:
    GetEdgesExecutor(const PlanNode *node, QueryContext *qctx)
        : GetPropExecutor("GetEdgesExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    DataSet buildRequestDataSet(const GetEdges* ge);

    folly::Future<Status> getEdges();
};

}   // namespace graph
}   // namespace nebula

#endif   // EXECUTOR_QUERY_GETEDGESEXECUTOR_H_
