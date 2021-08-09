/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXECUTOR_ALGO_CARTESIANPRODUCTEXECUTOR_H_
#define EXECUTOR_ALGO_CARTESIANPRODUCTEXECUTOR_H_

#include "executor/Executor.h"

namespace nebula {
namespace graph {
class CartesianProductExecutor : public Executor {
public:
    CartesianProductExecutor(const PlanNode* node, QueryContext* qctx)
        : Executor("CartesianProductExecutor", node, qctx) {}

    folly::Future<Status> execute() override;

private:
    void doCartesianProduct(const DataSet& lds, const DataSet& rds, DataSet& result);

    std::vector<std::vector<std::string>> colNames_;
};

}   // namespace graph
}   // namespace nebula
#endif  // EXECUTOR_ALGO_CARTESIANPRODUCTEXECUTOR_H_
