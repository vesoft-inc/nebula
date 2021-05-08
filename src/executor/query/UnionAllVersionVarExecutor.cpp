/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/UnionAllVersionVarExecutor.h"
#include "planner/plan/Query.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> UnionAllVersionVarExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* UnionAllVersionVarNode = asNode<UnionAllVersionVar>(node());
    // Retrive all versions of inputVar
    auto& results = ectx_->getHistory(UnionAllVersionVarNode->inputVar());
    DCHECK_GT(results.size(), 0);
    // List of iterators to be unioned
    std::vector<std::unique_ptr<Iterator>> inputList;
    // Iterate the results and union their datasets
    for (size_t i = 0; i < results.size(); i++) {
        auto iter = results[i].iter();
        DCHECK(!!iter);

        auto inputData = iter->valuePtr();
        // Check if inputData is null
        if (UNLIKELY(!inputData)) {
            return Status::Error(
                "UnionAllVersionVar related executor failed, input dataset is null");
        }
        // Check if inputData is a dataset
        if (UNLIKELY(!inputData->isDataSet())) {
            std::stringstream ss;
            ss << "Invalid data types of dependencies: " << inputData->type() << ".";
            return Status::Error(ss.str());
        }
        inputList.emplace_back(std::move(iter));
    }

    auto value = results[0].iter()->valuePtr();
    auto iter = std::make_unique<SequentialIter>(std::move(inputList));
    return finish(ResultBuilder().value(value).iter(std::move(iter)).finish());
}

}   // namespace graph
}   // namespace nebula
