/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/algo/SubgraphExecutor.h"

#include "planner/plan/Algo.h"

namespace nebula {
namespace graph {

folly::Future<Status> SubgraphExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto* subgraph = asNode<Subgraph>(node());
    DataSet ds;
    ds.colNames = subgraph->colNames();

    uint32_t steps = subgraph->steps();
    const auto& currentStepVal = ectx_->getValue(subgraph->currentStepVar());
    DCHECK(currentStepVal.isInt());
    auto currentStep = currentStepVal.getInt();
    VLOG(1) << "Current Step is: " << currentStep << " Total Steps is: " << steps;

    if (currentStep == steps) {
        oneMoreStep();
        return finish(ResultBuilder().value(Value(std::move(ds))).finish());
    }

    VLOG(1) << "input: " << subgraph->inputVar() << " output: " << node()->outputVar();
    auto iter = ectx_->getResult(subgraph->inputVar()).iter();
    DCHECK(iter && iter->isGetNeighborsIter());
    ds.rows.reserve(iter->size());
    if (currentStep == 1) {
        for (; iter->valid(); iter->next()) {
            const auto& src = iter->getColumn(nebula::kVid);
            historyVids_.emplace(src);
        }
        iter->reset();
    }
    for (; iter->valid(); iter->next()) {
        const auto& dst = iter->getEdgeProp("*", nebula::kDst);
        if (historyVids_.emplace(dst).second) {
            Row row;
            row.values.emplace_back(std::move(dst));
            ds.rows.emplace_back(std::move(row));
        }
    }

    VLOG(1) << "Next step vid is : " << ds;
    return finish(ResultBuilder().value(Value(std::move(ds))).finish());
}

void SubgraphExecutor::oneMoreStep() {
    auto* subgraph = asNode<Subgraph>(node());
    auto output = subgraph->oneMoreStepOutput();
    VLOG(1) << "OneMoreStep Input: " << subgraph->inputVar() << " Output: " << output;
    auto iter = ectx_->getResult(subgraph->inputVar()).iter();
    DCHECK(iter && iter->isGetNeighborsIter());

    ResultBuilder builder;
    builder.value(iter->valuePtr());
    while (iter->valid()) {
        const auto& dst = iter->getEdgeProp("*", nebula::kDst);
        if (historyVids_.find(dst) == historyVids_.end()) {
            iter->unstableErase();
        } else {
            iter->next();
        }
    }
    iter->reset();
    builder.iter(std::move(iter));
    ectx_->setResult(output, builder.finish());
}

}   // namespace graph
}   // namespace nebula
