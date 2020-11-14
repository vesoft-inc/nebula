/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/algo/CartesianProductExecutor.h"

#include "planner/Algo.h"

namespace nebula {
namespace graph {

folly::Future<Status> CartesianProductExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto* cartesianProduct = asNode<CartesianProduct>(node());
    colNames_ = cartesianProduct->allColNames();
    auto vars = cartesianProduct->inputVars();
    if (vars.size() < 2) {
        return Status::Error("vars's size : %zu, must be greater than 2", vars.size());
    }
    std::vector<std::string> emptyCol;
    auto leftIter = std::make_unique<JoinIter>(emptyCol);
    for (size_t i = 0; i < vars.size(); ++i) {
        auto rightIter = ectx_->getResult(vars[i]).iter();
        DCHECK(!!rightIter);
        VLOG(1) << "Vars[" << i << "] : " << vars[i];
        if (!rightIter->isSequentialIter() && !rightIter->isJoinIter()) {
            std::stringstream ss;
            ss << "CartesianProductExecutor does not support" << rightIter->kind();
            return Status::Error(ss.str());
        }
        std::vector<std::string> colNames = leftIter->colNames();
        colNames.reserve(colNames.size() + colNames_[i].size());
        for (auto& name : colNames_[i]) {
            colNames.emplace_back(name);
        }
        auto joinIter = std::make_unique<JoinIter>(std::move(colNames));
        joinIter->joinIndex(leftIter.get(), rightIter.get());
        if (i == 0) {
            initJoinIter(joinIter.get(), rightIter.get());
        } else {
            doCartesianProduct(leftIter.get(), rightIter.get(), joinIter.get());
        }

        leftIter.reset(joinIter.release());
    }
    return finish(ResultBuilder().iter(std::move(leftIter)).finish());
}

void CartesianProductExecutor::initJoinIter(JoinIter* joinIter, Iterator* rightIter) {
    for (; rightIter->valid(); rightIter->next()) {
        auto size = rightIter->row()->size();
        JoinIter::JoinLogicalRow newRow(
            rightIter->row()->segments(), size, &joinIter->getColIdxIndices());
        joinIter->addRow(std::move(newRow));
    }
}

void CartesianProductExecutor::doCartesianProduct(Iterator* leftIter,
                                                  Iterator* rightIter,
                                                  JoinIter* joinIter) {
    for (; leftIter->valid(); leftIter->next()) {
        auto lSegs = leftIter->row()->segments();
        for (; rightIter->valid(); rightIter->next()) {
            std::vector<const Row*> values;
            auto rSegs = rightIter->row()->segments();
            values.insert(values.end(), lSegs.begin(), lSegs.end());
            values.insert(values.end(), rSegs.begin(), rSegs.end());
            auto size = leftIter->row()->size() + rightIter->row()->size();
            JoinIter::JoinLogicalRow newRow(std::move(values), size, &joinIter->getColIdxIndices());
            joinIter->addRow(std::move(newRow));
        }
        rightIter->reset();
    }
}

}   // namespace graph
}   // namespace nebula
