/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Algo.h"

namespace nebula {
namespace graph {

Status CartesianProduct::addVar(std::string varName) {
    auto checkName = [&varName](auto var) { return var->name == varName; };
    if (std::find_if(inputVars_.begin(), inputVars_.end(), checkName) != inputVars_.end()) {
        return Status::SemanticError("Duplicate Var: %s", varName.c_str());
    }
    auto* varPtr = qctx_->symTable()->getVar(varName);
    DCHECK(varPtr != nullptr);
    allColNames_.emplace_back(varPtr->colNames);
    inputVars_.emplace_back(varPtr);
    return Status::OK();
}

std::vector<std::string> CartesianProduct::inputVars() const {
    std::vector<std::string> varNames;
    varNames.reserve(inputVars_.size());
    for (auto i : inputVars_) {
        varNames.emplace_back(i->name);
    }
    return varNames;
}

}  // namespace graph
}  // namespace nebula
