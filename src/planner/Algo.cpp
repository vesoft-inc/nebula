/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Algo.h"

namespace nebula {
namespace graph {

Status CartesianProduct::addVarAndColNames(std::string varName, std::vector<std::string> colNames) {
    auto checkName = [&varName](auto var) { return var->name == varName; };
    if (std::find_if(inputVars_.begin(), inputVars_.end(), checkName) != inputVars_.end()) {
        return Status::SemanticError("Duplicate Var: %s", varName.c_str());
    }
    auto* varPtr = qctx_->symTable()->getVar(varName);
    DCHECK(varPtr != nullptr);
    for (const auto& name : colNames) {
        if (uniqueNames_.find(name) != uniqueNames_.end()) {
            return Status::SemanticError(
                "Var : %s , exist duplicate ColName : %s", varName.c_str(), name.c_str());
        }
        uniqueNames_.emplace(name);
    }
    allColNames_.emplace_back(std::move(colNames));
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
