/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/plan/Algo.h"
#include "util/ToJson.h"
namespace nebula {
namespace graph {


std::unique_ptr<PlanNodeDescription> ConjunctPath::explain() const {
    auto desc = BinaryInputNode::explain();
    switch (pathKind_) {
        case PathKind::kBiBFS: {
            addDescription("kind", "BFS", desc.get());
            break;
        }
        case PathKind::kBiDijkstra: {
            addDescription("kind", "Dijkstra", desc.get());
            break;
        }
        case PathKind::kFloyd: {
            addDescription("kind", "Floyd", desc.get());
            break;
        }
        case PathKind::kAllPaths: {
            addDescription("kind", "AllPath", desc.get());
            break;
        }
    }
    addDescription("conditionalVar", util::toJson(conditionalVar_), desc.get());
    addDescription("noloop", util::toJson(noLoop_), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> ProduceAllPaths::explain() const {
    auto desc = SingleDependencyNode::explain();
    addDescription("noloop ", util::toJson(noLoop_), desc.get());
    return desc;
}

std::unique_ptr<PlanNodeDescription> CartesianProduct::explain() const {
    auto desc = SingleDependencyNode::explain();
    for (size_t i = 0; i < inputVars_.size(); ++i) {
        addDescription("var", folly::toJson(util::toJson(inputVars_[i])), desc.get());
    }
    return desc;
}

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
