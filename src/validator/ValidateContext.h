/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_VALIDATECONTEXT_H_
#define VALIDATOR_VALIDATECONTEXT_H_

#include "meta/SchemaManager.h"
#include "service/ClientSession.h"
#include "datatypes/Value.h"
#include "planner/ExecutionPlan.h"

namespace nebula {
namespace graph {
using ColDef = std::pair<std::string, Value::Type>;
using ColsDef = std::vector<ColDef>;
class ValidateContext final {
public:
    void switchToSpace(std::string spaceName, GraphSpaceID spaceId) {
        spaces_.emplace_back(std::make_pair(std::move(spaceName), spaceId));
    }

    void registerVariable(std::string var, ColsDef cols) {
        vars_.emplace(std::move(var), std::move(cols));
    }

    void setPlan(ExecutionPlan* plan) {
        plan_ = plan;
    }

    bool spaceChosen() const {
        return !spaces_.empty();
    }

    const std::pair<std::string, GraphSpaceID>& whichSpace() const {
        return spaces_.back();
    }

    meta::SchemaManager* schemaMng() const {
        return schemaMng_;
    }

    int64_t getId() {
        return ++idCounter_;
    }

    ExecutionPlan* plan() const {
        return plan_;
    }

private:
    meta::SchemaManager*                                schemaMng_;
    ClientSession*                                      session_;
    std::vector<std::pair<std::string, GraphSpaceID>>   spaces_;
    std::unordered_map<std::string, ColsDef>            vars_;
    int64_t                                             idCounter_;
    ExecutionPlan*                                      plan_;
};
}  // namespace graph
}  // namespace nebula
#endif
