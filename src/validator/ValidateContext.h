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

struct SpaceDescription {
    std::string name;
    GraphSpaceID id;
};
class ValidateContext final {
public:
    void switchToSpace(std::string spaceName, GraphSpaceID spaceId) {
        SpaceDescription space;
        space.name = std::move(spaceName);
        space.id = spaceId;
        spaces_.emplace_back(std::move(space));
    }

    void registerVariable(std::string var, ColsDef cols) {
        vars_.emplace(std::move(var), std::move(cols));
    }

    void setPlan(ExecutionPlan* plan) {
        plan_ = plan;
    }

    void setSession(ClientSession* session) {
        session_ = session;
    }

    void setSchemaMng(meta::SchemaManager* schemaMng) {
        schemaMng_ = schemaMng;
    }

    bool spaceChosen() const {
        return !spaces_.empty();
    }

    const SpaceDescription& whichSpace() const {
        return spaces_.back();
    }

    meta::SchemaManager* schemaMng() const {
        return schemaMng_;
    }

    ExecutionPlan* plan() const {
        return plan_;
    }

private:
    meta::SchemaManager*                                schemaMng_{nullptr};
    ClientSession*                                      session_{nullptr};
    std::vector<SpaceDescription>                       spaces_;
    std::unordered_map<std::string, ColsDef>            vars_;
    ExecutionPlan*                                      plan_{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif
