/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_VALIDATECONTEXT_H_
#define CONTEXT_VALIDATECONTEXT_H_

#include "common/meta/SchemaManager.h"
#include "common/datatypes/Value.h"
#include "common/charset/Charset.h"
#include "service/ClientSession.h"
#include "planner/ExecutionPlan.h"
#include "util/AnnoVarGenerator.h"

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
    ValidateContext() {
        varGen_ = std::make_unique<AnnoVarGenerator>();
    }

    void switchToSpace(std::string spaceName, GraphSpaceID spaceId) {
        SpaceDescription space;
        space.name = std::move(spaceName);
        space.id = spaceId;
        spaces_.emplace_back(std::move(space));
    }

    void registerVariable(std::string var, ColsDef cols) {
        vars_.emplace(std::move(var), std::move(cols));
    }

    bool spaceChosen() const {
        return !spaces_.empty();
    }

    const SpaceDescription& whichSpace() const {
        return spaces_.back();
    }

    AnnoVarGenerator* varGen() const {
        return varGen_.get();
    }

private:
    // spaces_ is the trace of space switch
    std::vector<SpaceDescription>                       spaces_;
    // vars_ saves all named variable
    std::unordered_map<std::string, ColsDef>            vars_;
    std::unique_ptr<AnnoVarGenerator>                   varGen_;
};
}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_VALIDATECONTEXT_H_
