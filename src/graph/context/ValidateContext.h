/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_VALIDATECONTEXT_H_
#define CONTEXT_VALIDATECONTEXT_H_

#include "common/charset/Charset.h"
#include "common/datatypes/Value.h"
#include "common/meta/SchemaManager.h"
#include "context/Symbols.h"
#include "planner/plan/ExecutionPlan.h"
#include "util/AnonColGenerator.h"
#include "util/AnonVarGenerator.h"
#include "session/ClientSession.h"

namespace nebula {
namespace graph {

class ValidateContext final {
public:
    explicit ValidateContext(std::unique_ptr<AnonVarGenerator> varGen) {
        anonVarGen_ = std::move(varGen);
        anonColGen_ = std::make_unique<AnonColGenerator>();
    }

    void switchToSpace(SpaceInfo space) {
        spaces_.emplace_back(std::move(space));
    }

    const ColsDef& getVar(const std::string& var) const {
        static const ColsDef kEmptyCols;
        if (!existVar(var)) {
            return kEmptyCols;
        }
        return vars_.at(var);
    }

    bool existVar(const std::string& var) const {
        return vars_.find(var) != vars_.end();
    }

    void addSpace(const std::string &spaceName) {
        createSpaces_.emplace(spaceName);
    }

    bool hasSpace(const std::string &spaceName) const {
        return createSpaces_.find(spaceName) != createSpaces_.end();
    }

    void registerVariable(std::string var, ColsDef cols) {
        vars_.emplace(std::move(var), std::move(cols));
    }

    bool spaceChosen() const {
        return !spaces_.empty();
    }

    const SpaceInfo& whichSpace() const {
        return spaces_.back();
    }

    AnonVarGenerator* anonVarGen() const {
        return anonVarGen_.get();
    }

    AnonColGenerator* anonColGen() const {
        return anonColGen_.get();
    }

    void addSchema(const std::string& name,
                   const std::shared_ptr<const meta::NebulaSchemaProvider> &schema) {
        schemas_.emplace(name, schema);
    }

    std::shared_ptr<const meta::NebulaSchemaProvider> getSchema(const std::string &name) const {
        auto find = schemas_.find(name);
        if (find == schemas_.end()) {
            return std::shared_ptr<const meta::NebulaSchemaProvider>();
        }
        return find->second;
    }

    void addIndex(const std::string &indexName) {
        indexs_.emplace(indexName);
    }

    bool hasIndex(const std::string &indexName) {
        return indexs_.find(indexName) != indexs_.end();
    }

private:
    // spaces_ is the trace of space switch
    std::vector<SpaceInfo>                              spaces_;
    // vars_ saves all named variable
    std::unordered_map<std::string, ColsDef>            vars_;
    std::unique_ptr<AnonVarGenerator>                   anonVarGen_;
    std::unique_ptr<AnonColGenerator>                   anonColGen_;
    using Schemas = std::unordered_map<std::string,
          std::shared_ptr<const meta::NebulaSchemaProvider>>;
    Schemas                                             schemas_;
    std::unordered_set<std::string>                     createSpaces_;
    std::unordered_set<std::string>                     indexs_;
};
}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_VALIDATECONTEXT_H_
