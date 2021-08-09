/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_SYMBOLS_H_
#define CONTEXT_SYMBOLS_H_

#include <unordered_set>
#include <vector>

#include "common/base/ObjectPool.h"
#include "common/datatypes/Value.h"
#include "common/base/StatusOr.h"

namespace nebula {
namespace graph {

class PlanNode;

struct ColDef {
    ColDef(std::string n, Value::Type t) {
        name = std::move(n);
        type = std::move(t);
    }

    bool operator==(const ColDef& rhs) const {
        return name == rhs.name && type == rhs.type;
    }

    std::string name;
    Value::Type type;
};

using ColsDef = std::vector<ColDef>;

struct Variable {
    explicit Variable(std::string n) : name(std::move(n)) {}
    std::string toString() const;

    std::string name;
    Value::Type type{Value::Type::DATASET};
    // Valid if type is dataset.
    std::vector<std::string> colNames;

    std::unordered_set<PlanNode*> readBy;
    std::unordered_set<PlanNode*> writtenBy;

    // the count of use the variable
    std::atomic<uint64_t>         userCount;
};

class SymbolTable final {
public:
    explicit SymbolTable(ObjectPool* objPool) {
        DCHECK(objPool != nullptr);
        objPool_ = objPool;
    }

    Variable* newVariable(std::string name) {
        VLOG(1) << "New variable for: " << name;
        auto* variable = objPool_->makeAndAdd<Variable>(name);
        addVar(std::move(name), variable);
        return variable;
    }

    void addVar(std::string varName, Variable* variable) {
        vars_.emplace(std::move(varName), variable);
    }

    bool readBy(const std::string& varName, PlanNode* node) {
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return false;
        }
        var->second->readBy.emplace(node);
        return true;
    }

    bool writtenBy(const std::string& varName, PlanNode* node) {
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return false;
        }
        var->second->writtenBy.emplace(node);
        return true;
    }

    bool deleteReadBy(const std::string& varName, PlanNode* node) {
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return false;
        }
        var->second->readBy.erase(node);
        return true;
    }

    bool deleteWrittenBy(const std::string& varName, PlanNode* node) {
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return false;
        }
        var->second->writtenBy.erase(node);
        return true;
    }

    bool updateReadBy(const std::string& oldVar, const std::string& newVar, PlanNode* node) {
        return deleteReadBy(oldVar, node) && readBy(newVar, node);
    }

    bool updateWrittenBy(const std::string& oldVar, const std::string& newVar, PlanNode* node) {
        return deleteWrittenBy(oldVar, node) && writtenBy(newVar, node);
    }

    Variable* getVar(const std::string& varName) {
        auto var = vars_.find(varName);
        if (var == vars_.end()) {
            return nullptr;
        } else {
            return var->second;
        }
    }

    std::string toString() const;

private:
    ObjectPool*                                                             objPool_{nullptr};
    // var name -> variable
    std::unordered_map<std::string, Variable*>                              vars_;
};

}  // namespace graph
}  // namespace nebula
#endif
