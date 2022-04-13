/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/context/Symbols.h"

#include <sstream>

#include "graph/planner/plan/PlanNode.h"
#include "graph/util/Utils.h"

namespace nebula {
namespace graph {

std::string Variable::toString() const {
  std::stringstream ss;
  ss << "name: " << name << ", type: " << type << ", colNames: <" << folly::join(",", colNames)
     << ">, readBy: <" << util::join(readBy, [](auto pn) { return pn->toString(); })
     << ">, writtenBy: <" << util::join(writtenBy, [](auto pn) { return pn->toString(); }) << ">";
  return ss.str();
}

std::string SymbolTable::toString() const {
  std::stringstream ss;
  ss << "SymTable: [";
  for (const auto& p : vars_) {
    ss << "\n" << p.first << ": ";
    if (p.second) {
      ss << p.second->toString();
    }
  }
  ss << "\n]";
  return ss.str();
}

SymbolTable::SymbolTable(ObjectPool* objPool) {
  DCHECK(objPool != nullptr);
  objPool_ = objPool;
}

Variable* SymbolTable::newVariable(std::string name) {
  VLOG(1) << "New variable for: " << name;
  DCHECK(vars_.find(name) == vars_.end());
  auto* variable = objPool_->makeAndAdd<Variable>(name);
  addVar(std::move(name), variable);
  return variable;
}

void SymbolTable::addVar(std::string varName, Variable* variable) {
  vars_.emplace(std::move(varName), variable);
}

bool SymbolTable::readBy(const std::string& varName, PlanNode* node) {
  auto var = vars_.find(varName);
  if (var == vars_.end()) {
    return false;
  }
  var->second->readBy.emplace(node);
  return true;
}

bool SymbolTable::writtenBy(const std::string& varName, PlanNode* node) {
  auto var = vars_.find(varName);
  if (var == vars_.end()) {
    return false;
  }
  var->second->writtenBy.emplace(node);
  return true;
}

bool SymbolTable::deleteReadBy(const std::string& varName, PlanNode* node) {
  auto var = vars_.find(varName);
  if (var == vars_.end()) {
    return false;
  }
  var->second->readBy.erase(node);
  return true;
}

bool SymbolTable::deleteWrittenBy(const std::string& varName, PlanNode* node) {
  auto var = vars_.find(varName);
  if (var == vars_.end()) {
    return false;
  }
  for (auto& alias : var->second->colNames) {
    auto found = aliasGeneratedBy_.find(alias);
    if (found != aliasGeneratedBy_.end()) {
      if (found->second == varName) {
        aliasGeneratedBy_.erase(alias);
      }
    }
  }
  var->second->writtenBy.erase(node);
  return true;
}

bool SymbolTable::updateReadBy(const std::string& oldVar,
                               const std::string& newVar,
                               PlanNode* node) {
  return deleteReadBy(oldVar, node) && readBy(newVar, node);
}

bool SymbolTable::updateWrittenBy(const std::string& oldVar,
                                  const std::string& newVar,
                                  PlanNode* node) {
  return deleteWrittenBy(oldVar, node) && writtenBy(newVar, node);
}

Variable* SymbolTable::getVar(const std::string& varName) {
  auto var = vars_.find(varName);
  if (var == vars_.end()) {
    return nullptr;
  } else {
    return var->second;
  }
}

void SymbolTable::setAliasGeneratedBy(const std::vector<std::string>& aliases,
                                      const std::string& varName) {
  for (auto& alias : aliases) {
    if (aliasGeneratedBy_.count(alias) == 0) {
      aliasGeneratedBy_.emplace(alias, varName);
    }
  }
}

StatusOr<std::string> SymbolTable::getAliasGeneratedBy(const std::string& alias) {
  auto found = aliasGeneratedBy_.find(alias);
  if (found == aliasGeneratedBy_.end()) {
    return Status::Error("Not found a variable that generates the alias: %s", alias.c_str());
  } else {
    return found->second;
  }
}
}  // namespace graph
}  // namespace nebula
