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
  folly::RWSpinLock::ReadHolder holder(lock_);
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

bool SymbolTable::existsVar(const std::string& varName) const {
  folly::RWSpinLock::ReadHolder holder(lock_);
  return vars_.find(varName) != vars_.end();
}

Variable* SymbolTable::newVariable(const std::string& name) {
#ifdef NDEBUG
  {
    // addVar has a write lock, should warp this read lock block
    folly::RWSpinLock::ReadHolder holder(lock_);
    VLOG(1) << "New variable for: " << name;
    DCHECK(vars_.find(name) == vars_.end());
  }
#endif
  auto* variable = objPool_->makeAndAdd<Variable>(name);
  addVar(name, variable);
  // Initialize all variable in variable map (output of node, inner variable etc.)
  // Some variable will be useless after optimizer, maybe we could remove it.
  ectx_->initVar(name);
  return variable;
}

void SymbolTable::addVar(std::string varName, Variable* variable) {
  folly::RWSpinLock::WriteHolder holder(lock_);
  vars_.emplace(std::move(varName), variable);
}

bool SymbolTable::readBy(const std::string& varName, PlanNode* node) {
  folly::RWSpinLock::WriteHolder holder(lock_);
  auto var = vars_.find(varName);
  if (var == vars_.end()) {
    return false;
  }
  var->second->readBy.emplace(node);
  return true;
}

bool SymbolTable::writtenBy(const std::string& varName, PlanNode* node) {
  folly::RWSpinLock::WriteHolder holder(lock_);
  auto var = vars_.find(varName);
  if (var == vars_.end()) {
    return false;
  }
  var->second->writtenBy.emplace(node);
  return true;
}

bool SymbolTable::deleteReadBy(const std::string& varName, PlanNode* node) {
  folly::RWSpinLock::WriteHolder holder(lock_);
  auto var = vars_.find(varName);
  if (var == vars_.end()) {
    return false;
  }
  var->second->readBy.erase(node);
  return true;
}

bool SymbolTable::deleteWrittenBy(const std::string& varName, PlanNode* node) {
  folly::RWSpinLock::WriteHolder holder(lock_);
  auto var = vars_.find(varName);
  if (var == vars_.end()) {
    return false;
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
  folly::RWSpinLock::ReadHolder holder(lock_);
  DCHECK(!varName.empty()) << "the variable name is empty";
  auto var = vars_.find(varName);
  if (var == vars_.end()) {
    return nullptr;
  } else {
    return var->second;
  }
}

}  // namespace graph
}  // namespace nebula
