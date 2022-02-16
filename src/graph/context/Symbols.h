/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_CONTEXT_SYMBOLS_H_
#define GRAPH_CONTEXT_SYMBOLS_H_

#include <atomic>         // for atomic
#include <cstdint>        // for uint64_t
#include <string>         // for string, basic_string, operator==
#include <unordered_map>  // for unordered_map
#include <unordered_set>  // for unordered_set
#include <utility>        // for move
#include <vector>         // for vector

#include "common/base/ObjectPool.h"
#include "common/base/StatusOr.h"    // for StatusOr
#include "common/datatypes/Value.h"  // for Value, Value::Type, Value::Type:...

namespace nebula {
class ObjectPool;

class ObjectPool;

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
  std::atomic<uint64_t> userCount;
};

class SymbolTable final {
 public:
  explicit SymbolTable(ObjectPool* objPool);

  Variable* newVariable(std::string name);

  void addVar(std::string varName, Variable* variable);

  bool readBy(const std::string& varName, PlanNode* node);

  bool writtenBy(const std::string& varName, PlanNode* node);

  bool deleteReadBy(const std::string& varName, PlanNode* node);

  bool deleteWrittenBy(const std::string& varName, PlanNode* node);

  bool updateReadBy(const std::string& oldVar, const std::string& newVar, PlanNode* node);

  bool updateWrittenBy(const std::string& oldVar, const std::string& newVar, PlanNode* node);

  Variable* getVar(const std::string& varName);

  void setAliasGeneratedBy(const std::vector<std::string>& aliases, const std::string& varName);

  StatusOr<std::string> getAliasGeneratedBy(const std::string& alias);

  std::string toString() const;

 private:
  ObjectPool* objPool_{nullptr};
  // var name -> variable
  std::unordered_map<std::string, Variable*> vars_;
  // alias -> first variable that generate the alias
  std::unordered_map<std::string, std::string> aliasGeneratedBy_;
};

}  // namespace graph
}  // namespace nebula
#endif
