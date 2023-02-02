/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_CONTEXT_SYMBOLS_H_
#define GRAPH_CONTEXT_SYMBOLS_H_

#include <mutex>
#include <unordered_set>
#include <vector>

#include "common/base/ObjectPool.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/Value.h"
#include "graph/context/ExecutionContext.h"

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
  std::atomic<uint64_t> userCount{0};
};

class SymbolTable final {
 public:
  explicit SymbolTable(ObjectPool* objPool, ExecutionContext* ectx)
      : objPool_(DCHECK_NOTNULL(objPool)), ectx_(DCHECK_NOTNULL(ectx)) {}

  bool existsVar(const std::string& varName) const;

  Variable* newVariable(const std::string& name);

  bool readBy(const std::string& varName, PlanNode* node);

  bool writtenBy(const std::string& varName, PlanNode* node);

  bool deleteReadBy(const std::string& varName, PlanNode* node);

  bool deleteWrittenBy(const std::string& varName, PlanNode* node);

  bool updateReadBy(const std::string& oldVar, const std::string& newVar, PlanNode* node);

  bool updateWrittenBy(const std::string& oldVar, const std::string& newVar, PlanNode* node);

  Variable* getVar(const std::string& varName);

  std::string toString() const;

 private:
  void addVar(std::string varName, Variable* variable);

  ObjectPool* objPool_{nullptr};
  ExecutionContext* ectx_{nullptr};
  // var name -> variable
  mutable folly::RWSpinLock lock_;
  std::unordered_map<std::string, Variable*> vars_;
};

}  // namespace graph
}  // namespace nebula
#endif
