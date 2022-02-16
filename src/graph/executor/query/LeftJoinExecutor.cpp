/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/query/LeftJoinExecutor.h"

#include <algorithm>  // for max
#include <iterator>   // for move_iterator, mak...
#include <memory>     // for unique_ptr, __shar...
#include <ostream>    // for operator<<, basic_...
#include <utility>    // for move, pair

#include "common/base/Logging.h"                   // for Check_EQImpl, COMP...
#include "common/datatypes/List.h"                 // for hash, List
#include "common/datatypes/Value.h"                // for Value, hash, Value...
#include "common/expression/Expression.h"          // for Expression
#include "common/time/ScopedTimer.h"               // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"        // for ExecutionContext
#include "graph/context/Iterator.h"                // for Iterator
#include "graph/context/QueryExpressionContext.h"  // for QueryExpressionCon...
#include "graph/context/Result.h"                  // for ResultBuilder, Result
#include "graph/executor/Executor.h"               // for Executor
#include "graph/planner/plan/PlanNode.h"           // for PlanNode
#include "graph/planner/plan/Query.h"              // for BiJoin, Join

namespace nebula {
namespace graph {
class QueryContext;

class QueryContext;

folly::Future<Status> LeftJoinExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* joinNode = asNode<Join>(node());
  auto& rhsResult =
      ectx_->getVersionedResult(joinNode->rightVar().first, joinNode->rightVar().second);
  rightColSize_ = rhsResult.valuePtr()->getDataSet().colNames.size();
  NG_RETURN_IF_ERROR(checkInputDataSets());
  return join(joinNode->hashKeys(), joinNode->probeKeys(), joinNode->colNames());
}

Status LeftJoinExecutor::close() {
  return Executor::close();
}

folly::Future<Status> LeftJoinExecutor::join(const std::vector<Expression*>& hashKeys,
                                             const std::vector<Expression*>& probeKeys,
                                             const std::vector<std::string>& colNames) {
  DCHECK_EQ(hashKeys.size(), probeKeys.size());
  DataSet result;
  if (hashKeys.size() == 1 && probeKeys.size() == 1) {
    std::unordered_map<Value, std::vector<const Row*>> hashTable;
    hashTable.reserve(rhsIter_->empty() ? 1 : rhsIter_->size());
    if (!lhsIter_->empty()) {
      buildSingleKeyHashTable(probeKeys.front(), rhsIter_.get(), hashTable);
      result = singleKeyProbe(hashKeys.front(), lhsIter_.get(), hashTable);
    }
  } else {
    std::unordered_map<List, std::vector<const Row*>> hashTable;
    hashTable.reserve(rhsIter_->empty() ? 1 : rhsIter_->size());
    if (!lhsIter_->empty()) {
      buildHashTable(probeKeys, rhsIter_.get(), hashTable);
      result = probe(hashKeys, lhsIter_.get(), hashTable);
    }
  }

  result.colNames = colNames;
  VLOG(2) << node_->toString() << ", result: " << result;
  return finish(ResultBuilder().value(Value(std::move(result))).build());
}

DataSet LeftJoinExecutor::probe(
    const std::vector<Expression*>& probeKeys,
    Iterator* probeIter,
    const std::unordered_map<List, std::vector<const Row*>>& hashTable) const {
  DataSet ds;
  ds.rows.reserve(probeIter->size());
  QueryExpressionContext ctx(ectx_);
  for (; probeIter->valid(); probeIter->next()) {
    List list;
    list.values.reserve(probeKeys.size());
    for (auto& col : probeKeys) {
      Value val = col->eval(ctx(probeIter));
      list.values.emplace_back(std::move(val));
    }

    buildNewRow<List>(hashTable, list, *probeIter->row(), ds);
  }
  return ds;
}

DataSet LeftJoinExecutor::singleKeyProbe(
    Expression* probeKey,
    Iterator* probeIter,
    const std::unordered_map<Value, std::vector<const Row*>>& hashTable) const {
  DataSet ds;
  ds.rows.reserve(probeIter->size());
  QueryExpressionContext ctx(ectx_);
  for (; probeIter->valid(); probeIter->next()) {
    auto& val = probeKey->eval(ctx(probeIter));
    buildNewRow<Value>(hashTable, val, *probeIter->row(), ds);
  }
  return ds;
}

template <class T>
void LeftJoinExecutor::buildNewRow(const std::unordered_map<T, std::vector<const Row*>>& hashTable,
                                   const T& val,
                                   const Row& lRow,
                                   DataSet& ds) const {
  auto range = hashTable.find(val);
  if (range == hashTable.end()) {
    auto lRowSize = lRow.size();
    Row newRow;
    newRow.reserve(colSize_);
    auto& values = newRow.values;
    values.insert(values.end(),
                  std::make_move_iterator(lRow.values.begin()),
                  std::make_move_iterator(lRow.values.end()));
    values.insert(values.end(), colSize_ - lRowSize, Value::kNullValue);
    ds.rows.emplace_back(std::move(newRow));
  } else {
    for (auto* row : range->second) {
      auto& rRow = *row;
      Row newRow;
      auto& values = newRow.values;
      values.reserve(lRow.size() + rRow.size());
      values.insert(values.end(),
                    std::make_move_iterator(lRow.values.begin()),
                    std::make_move_iterator(lRow.values.end()));
      values.insert(values.end(), rRow.values.begin(), rRow.values.end());
      ds.rows.emplace_back(std::move(newRow));
    }
  }
}

BiLeftJoinExecutor::BiLeftJoinExecutor(const PlanNode* node, QueryContext* qctx)
    : LeftJoinExecutor(node, qctx) {
  name_ = "BiLeftJoinExecutor";
}
folly::Future<Status> BiLeftJoinExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* joinNode = asNode<BiJoin>(node());
  auto& rhsResult = ectx_->getResult(joinNode->rightInputVar());
  rightColSize_ = rhsResult.valuePtr()->getDataSet().colNames.size();
  NG_RETURN_IF_ERROR(checkBiInputDataSets());
  return join(joinNode->hashKeys(), joinNode->probeKeys(), joinNode->colNames());
}
}  // namespace graph
}  // namespace nebula
