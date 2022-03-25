/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/algo/SubgraphExecutor.h"

#include "graph/planner/plan/Algo.h"

namespace nebula {
namespace graph {

folly::Future<Status> SubgraphExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* subgraph = asNode<Subgraph>(node());
  DataSet ds;
  ds.colNames = subgraph->colNames();

  uint32_t steps = subgraph->steps();
  const auto& currentStepVal = ectx_->getValue(subgraph->currentStepVar());
  DCHECK(currentStepVal.isInt());
  auto currentStep = currentStepVal.getInt();
  VLOG(1) << "Current Step is: " << currentStep << " Total Steps is: " << steps;
  auto resultVar = subgraph->resultVar();

  VLOG(1) << "input: " << subgraph->inputVar() << " output: " << node()->outputVar();
  auto iter = ectx_->getResult(subgraph->inputVar()).iter();
  auto gnSize = iter->size();

  ResultBuilder builder;
  builder.value(iter->valuePtr());

  std::unordered_map<Value, int64_t> currentVids;
  currentVids.reserve(gnSize);
  historyVids_.reserve(historyVids_.size() + gnSize);
  if (currentStep == 1) {
    for (; iter->valid(); iter->next()) {
      const auto& src = iter->getColumn(nebula::kVid);
      currentVids.emplace(src, 0);
    }
    iter->reset();
  }
  auto& biDirectEdgeTypes = subgraph->biDirectEdgeTypes();
  while (iter->valid()) {
    const auto& dst = iter->getEdgeProp("*", nebula::kDst);
    auto findIter = historyVids_.find(dst);
    if (findIter != historyVids_.end()) {
      if (biDirectEdgeTypes.empty()) {
        iter->next();
      } else {
        const auto& typeVal = iter->getEdgeProp("*", nebula::kType);
        if (UNLIKELY(!typeVal.isInt())) {
          iter->erase();
          continue;
        }
        auto type = typeVal.getInt();
        if (biDirectEdgeTypes.find(type) != biDirectEdgeTypes.end()) {
          if (type < 0 || findIter->second + 2 == currentStep) {
            iter->erase();
          } else {
            iter->next();
          }
        } else {
          iter->next();
        }
      }
    } else {
      if (currentStep == steps) {
        iter->erase();
        continue;
      }
      if (currentVids.emplace(dst, currentStep).second) {
        Row row;
        row.values.emplace_back(std::move(dst));
        ds.rows.emplace_back(std::move(row));
      }
      iter->next();
    }
  }
  iter->reset();
  builder.iter(std::move(iter));
  ectx_->setResult(resultVar, builder.build());
  // update historyVids
  historyVids_.insert(std::make_move_iterator(currentVids.begin()),
                      std::make_move_iterator(currentVids.end()));
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

}  // namespace graph
}  // namespace nebula
