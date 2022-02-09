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

  if (currentStep == steps) {
    oneMoreStep();
    return finish(ResultBuilder().value(Value(std::move(ds))).build());
  }

  VLOG(1) << "input: " << subgraph->inputVar() << " output: " << node()->outputVar();
  auto iter = ectx_->getResult(subgraph->inputVar()).iter();
  std::unordered_set<Value> currentVids;
  currentVids.reserve(iter->size());
  historyVids_.reserve(historyVids_.size() + iter->size());
  if (currentStep == 1) {
    for (; iter->valid(); iter->next()) {
      const auto& src = iter->getColumn(nebula::kVid);
      currentVids.emplace(src);
    }
    iter->reset();
  }
  auto& biDirectEdgeTypes = subgraph->biDirectEdgeTypes();
  while (iter->valid()) {
    const auto& dst = iter->getEdgeProp("*", nebula::kDst);
    if (historyVids_.find(dst) != historyVids_.end()) {
      if (biDirectEdgeTypes.empty()) {
        iter->next();
      } else {
        const auto& type = iter->getEdgeProp("*", nebula::kType);
        if (type.isInt() && biDirectEdgeTypes.find(type.getInt()) != biDirectEdgeTypes.end()) {
          iter->erase();
        } else {
          iter->next();
        }
      }
    } else {
      if (currentVids.emplace(dst).second) {
        Row row;
        row.values.emplace_back(std::move(dst));
        ds.rows.emplace_back(std::move(row));
      }
      iter->next();
    }
  }
  iter->reset();
  // update historyVids
  historyVids_.insert(std::make_move_iterator(currentVids.begin()),
                      std::make_move_iterator(currentVids.end()));
  VLOG(1) << "Next step vid is : " << ds;
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

void SubgraphExecutor::oneMoreStep() {
  auto* subgraph = asNode<Subgraph>(node());
  auto output = subgraph->oneMoreStepOutput();
  VLOG(1) << "OneMoreStep Input: " << subgraph->inputVar() << " Output: " << output;
  auto iter = ectx_->getResult(subgraph->inputVar()).iter();
  DCHECK(iter && iter->isGetNeighborsIter());
  auto& biDirectEdgeTypes = subgraph->biDirectEdgeTypes();

  ResultBuilder builder;
  builder.value(iter->valuePtr());
  while (iter->valid()) {
    const auto& dst = iter->getEdgeProp("*", nebula::kDst);
    if (historyVids_.find(dst) != historyVids_.end()) {
      if (biDirectEdgeTypes.empty()) {
        iter->next();
      } else {
        const auto& type = iter->getEdgeProp("*", nebula::kType);
        if (type.isInt() && biDirectEdgeTypes.find(type.getInt()) != biDirectEdgeTypes.end()) {
          iter->erase();
        } else {
          iter->next();
        }
      }
    } else {
      iter->erase();
    }
  }
  iter->reset();
  builder.iter(std::move(iter));
  ectx_->setResult(output, builder.build());
}

}  // namespace graph
}  // namespace nebula
