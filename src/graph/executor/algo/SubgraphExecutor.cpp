/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/algo/SubgraphExecutor.h"

#include <stdint.h>  // for uint32_t

#include <algorithm>  // for max
#include <string>     // for string, operator<<, basi...
#include <utility>    // for move, pair
#include <vector>     // for vector

#include "common/base/Base.h"                // for kDst, kVid
#include "common/base/Logging.h"             // for COMPACT_GOOGLE_LOG_INFO
#include "common/base/Status.h"              // for Status
#include "common/datatypes/DataSet.h"        // for Row, DataSet, operator<<
#include "common/time/ScopedTimer.h"         // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"  // for ExecutionContext
#include "graph/context/Iterator.h"          // for Iterator
#include "graph/context/Result.h"            // for ResultBuilder, Result
#include "graph/planner/plan/Algo.h"         // for Subgraph
#include "graph/planner/plan/PlanNode.h"     // for PlanNode

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
  DCHECK(iter && iter->isGetNeighborsIter());
  if (currentStep == 1) {
    for (; iter->valid(); iter->next()) {
      const auto& src = iter->getColumn(nebula::kVid);
      historyVids_.emplace(src);
    }
    iter->reset();
  }
  for (; iter->valid(); iter->next()) {
    const auto& dst = iter->getEdgeProp("*", nebula::kDst);
    if (historyVids_.emplace(dst).second) {
      Row row;
      row.values.emplace_back(std::move(dst));
      ds.rows.emplace_back(std::move(row));
    }
  }

  VLOG(1) << "Next step vid is : " << ds;
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

void SubgraphExecutor::oneMoreStep() {
  auto* subgraph = asNode<Subgraph>(node());
  auto output = subgraph->oneMoreStepOutput();
  VLOG(1) << "OneMoreStep Input: " << subgraph->inputVar() << " Output: " << output;
  auto iter = ectx_->getResult(subgraph->inputVar()).iter();
  DCHECK(iter && iter->isGetNeighborsIter());

  ResultBuilder builder;
  builder.value(iter->valuePtr());
  while (iter->valid()) {
    const auto& dst = iter->getEdgeProp("*", nebula::kDst);
    if (historyVids_.find(dst) == historyVids_.end()) {
      iter->unstableErase();
    } else {
      iter->next();
    }
  }
  iter->reset();
  builder.iter(std::move(iter));
  ectx_->setResult(output, builder.build());
}

}  // namespace graph
}  // namespace nebula
