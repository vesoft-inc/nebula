// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/algo/IsomorExecutor.h"

#include "graph/planner/plan/Algo.h"

namespace nebula {
namespace graph {

folly::Future<Status> IsomorExecutor::execute() {
   // TODO: Replace the following codes with subgraph matching. Return type.
   SCOPED_TIMER(&execTime_);
   auto* isomor = asNode<Isomor>(node());
   DataSet ds;
   ds.colNames = isomor->colNames();

   // auto iterDV = ectx_->getResult(isomor->getdScanVOut()).iter();
   // auto iterQV = ectx_->getResult(isomor->getqScanVOut()).iter();
   ResultBuilder builder;
   // Set result in the ds and set the new column name for the (isomor matching 's) result.
   return finish(ResultBuilder().value(Value(std::move(ds))).build());
 }

}  // namespace graph
}  // namespace nebula
