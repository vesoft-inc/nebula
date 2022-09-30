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

   auto iterDV = ectx_->getResult(isomor->getdScanVOut()).iter();
   auto iterQV = ectx_->getResult(isomor->getqScanVOut()).iter();
   auto iterDE = ectx_->getResult(isomor->getdScanEOut()).iter();
   auto iterQE = ectx_->getResult(isomor->getqScanEOut()).iter();

   unsigned int v_count = iterDV->size();
   unsigned int l_count = iterDV->size();
   unsigned int e_count = iterDE->size();

   unsigned int *offset = new unsigned int[v_count + 2];
   unsigned int *neighbors = new unsigned int[e_count * 2];
   unsigned int *labels = new unsigned int[l_count];
   // load data vertices id and tags
   while (iterDV->valid()) {
     const auto v = iterDV->getColumn(nebula::kVid);  // check if v is a vertex
     auto v_id = v.getInt();
     const auto v2 = iterDV->getColumn(1);  // get label
     auto l_id = v2.getInt();
     // unsigned int v_id = (unsigned int)v.getInt(0);
     labels[v_id] = l_id;  // Tag Id
     iterDV->next();
   }

   // load edges degree
   while (iterDE->valid()) {
     auto s = iterDE->getEdgeProp("*", kSrc);
     unsigned int src = s.getInt();
     offset[src]++;
     iterDE->next();
   }

   // load data edges
   offset[0] = 0;
   iterDE = ectx_->getResult(isomor->getdScanEOut()).iter();
   while (iterDE->valid()) {
     unsigned int src = iterDE->getEdgeProp("*", kSrc).getInt();
     unsigned int dst = iterDE->getEdgeProp("*", kDst).getInt();

     neighbors[offset[src + 1]] = dst;
     offset[src + 1]++;
     iterDE->next();
   }

   delete offset;
   delete neighbors;
   delete labels;

   ResultBuilder builder;

   // Set result in the ds and set the new column name for the (isomor matching 's) result.
   return finish(ResultBuilder().value(Value(std::move(ds))).build());
 }
}  // namespace graph
}  // namespace nebula
