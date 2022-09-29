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

   unsigned int *offset = new unsigned int[v_count + 1];
   unsigned int *neighbors = new unsigned int[e_count * 2];
   // neighborhood offset
   unsigned int *offset_neighbors = new unsigned int[v_count + 1];

   unsigned int *labels = new unsigned int[l_count];

   // load data vertices id and tags

   while (iterDV->valid()) {
     unsigned int v_id = (unsigned int)iterDV->getVertices();
     unsigned int l_id = (unsigned int)iterDV->getTags();
     labels[v_id] = (unsigned int)v.getInt(1);
     iterDV->next();
   }
   // load edges degree
   while (iterDE->valid()) {
     auto &e = iterDE->getEdge();
     unsigned int src = (unsigned int)e.src;
     offset[src + 1]++;
     iterDE->next();
   }
   for (unsigned int i = 0; i < v_count; i++) {
     offset[i + 1] += offset[i];
     offset_neighbors[i] = 0;
   }

   // load data edges
   offset[0] = 0;
   auto iterDE = ectx_->getResult(isomor->getdScanEOut()).iter();
   while (iterDE->valid()) {
     auto &e = iterDE->getEdge();
     unsigned int src = (unsigned int)e.src;
     unsigned int dst = (unsigned int)e.dst;
     neighbors[offset[src] + (offset_neighbors[src]++)] = dst;
     iterDE->next();
   }
   // load query vertices and tags
   ResultBuilder builder;
   // Set result in the ds and set the new column name for the (isomor matching 's) result.
   return finish(ResultBuilder().value(Value(std::move(ds))).build());
 }

}  // namespace graph
}  // namespace nebula
