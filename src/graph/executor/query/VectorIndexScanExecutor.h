// Copyright (c) 2024 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_VECTORINDEXSCANEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_VECTORINDEXSCANEXECUTOR_H_

#include "common/plugin/fulltext/elasticsearch/ESAdapter.h"
#include "graph/executor/StorageAccessExecutor.h"
#include "graph/service/GraphFlags.h"

namespace nebula::graph {
class VectorIndexScan;
class VectorIndexScanExecutor final : public Executor {
 public:
  VectorIndexScanExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("VectorIndexScanExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  StatusOr<plugin::ESQueryResult> accessVectorIndex(VectorQueryExpression* expr);

  bool isIntVidType(const SpaceInfo& space) const {
    return (*space.spaceDesc.vid_type_ref()).type == nebula::cpp2::PropertyType::INT64;
  }
  plugin::ESAdapter esAdapter_;
};

}  // namespace nebula::graph

#endif  // GRAPH_EXECUTOR_QUERY_VECTORINDEXSCANEXECUTOR_H_ 