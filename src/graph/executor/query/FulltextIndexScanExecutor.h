// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_FULLTEXTINDEXSCANEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_FULLTEXTINDEXSCANEXECUTOR_H_

#include "common/plugin/fulltext/elasticsearch/ESAdapter.h"
#include "graph/executor/StorageAccessExecutor.h"
#include "graph/service/GraphFlags.h"

namespace nebula::graph {
class FulltextIndexScan;
class FulltextIndexScanExecutor final : public Executor {
 public:
  FulltextIndexScanExecutor(const PlanNode* node, QueryContext* qctx)
      : Executor("FulltextIndexScanExecutor", node, qctx) {}

  folly::Future<Status> execute() override;

 private:
  StatusOr<plugin::ESQueryResult> accessFulltextIndex(TextSearchExpression* expr);

  bool isIntVidType(const SpaceInfo& space) const {
    return (*space.spaceDesc.vid_type_ref()).type == nebula::cpp2::PropertyType::INT64;
  }
  plugin::ESAdapter esAdapter_;
};

}  // namespace nebula::graph

#endif
