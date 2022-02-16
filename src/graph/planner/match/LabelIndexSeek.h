/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_LABELINDEXSEEK_H_
#define GRAPH_PLANNER_MATCH_LABELINDEXSEEK_H_

#include <memory>  // for shared_ptr, unique_ptr
#include <vector>  // for vector

#include "common/base/StatusOr.h"                // for StatusOr
#include "common/thrift/ThriftTypes.h"           // for IndexID
#include "graph/planner/match/StartVidFinder.h"  // for StartVidFinder
#include "interface/gen-cpp2/meta_types.h"       // for IndexItem

namespace nebula {
namespace graph {
struct EdgeContext;
struct NodeContext;
struct SubPlan;

struct EdgeContext;
struct NodeContext;
struct SubPlan;

/*
 * The LabelIndexSeek was designed to find if could get the starting vids by tag
 * index.
 */
class LabelIndexSeek final : public StartVidFinder {
 public:
  static std::unique_ptr<LabelIndexSeek> make() {
    return std::unique_ptr<LabelIndexSeek>(new LabelIndexSeek());
  }

 private:
  LabelIndexSeek() = default;

  bool matchNode(NodeContext* nodeCtx) override;

  bool matchEdge(EdgeContext* edgeCtx) override;

  StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) override;

  StatusOr<SubPlan> transformEdge(EdgeContext* edgeCtx) override;

  static StatusOr<std::vector<IndexID>> pickTagIndex(const NodeContext* nodeCtx);

  static StatusOr<std::vector<IndexID>> pickEdgeIndex(const EdgeContext* edgeCtx);

  static std::shared_ptr<meta::cpp2::IndexItem> selectIndex(
      const std::shared_ptr<meta::cpp2::IndexItem> candidate,
      const std::shared_ptr<meta::cpp2::IndexItem> income) {
    // less fields is better
    // TODO(shylock) rank for field itself(e.g. INT is better than STRING)
    if (candidate->get_fields().size() > income->get_fields().size()) {
      return income;
    }
    return candidate;
  }
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_LABELINDEXSEEK_H_
