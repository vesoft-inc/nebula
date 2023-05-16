/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_PLANNER_MATCH_VARIABLEPROPINDEXSEEK_H_
#define GRAPH_PLANNER_MATCH_VARIABLEPROPINDEXSEEK_H_

#include "graph/planner/match/StartVidFinder.h"

namespace nebula {
namespace graph {

// The VariablePropIndexSeek finds if a plan could get starting vids by tag
// props or edge props index.
class VariablePropIndexSeek final : public StartVidFinder {
 public:
  static std::unique_ptr<VariablePropIndexSeek> make() {
    return std::unique_ptr<VariablePropIndexSeek>(new VariablePropIndexSeek());
  }

  bool matchNode(NodeContext* nodeCtx) override;

  StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) override;

  const char* name() const override {
    return "VariablePropIndexSeekFinder";
  }

 private:
  VariablePropIndexSeek() = default;

  static bool getIndexItem(const NodeContext* nodeCtx,
                           const Expression* filter,
                           const std::string& label,
                           const std::string& alias,
                           std::string* refVarName,
                           Expression** indexFilter,
                           std::shared_ptr<meta::cpp2::IndexItem>* idxItem);
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_MATCH_PropIndexSeek_H_
