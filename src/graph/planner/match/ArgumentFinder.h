/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "graph/planner/match/StartVidFinder.h"

namespace nebula {
namespace graph {
class ArgumentFinder final : public StartVidFinder {
 public:
  static std::unique_ptr<ArgumentFinder> make() {
    return std::unique_ptr<ArgumentFinder>(new ArgumentFinder());
  }

  bool matchNode(NodeContext* nodeCtx) override;

  bool matchEdge(EdgeContext* edgeCtx) override;

  StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) override;

  StatusOr<SubPlan> transformEdge(EdgeContext* edgeCtx) override;

 private:
  ArgumentFinder() = default;
};

}  // namespace graph
}  // namespace nebula
