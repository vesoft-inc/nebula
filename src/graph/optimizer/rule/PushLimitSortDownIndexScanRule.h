/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include <memory>

#include "graph/optimizer/OptRule.h"
#include "graph/planner/plan/Scan.h"

namespace nebula {
namespace opt {

class PushLimitSortDownIndexScanRule : public OptRule {
 public:
  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

 protected:
  virtual int64_t limit(const MatchedResult &matched) const = 0;
  virtual StatusOr<std::vector<storage::cpp2::OrderBy>> orderBy(
      const MatchedResult &matched) const = 0;
  virtual OptGroupNode *topN(OptContext *ctx, const MatchedResult &matched) const = 0;
  virtual OptGroupNode *project(OptContext *ctx, const MatchedResult &matched) const = 0;
};

}  // namespace opt
}  // namespace nebula
