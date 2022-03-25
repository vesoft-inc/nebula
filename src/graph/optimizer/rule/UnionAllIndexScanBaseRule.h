/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_UNIONALLINDEXSCANBASERULE_H_
#define GRAPH_OPTIMIZER_RULE_UNIONALLINDEXSCANBASERULE_H_

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

class UnionAllIndexScanBaseRule : public OptRule {
 public:
  bool match(OptContext *ctx, const MatchedResult &matched) const override;
  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_UNIONALLINDEXSCANBASERULE_H_
