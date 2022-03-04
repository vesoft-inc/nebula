/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNSCANAPPENDVERTICESRULE_H
#define GRAPH_OPTIMIZER_RULE_PUSHLIMITDOWNSCANAPPENDVERTICESRULE_H

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {
/*
  Push down the limit to storage layer
  Required conditions:
   1. Match the pattern
  Benefits:
   1. Limit data early to optimize performance

  Tranformation:
  Before:

  +--------+--------+
  |      Limit      |
  |    (limit=3)    |
  +--------+--------+
           |
 +---------+---------+
 |    IndexScan      |
 +---------+---------+

  After:

  +--------+--------+
  |      Limit      |
  |    (limit=3)    |
  +--------+--------+
           |
 +---------+---------+
 |     IndexScan     |
 |     (limit=3)     |
 +---------+---------+

*/
class PushLimitDownScanAppendVerticesRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;
  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushLimitDownScanAppendVerticesRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
#endif
