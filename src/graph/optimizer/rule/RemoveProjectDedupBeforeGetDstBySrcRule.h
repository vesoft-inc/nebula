/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_REMOVEPROJECTDEDUPBEFOREGETDSTBYSRCRULE_H_
#define GRAPH_OPTIMIZER_RULE_REMOVEPROJECTDEDUPBEFOREGETDSTBYSRCRULE_H_

#include <initializer_list>

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

//  DataCollect contains only one column that is a deduped vid column which GetDstBySrc needs,
//  so the following Project and Dedup are useless.
//
//  Transformation:
//  Before:
//
// +---------+---------+
// |    GetDstBySrc    |
// +---------+---------+
// |      Dedup        |
// +---------+---------+
// |      Project      |
// +---------+---------+
// |     DataCollect   |
// +---------+---------+
//
//  After:
//  // Remove Project node

class RemoveProjectDedupBeforeGetDstBySrcRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  bool match(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  RemoveProjectDedupBeforeGetDstBySrcRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_REMOVEPROJECTDEDUPBEFOREGETDSTBYSRCRULE_H_
