/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_MERGEGETNBRSANDPROJECTRULE_H_
#define GRAPH_OPTIMIZER_RULE_MERGEGETNBRSANDPROJECTRULE_H_

#include <memory>  // for unique_ptr
#include <string>  // for string

#include "common/base/StatusOr.h"     // for StatusOr
#include "graph/optimizer/OptRule.h"  // for MatchedResult (ptr only)

namespace nebula {
namespace opt {
class OptContext;

class OptContext;

class MergeGetNbrsAndProjectRule final : public OptRule {
 public:
  const Pattern &pattern() const override;
  bool match(OptContext *ctx, const MatchedResult &matched) const override;

  StatusOr<TransformResult> transform(OptContext *ctx, const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  MergeGetNbrsAndProjectRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_MERGEGETNBRSANDPROJECTRULE_H_
