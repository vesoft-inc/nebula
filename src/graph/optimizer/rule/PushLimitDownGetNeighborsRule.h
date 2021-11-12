/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <memory>

#include "graph/optimizer/OptRule.h"

namespace nebula {
namespace opt {

class PushLimitDownGetNeighborsRule final : public OptRule {
 public:
  const Pattern &pattern() const override;

  StatusOr<OptRule::TransformResult> transform(OptContext *ctx,
                                               const MatchedResult &matched) const override;

  std::string toString() const override;

 private:
  PushLimitDownGetNeighborsRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
