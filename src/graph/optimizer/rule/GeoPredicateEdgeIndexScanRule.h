/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "graph/optimizer/rule/GeoPredicateIndexScanBaseRule.h"

namespace nebula {
namespace opt {

class GeoPredicateEdgeIndexScanRule final : public GeoPredicateIndexScanBaseRule {
 public:
  const Pattern& pattern() const override;
  std::string toString() const override;

 private:
  GeoPredicateEdgeIndexScanRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula
