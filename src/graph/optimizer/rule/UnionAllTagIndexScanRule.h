/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_UNIONALLTAGINDEXSCANRULE_H_
#define GRAPH_OPTIMIZER_RULE_UNIONALLTAGINDEXSCANRULE_H_

#include <memory>  // for unique_ptr
#include <string>  // for string

#include "graph/optimizer/rule/UnionAllIndexScanBaseRule.h"  // for UnionAll...

namespace nebula {
namespace opt {
class OptRule;
class Pattern;

class OptRule;
class Pattern;

class UnionAllTagIndexScanRule final : public UnionAllIndexScanBaseRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  UnionAllTagIndexScanRule();

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_UNIONALLTAGINDEXSCANRULE_H_
