/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_EDGEINDEXFULLSCANRULE_H_
#define GRAPH_OPTIMIZER_RULE_EDGEINDEXFULLSCANRULE_H_

#include <memory>  // for unique_ptr
#include <string>  // for string

#include "graph/optimizer/rule/IndexFullScanBaseRule.h"  // for IndexFullSca...

namespace nebula {
namespace opt {
class OptContext;
class OptRule;
class Pattern;
}  // namespace opt

namespace graph {
class IndexScan;
class PlanNode;

class IndexScan;
class PlanNode;
}  // namespace graph

namespace opt {
class OptContext;
class OptRule;
class Pattern;

class EdgeIndexFullScanRule final : public IndexFullScanBaseRule {
 public:
  const Pattern& pattern() const override;
  std::string toString() const override;

 private:
  EdgeIndexFullScanRule();
  graph::IndexScan* scan(OptContext* ctx, const graph::PlanNode* node) const override;

  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_EDGEINDEXFULLSCANRULE_H_
