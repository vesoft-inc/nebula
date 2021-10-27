/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_RULE_PUSHTOPNDOWNTAGFULLINDEXSCAN_H_
#define OPTIMIZER_RULE_PUSHTOPNDOWNTAGFULLINDEXSCAN_H_

#include <memory>

#include "graph/optimizer/OptRule.h"
#include "graph/optimizer/rule/PushLimitSortDownIndexScanRule.h"
#include "graph/planner/plan/Scan.h"

namespace nebula {
namespace opt {

class PushLimitDownIndexRule : public PushLimitSortDownIndexScanRule {
 protected:
  int64_t limit(const MatchedResult &matched) const override;
  StatusOr<std::vector<storage::cpp2::OrderBy>> orderBy(
      const MatchedResult &matched) const override;
  OptGroupNode *topN(OptContext *ctx, const MatchedResult &matched) const override;
  OptGroupNode *project(OptContext *ctx, const MatchedResult &matched) const override;
};

class PushLimitDownTagIndexFullScanRule final : public PushLimitDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushLimitDownTagIndexFullScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

class PushLimitDownTagIndexPrefixScanRule final : public PushLimitDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushLimitDownTagIndexPrefixScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

class PushLimitDownTagIndexRangeScanRule final : public PushLimitDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushLimitDownTagIndexRangeScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

class PushLimitDownEdgeIndexFullScanRule final : public PushLimitDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushLimitDownEdgeIndexFullScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

class PushLimitDownEdgeIndexPrefixScanRule final : public PushLimitDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushLimitDownEdgeIndexPrefixScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

class PushLimitDownEdgeIndexRangeScanRule final : public PushLimitDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushLimitDownEdgeIndexRangeScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

class PushLimitDownIndexScanRule final : public PushLimitDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushLimitDownIndexScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // OPTIMIZER_RULE_PUSHTOPNDOWNTAGFULLINDEXSCAN_H_
