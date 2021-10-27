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

class PushTopNDownIndexRule : public PushLimitSortDownIndexScanRule {
 protected:
  int64_t limit(const MatchedResult &matched) const override;
  StatusOr<std::vector<storage::cpp2::OrderBy>> orderBy(
      const MatchedResult &matched) const override;
  OptGroupNode *topN(OptContext *ctx, const MatchedResult &matched) const override;
  OptGroupNode *project(OptContext *ctx, const MatchedResult &matched) const override;
};

class PushTopNDownTagIndexFullScanRule final : public PushTopNDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushTopNDownTagIndexFullScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

class PushTopNDownTagIndexPrefixScanRule final : public PushTopNDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushTopNDownTagIndexPrefixScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

class PushTopNDownTagIndexRangeScanRule final : public PushTopNDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushTopNDownTagIndexRangeScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

class PushTopNDownEdgeIndexFullScanRule final : public PushTopNDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushTopNDownEdgeIndexFullScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

class PushTopNDownEdgeIndexPrefixScanRule final : public PushTopNDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushTopNDownEdgeIndexPrefixScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

class PushTopNDownEdgeIndexRangeScanRule final : public PushTopNDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushTopNDownEdgeIndexRangeScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

class PushTopNDownIndexScanRule final : public PushTopNDownIndexRule {
 public:
  const Pattern &pattern() const override;
  std::string toString() const override;

 private:
  PushTopNDownIndexScanRule();
  static std::unique_ptr<OptRule> kInstance;
};

}  // namespace opt
}  // namespace nebula

#endif  // OPTIMIZER_RULE_PUSHTOPNDOWNTAGFULLINDEXSCAN_H_
