/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_OPTIMIZER_RULE_TAGANNINDEXSCANRULE_H_
#define GRAPH_OPTIMIZER_RULE_TAGANNINDEXSCANRULE_H_

#include "common/thrift/ThriftTypes.h"
#include "graph/optimizer/OptRule.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {
class ApproximateLimit;
class Sort;
}  // namespace graph
namespace opt {
static constexpr int64_t DEFAULT_SEARCH = 500;
static StatusOr<OptRule::TransformResult> runTransform(OptContext* ctx,
                                                       const MatchedResult& matched,
                                                       bool hasProject);
// static StatusOr<int32_t> findAnnIndex(OptContext* ctx,
//                                       GraphSpaceID spaceId,
//                                       const std::set<TagID>& tagIds,
//                                       const std::string& vectorProp);
/**
 * Transforms ApproximateLimit->Sort->ScanVertices pattern to AnnIndexScan
 *
 * Before:
 *   ApproximateLimit(k)
 *     |
 *   Sort(ORDER BY vector_distance_func($query, v.vector_prop))
 *     |
 *   ScanVertices(v:Tag)
 *
 * After:
 *   AnnIndexScan(query_vector, k, distance_func)
 */
class TagAnnIndexScanRuleWithProject final : public OptRule {
 public:
  const Pattern& pattern() const override;

  StatusOr<TransformResult> transform(OptContext* ctx, const MatchedResult& matched) const override;

  std::string toString() const override;

 private:
  TagAnnIndexScanRuleWithProject();

  static std::unique_ptr<OptRule> kInstance;
};

class TagAnnIndexScanRuleNoProject final : public OptRule {
 public:
  const Pattern& pattern() const override;

  StatusOr<TransformResult> transform(OptContext* ctx, const MatchedResult& matched) const override;

  std::string toString() const override;

 private:
  TagAnnIndexScanRuleNoProject();

  static std::unique_ptr<OptRule> kInstance;

  StatusOr<int32_t> findAnnIndex(OptContext* ctx,
                                 GraphSpaceID spaceId,
                                 const std::set<TagID>& tagIds,
                                 const std::string& vectorProp) const;
};

}  // namespace opt
}  // namespace nebula

#endif  // GRAPH_OPTIMIZER_RULE_TAGANNINDEXSCANRULE_H_
