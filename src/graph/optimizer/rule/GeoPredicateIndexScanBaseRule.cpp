/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/optimizer/rule/GeoPredicateIndexScanBaseRule.h"

#include "common/geo/GeoIndex.h"
#include "graph/optimizer/OptContext.h"
#include "graph/optimizer/OptGroup.h"
#include "graph/optimizer/OptRule.h"
#include "graph/optimizer/OptimizerUtils.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"
#include "graph/planner/plan/Scan.h"
#include "graph/util/ExpressionUtils.h"

using nebula::graph::Filter;
using nebula::graph::IndexScan;
using nebula::graph::OptimizerUtils;
using nebula::graph::TagIndexFullScan;
using nebula::storage::cpp2::IndexColumnHint;
using nebula::storage::cpp2::IndexQueryContext;

using Kind = nebula::graph::PlanNode::Kind;
using ExprKind = nebula::Expression::Kind;
using TransformResult = nebula::opt::OptRule::TransformResult;

namespace nebula {
namespace opt {

bool GeoPredicateIndexScanBaseRule::match(OptContext* ctx, const MatchedResult& matched) const {
  if (!OptRule::match(ctx, matched)) {
    return false;
  }
  auto filter = static_cast<const Filter*>(matched.planNode());
  auto scan = static_cast<const IndexScan*>(matched.planNode({0, 0}));
  for (auto& ictx : scan->queryContext()) {
    if (ictx.column_hints_ref().is_set()) {
      return false;
    }
  }
  auto condition = filter->condition();
  return graph::ExpressionUtils::isGeoIndexAcceleratedPredicate(condition);
}

StatusOr<TransformResult> GeoPredicateIndexScanBaseRule::transform(
    OptContext* ctx, const MatchedResult& matched) const {
  auto filter = static_cast<const Filter*>(matched.planNode());
  auto node = matched.planNode({0, 0});
  auto scan = static_cast<const IndexScan*>(node);

  auto metaClient = ctx->qctx()->getMetaClient();
  auto status = node->kind() == graph::PlanNode::Kind::kTagIndexFullScan
                    ? metaClient->getTagIndexesFromCache(scan->space())
                    : metaClient->getEdgeIndexesFromCache(scan->space());

  NG_RETURN_IF_ERROR(status);
  auto indexItems = std::move(status).value();

  OptimizerUtils::eraseInvalidIndexItems(scan->schemaId(), &indexItems);

  if (indexItems.empty()) {
    return TransformResult::noTransform();
  }

  auto condition = filter->condition();
  DCHECK(graph::ExpressionUtils::isGeoIndexAcceleratedPredicate(condition));

  auto* geoPredicate = static_cast<FunctionCallExpression*>(condition);
  DCHECK_GE(geoPredicate->args()->numArgs(), 2);
  std::string geoPredicateName = geoPredicate->name();
  folly::toLowerAscii(geoPredicateName);
  auto* first = geoPredicate->args()->args()[0];
  auto* second = geoPredicate->args()->args()[1];
  DCHECK(first->kind() == Expression::Kind::kTagProperty ||
         first->kind() == Expression::Kind::kEdgeProperty);
  DCHECK(second->kind() == Expression::Kind::kConstant);
  const auto& secondVal = static_cast<const ConstantExpression*>(second)->value();
  DCHECK(secondVal.isGeography());
  const auto& geog = secondVal.getGeography();

  auto indexItem = indexItems.back();
  const auto& fields = indexItem->get_fields();
  DCHECK_EQ(fields.size(), 1);  // geo field
  auto& geoField = fields.back();
  auto& geoColumnTypeDef = geoField.get_type();
  bool isPointColumn = geoColumnTypeDef.geo_shape_ref().has_value() &&
                       geoColumnTypeDef.geo_shape_ref().value() == meta::cpp2::GeoShape::POINT;

  geo::RegionCoverParams rc;
  const auto* indexParams = indexItem->get_index_params();
  if (indexParams) {
    if (indexParams->s2_max_level_ref().has_value()) {
      rc.maxCellLevel_ = indexParams->s2_max_level_ref().value();
    }
    if (indexParams->s2_max_cells_ref().has_value()) {
      rc.maxCellNum_ = indexParams->s2_max_cells_ref().value();
    }
  }
  geo::GeoIndex geoIndex(rc, isPointColumn);
  std::vector<geo::ScanRange> scanRanges;
  if (geoPredicateName == "st_intersects") {
    scanRanges = geoIndex.intersects(geog);
  } else if (geoPredicateName == "st_covers") {
    scanRanges = geoIndex.coveredBy(geog);
  } else if (geoPredicateName == "st_coveredby") {
    scanRanges = geoIndex.covers(geog);
  } else if (geoPredicateName == "st_dwithin") {
    DCHECK_GE(geoPredicate->args()->numArgs(), 3);
    auto* third = geoPredicate->args()->args()[2];
    DCHECK_EQ(third->kind(), Expression::Kind::kConstant);
    const auto& thirdVal = static_cast<const ConstantExpression*>(third)->value();
    DCHECK(thirdVal.isNumeric());
    double distanceInMeters = thirdVal.isFloat() ? thirdVal.getFloat() : thirdVal.getInt();
    scanRanges = geoIndex.dWithin(geog, distanceInMeters);
  }
  std::vector<IndexQueryContext> idxCtxs;
  idxCtxs.reserve(scanRanges.size());
  auto fieldName = geoField.get_name();
  for (auto& scanRange : scanRanges) {
    IndexQueryContext ictx;
    auto indexColumnHint = scanRange.toIndexColumnHint();
    indexColumnHint.column_name_ref() = fieldName;
    ictx.filter_ref() = condition->encode();
    ictx.index_id_ref() = indexItem->get_index_id();
    ictx.column_hints_ref() = {indexColumnHint};
    idxCtxs.emplace_back(std::move(ictx));
  }

  auto scanNode = IndexScan::make(ctx->qctx(), nullptr);
  OptimizerUtils::copyIndexScanData(scan, scanNode, ctx->qctx());
  scanNode->setIndexQueryContext(std::move(idxCtxs));
  // TODO(jie): geo predicate's calculation is a little heavy,
  // which is not suitable to push down to the storage
  scanNode->setOutputVar(filter->outputVar());
  scanNode->setColNames(filter->colNames());
  auto filterGroup = matched.node->group();
  auto optScanNode = OptGroupNode::create(ctx, scanNode, filterGroup);
  for (auto group : matched.dependencies[0].node->dependencies()) {
    optScanNode->dependsOn(group);
  }
  TransformResult result;
  result.newGroupNodes.emplace_back(optScanNode);
  result.eraseCurr = true;
  return result;
}

}  // namespace opt
}  // namespace nebula
