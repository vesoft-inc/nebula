/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/ngql/IsomorPlanner.h"

#include "graph/planner/plan/Query.h"
#include "graph/util/PlannerUtil.h"

namespace nebula {
namespace graph {

static TagID kDefaultTag = 1;          // what's the id of first tag?
static EdgeType kDefaultEdgeType = 1;  // what's the id of first edge type?
static char[] kDefaultProp = "default";

std::unique_ptr<IsomorPlanner::VertexProps> IsomorPlanner::buildVertexProps(
    const ExpressionProps::TagIDPropsMap& propsMap) {
  if (propsMap.empty()) {
    return std::make_unique<IsomorPlanner::VertexProps>();
  }
  auto vertexProps = std::make_unique<VertexProps>(propsMap.size());
  auto fun = [](auto& tag) {
    VertexProp vp;
    vp.tag_ref() = tag.first;
    std::vector<std::string> props(tag.second.begin(), tag.second.end());
    vp.props_ref() = std::move(props);
    return vp;
  };
  std::transform(propsMap.begin(), propsMap.end(), vertexProps->begin(), fun);
  return vertexProps;
}

StatusOr<SubPlan> IsomorPlanner::transform(AstContext* astCtx) {
  isoCtx_ = static_cast<IsomorContext*>(astCtx);
  auto qctx = isoCtx_->qctx;
  auto dSpace = isoCtx_->space;
  auto qSpaceId = isoCtx_->querySpace;

  auto dScanVertics = createScanVerticesPlan(qctx, dSpace.id, nullptr);
  auto qScanVertics = createScanVerticesPlan(qctx, qSpaceId, nullptr);

  auto dScanEdges = createScanEdgesPlan(qctx, dSpace.id, nullptr);
  auto qScanEdges = createScanEdgesPlan(qctx, qSpaceId, nullptr);

  SubPlan subPlan;
  subPlan.root = dScanVertics;
  subPlan.tail = qScanVertics;
  return subPlan;
  // TODO: Add some to do while combining the executor and the planner.
  //  (1) create a plan node for Isomorphism.cpp
  //  (2) Define the Input and output of the plan node function.
  //  (3) Add the Register.
}

PlanNode* IsomorPlanner::createScanVerticesPlan(QueryContext* qctx,
                                                GraphSpaceID spaceId,
                                                PlanNode* input) {
  // create plan node
  auto tagName = qctx->schemaMng()->toTagName(spaceId, kDefaultTag);
  auto vProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
  std::vector<std::string> colNames{kVid};

  storage::cpp2::VertexProp vProp;
  std::vector<std::string> props{kDefaultProp};
  vProp.tag_ref() = kDefaultTag;
  vProp.props_ref() = std::move(props);
  vProps->emplace_back(std::move(vProp));
  colNames.emplace_back(tagName + "." + kDefaultProp);

  auto* scanVertices = ScanVertices::make(qctx, input, spaceId, std::move(vProps));
  scanVertices->setColNames(std::move(colNames));

  return scanVertices;
}

PlanNode* IsomorPlanner::createScanEdgesPlan(QueryContext* qctx,
                                             GraphSpaceID spaceId,
                                             PlanNode* input) {
  // create plan node
  auto edgeName = qctx->schemaMng()->toEdgeName(spaceId, kDefaultEdgeType);
  auto eProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
  std::vector<std::string> colNames;

  storage::cpp2::EdgeProp eProp;
  std::vector<std::string> props = {kSrc, kType, kRank, kDst, kDefaultProp};

  // add column name
  for (auto prop : props) {
    colNames.emplace_back(edgeName + "." + prop);
  }

  eProp.type_ref() = kDefaultEdgeType;
  eProp.props_ref() = std::move(props);
  eProps->emplace_back(std::move(eProp));

  auto* scanEdges = ScanEdges::make(qctx, input, spaceId, std::move(eProps));
  scanEdges->setColNames(std::move(colNames));

  return scanEdges;
}

}  // namespace graph
}  // namespace nebula
