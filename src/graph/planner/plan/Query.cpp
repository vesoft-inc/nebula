/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/planner/plan/Query.h"

#include <folly/String.h>
#include <folly/dynamic.h>
#include <folly/json.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "graph/planner/plan/PlanNodeVisitor.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/util/ToJson.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

int64_t Explore::limit(QueryContext* qctx) const {
  DCHECK(ExpressionUtils::isEvaluableExpr(limit_, qctx));
  QueryExpressionContext ctx(qctx ? qctx->ectx() : nullptr);
  return DCHECK_NOTNULL(limit_)->eval(ctx).getInt();
}

std::unique_ptr<PlanNodeDescription> Explore::explain() const {
  auto desc = SingleInputNode::explain();
  addDescription("space", folly::to<std::string>(space_), desc.get());
  addDescription("dedup", folly::to<std::string>(dedup_), desc.get());
  addDescription("limit", limit_ ? limit_->toString() : "", desc.get());
  addDescription("filter", filter_ ? filter_->toString() : "", desc.get());
  addDescription("orderBy", folly::toJson(util::toJson(orderBy_)), desc.get());
  return desc;
}

void Explore::cloneMembers(const Explore& e) {
  SingleInputNode::cloneMembers(e);

  space_ = e.space_;
  dedup_ = e.dedup_;
  limit_ = e.limit_;
  filter_ = e.filter_;
  orderBy_ = e.orderBy_;
}

std::unique_ptr<PlanNodeDescription> GetNeighbors::explain() const {
  auto desc = Explore::explain();
  addDescription("src", src_ ? src_->toString() : "", desc.get());
  addDescription("edgeTypes", folly::toJson(util::toJson(edgeTypes_)), desc.get());
  addDescription("edgeDirection", apache::thrift::util::enumNameSafe(edgeDirection_), desc.get());
  addDescription(
      "vertexProps", vertexProps_ ? folly::toJson(util::toJson(*vertexProps_)) : "", desc.get());
  addDescription(
      "edgeProps", edgeProps_ ? folly::toJson(util::toJson(*edgeProps_)) : "", desc.get());
  addDescription(
      "statProps", statProps_ ? folly::toJson(util::toJson(*statProps_)) : "", desc.get());
  addDescription("exprs", exprs_ ? folly::toJson(util::toJson(*exprs_)) : "", desc.get());
  addDescription("random", folly::toJson(util::toJson(random_)), desc.get());
  return desc;
}

PlanNode* GetNeighbors::clone() const {
  auto* newGN = GetNeighbors::make(qctx_, nullptr, space_);
  newGN->cloneMembers(*this);
  return newGN;
}

void GetNeighbors::cloneMembers(const GetNeighbors& g) {
  Explore::cloneMembers(g);

  setSrc(g.src_->clone());
  setEdgeTypes(g.edgeTypes_);
  setEdgeDirection(g.edgeDirection_);
  setRandom(g.random_);
  if (g.vertexProps_) {
    auto vertexProps = *g.vertexProps_;
    auto vertexPropsPtr = std::make_unique<decltype(vertexProps)>(vertexProps);
    setVertexProps(std::move(vertexPropsPtr));
  }

  if (g.edgeProps_) {
    auto edgeProps = *g.edgeProps_;
    auto edgePropsPtr = std::make_unique<decltype(edgeProps)>(std::move(edgeProps));
    setEdgeProps(std::move(edgePropsPtr));
  }

  if (g.statProps_) {
    auto statProps = *g.statProps_;
    auto statPropsPtr = std::make_unique<decltype(statProps)>(std::move(statProps));
    setStatProps(std::move(statPropsPtr));
  }

  if (g.exprs_) {
    auto exprs = *g.exprs_;
    auto exprsPtr = std::make_unique<decltype(exprs)>(exprs);
    setExprs(std::move(exprsPtr));
  }
}

std::unique_ptr<PlanNodeDescription> Expand::explain() const {
  auto desc = Explore::explain();
  addDescription("sample", folly::toJson(util::toJson(sample_)), desc.get());
  addDescription("joinInput", folly::toJson(util::toJson(joinInput_)), desc.get());
  addDescription("maxSteps", folly::to<std::string>(maxSteps_), desc.get());
  addDescription(
      "edgeProps", edgeProps_ ? folly::toJson(util::toJson(*edgeProps_)) : "", desc.get());
  auto limits = folly::dynamic::array();
  for (auto i : stepLimits_) {
    limits.push_back(folly::to<std::string>(i));
  }
  addDescription("stepLimits", folly::toJson(limits), desc.get());
  return desc;
}

PlanNode* Expand::clone() const {
  auto* expand = Expand::make(qctx_, nullptr, space_);
  expand->cloneMembers(*this);
  return expand;
}

void Expand::cloneMembers(const Expand& expand) {
  Explore::cloneMembers(expand);
  sample_ = expand.sample();
  maxSteps_ = expand.maxSteps();
  if (expand.edgeProps()) {
    auto edgeProps = *expand.edgeProps_;
    auto edgePropsPtr = std::make_unique<decltype(edgeProps)>(std::move(edgeProps));
    setEdgeProps(std::move(edgePropsPtr));
  }
  stepLimits_ = expand.stepLimits();
  joinInput_ = expand.joinInput();
  edgeTypes_ = expand.edgeTypes();
}

std::unique_ptr<PlanNodeDescription> ExpandAll::explain() const {
  auto desc = Expand::explain();
  addDescription("minSteps", folly::to<std::string>(minSteps_), desc.get());
  addDescription(
      "vertexProps", vertexProps_ ? folly::toJson(util::toJson(*vertexProps_)) : "", desc.get());
  auto vertexColumns = folly::dynamic::array();
  if (vertexColumns_) {
    for (const auto* col : vertexColumns_->columns()) {
      DCHECK(col != nullptr);
      vertexColumns.push_back(col->toString());
    }
    addDescription("vertexColumns", folly::toJson(vertexColumns), desc.get());
  }
  auto edgeColumns = folly::dynamic::array();
  if (edgeColumns_) {
    for (const auto* col : edgeColumns_->columns()) {
      DCHECK(col != nullptr);
      edgeColumns.push_back(col->toString());
    }
    addDescription("edgeColumns", folly::toJson(edgeColumns), desc.get());
  }
  return desc;
}

PlanNode* ExpandAll::clone() const {
  auto* expandAll = ExpandAll::make(qctx_, nullptr, space_);
  expandAll->cloneMembers(*this);
  return expandAll;
}

void ExpandAll::cloneMembers(const ExpandAll& expandAll) {
  Expand::cloneMembers(expandAll);
  minSteps_ = expandAll.minSteps();
  if (expandAll.vertexProps()) {
    auto vertexProps = *expandAll.vertexProps_;
    auto vertexPropsPtr = std::make_unique<decltype(vertexProps)>(vertexProps);
    setVertexProps(std::move(vertexPropsPtr));
  }
  if (expandAll.vertexColumns()) {
    vertexColumns_ = qctx_->objPool()->makeAndAdd<YieldColumns>();
    for (const auto& col : expandAll.vertexColumns()->columns()) {
      vertexColumns_->addColumn(col->clone().release());
    }
  }
  if (expandAll.edgeColumns()) {
    edgeColumns_ = qctx_->objPool()->makeAndAdd<YieldColumns>();
    for (const auto& col : expandAll.edgeColumns()->columns()) {
      edgeColumns_->addColumn(col->clone().release());
    }
  }
}

std::unique_ptr<PlanNodeDescription> GetVertices::explain() const {
  auto desc = Explore::explain();
  addDescription("src", src_ ? src_->toString() : "", desc.get());
  addDescription("props", props_ ? folly::toJson(util::toJson(*props_)) : "", desc.get());
  addDescription("exprs", exprs_ ? folly::toJson(util::toJson(*exprs_)) : "", desc.get());
  return desc;
}

PlanNode* GetVertices::clone() const {
  auto* newGV = GetVertices::make(qctx_, nullptr, space_);
  newGV->cloneMembers(*this);
  return newGV;
}

void GetVertices::cloneMembers(const GetVertices& gv) {
  Explore::cloneMembers(gv);

  src_ = gv.src()->clone();

  if (gv.props_) {
    auto vertexProps = *gv.props_;
    auto vertexPropsPtr = std::make_unique<decltype(vertexProps)>(std::move(vertexProps));
    setVertexProps(std::move(vertexPropsPtr));
  }

  if (gv.exprs_) {
    auto exprs = *gv.exprs_;
    auto exprsPtr = std::make_unique<decltype(exprs)>(std::move(exprs));
    setExprs(std::move(exprsPtr));
  }
}

std::unique_ptr<PlanNodeDescription> GetEdges::explain() const {
  auto desc = Explore::explain();
  addDescription("src", src_ ? src_->toString() : "", desc.get());
  addDescription("type", folly::toJson(util::toJson(type_)), desc.get());
  addDescription("ranking", ranking_ ? ranking_->toString() : "", desc.get());
  addDescription("dst", dst_ ? dst_->toString() : "", desc.get());
  addDescription("props", props_ ? folly::toJson(util::toJson(*props_)) : "", desc.get());
  addDescription("exprs", exprs_ ? folly::toJson(util::toJson(*exprs_)) : "", desc.get());
  return desc;
}

PlanNode* GetEdges::clone() const {
  auto* newGE = GetEdges::make(qctx_, nullptr, space_);
  newGE->cloneMembers(*this);
  return newGE;
}

void GetEdges::cloneMembers(const GetEdges& ge) {
  Explore::cloneMembers(ge);

  src_ = ge.src()->clone();
  type_ = ge.type()->clone();
  ranking_ = ge.ranking()->clone();
  dst_ = ge.dst()->clone();

  if (ge.props_) {
    auto edgeProps = *ge.props_;
    auto edgePropsPtr = std::make_unique<decltype(edgeProps)>(std::move(edgeProps));
    setEdgeProps(std::move(edgePropsPtr));
  }

  if (ge.exprs_) {
    auto exprs = *ge.exprs_;
    auto exprsPtr = std::make_unique<decltype(exprs)>(std::move(exprs));
    setExprs(std::move(exprsPtr));
  }
}

std::unique_ptr<PlanNodeDescription> IndexScan::explain() const {
  auto desc = Explore::explain();
  addDescription("schemaId", folly::toJson(util::toJson(schemaId_)), desc.get());
  addDescription("isEdge", folly::toJson(util::toJson(isEdge_)), desc.get());
  addDescription("returnCols", folly::toJson(util::toJson(returnCols_)), desc.get());
  addDescription("indexCtx", folly::toJson(util::toJson(contexts_)), desc.get());
  return desc;
}

PlanNode* IndexScan::clone() const {
  auto* newIndexScan = IndexScan::make(qctx_, nullptr);
  newIndexScan->cloneMembers(*this);
  return newIndexScan;
}

void IndexScan::cloneMembers(const IndexScan& g) {
  Explore::cloneMembers(g);

  contexts_ = g.contexts_;
  returnCols_ = g.returnCols_;
  isEdge_ = g.isEdge();
  schemaId_ = g.schemaId();
  yieldColumns_ = g.yieldColumns();
}

std::unique_ptr<PlanNodeDescription> ScanVertices::explain() const {
  auto desc = Explore::explain();
  addDescription("props", props_ ? folly::toJson(util::toJson(*props_)) : "", desc.get());
  addDescription("exprs", exprs_ ? folly::toJson(util::toJson(*exprs_)) : "", desc.get());
  return desc;
}

PlanNode* ScanVertices::clone() const {
  auto* newGV = ScanVertices::make(qctx_, nullptr, space_);
  newGV->cloneMembers(*this);
  return newGV;
}

void ScanVertices::cloneMembers(const ScanVertices& gv) {
  Explore::cloneMembers(gv);

  if (gv.props_) {
    auto vertexProps = *gv.props_;
    auto vertexPropsPtr = std::make_unique<decltype(vertexProps)>(std::move(vertexProps));
    setVertexProps(std::move(vertexPropsPtr));
  }

  if (gv.exprs_) {
    auto exprs = *gv.exprs_;
    auto exprsPtr = std::make_unique<decltype(exprs)>(std::move(exprs));
    setExprs(std::move(exprsPtr));
  }
}

std::unique_ptr<PlanNodeDescription> ScanEdges::explain() const {
  auto desc = Explore::explain();
  addDescription("props", props_ ? folly::toJson(util::toJson(*props_)) : "", desc.get());
  addDescription("exprs", exprs_ ? folly::toJson(util::toJson(*exprs_)) : "", desc.get());
  return desc;
}

PlanNode* ScanEdges::clone() const {
  auto* newGE = ScanEdges::make(qctx_, nullptr, space_);
  newGE->cloneMembers(*this);
  return newGE;
}

void ScanEdges::accept(PlanNodeVisitor* visitor) {
  visitor->visit(this);
}

void ScanEdges::cloneMembers(const ScanEdges& ge) {
  Explore::cloneMembers(ge);

  if (ge.props_) {
    auto edgeProps = *ge.props_;
    auto edgePropsPtr = std::make_unique<decltype(edgeProps)>(std::move(edgeProps));
    setEdgeProps(std::move(edgePropsPtr));
  }

  if (ge.exprs_) {
    auto exprs = *ge.exprs_;
    auto exprsPtr = std::make_unique<decltype(exprs)>(std::move(exprs));
    setExprs(std::move(exprsPtr));
  }
}

Filter::Filter(QueryContext* qctx, PlanNode* input, Expression* condition, bool needStableFilter)
    : SingleInputNode(qctx, Kind::kFilter, input) {
  condition_ = condition;
  needStableFilter_ = needStableFilter;
  copyInputColNames(input);
}

std::unique_ptr<PlanNodeDescription> Filter::explain() const {
  auto desc = SingleInputNode::explain();
  addDescription("condition", condition_ ? condition_->toString() : "", desc.get());
  addDescription("isStable", needStableFilter_ ? "true" : "false", desc.get());
  return desc;
}

void Filter::accept(PlanNodeVisitor* visitor) {
  visitor->visit(this);
}

PlanNode* Filter::clone() const {
  auto* newFilter = Filter::make(qctx_, nullptr);
  newFilter->cloneMembers(*this);
  return newFilter;
}

void Filter::cloneMembers(const Filter& f) {
  SingleInputNode::cloneMembers(f);

  condition_ = f.condition()->clone();
  needStableFilter_ = f.needStableFilter();
}

void SetOp::cloneMembers(const SetOp& s) {
  BinaryInputNode::cloneMembers(s);
}

PlanNode* Union::clone() const {
  auto* newUnion = Union::make(qctx_, nullptr, nullptr);
  newUnion->cloneMembers(*this);
  return newUnion;
}

void Union::cloneMembers(const Union& f) {
  SetOp::cloneMembers(f);
}

void Union::accept(PlanNodeVisitor* visitor) {
  visitor->visit(this);
}

PlanNode* Intersect::clone() const {
  auto* newIntersect = Intersect::make(qctx_, nullptr, nullptr);
  newIntersect->cloneMembers(*this);
  return newIntersect;
}

void Intersect::cloneMembers(const Intersect& f) {
  SetOp::cloneMembers(f);
}

PlanNode* Minus::clone() const {
  auto* newMinus = Minus::make(qctx_, nullptr, nullptr);
  newMinus->cloneMembers(*this);
  return newMinus;
}

void Minus::cloneMembers(const Minus& f) {
  SetOp::cloneMembers(f);
}

Project::Project(QueryContext* qctx, PlanNode* input, YieldColumns* cols)
    : SingleInputNode(qctx, Kind::kProject, input), cols_(cols) {
  if (cols_ != nullptr) {
    setColNames(cols_->names());
  }
}

std::unique_ptr<PlanNodeDescription> Project::explain() const {
  auto desc = SingleInputNode::explain();
  auto columns = folly::dynamic::array();
  if (cols_) {
    for (const auto* col : cols_->columns()) {
      DCHECK(col != nullptr);
      columns.push_back(col->toString());
    }
  }
  addDescription("columns", folly::toJson(columns), desc.get());
  return desc;
}

void Project::accept(PlanNodeVisitor* visitor) {
  visitor->visit(this);
}

PlanNode* Project::clone() const {
  auto* newProj = Project::make(qctx_, nullptr);
  newProj->cloneMembers(*this);
  return newProj;
}

void Project::cloneMembers(const Project& p) {
  SingleInputNode::cloneMembers(p);

  cols_ = qctx_->objPool()->makeAndAdd<YieldColumns>();
  for (const auto& col : p.columns()->columns()) {
    cols_->addColumn(col->clone().release());
  }
}

std::unique_ptr<PlanNodeDescription> Unwind::explain() const {
  auto desc = SingleInputNode::explain();
  addDescription("alias", alias(), desc.get());
  addDescription("unwindExpr", unwindExpr()->toString(), desc.get());
  return desc;
}

PlanNode* Unwind::clone() const {
  auto* newUnwind = Unwind::make(qctx_, nullptr);
  newUnwind->setFromPipe(fromPipe_);
  newUnwind->cloneMembers(*this);
  return newUnwind;
}

void Unwind::cloneMembers(const Unwind& p) {
  SingleInputNode::cloneMembers(p);

  unwindExpr_ = p.unwindExpr()->clone();
  alias_ = p.alias();
}

void Unwind::accept(PlanNodeVisitor* visitor) {
  visitor->visit(this);
}

std::unique_ptr<PlanNodeDescription> Sort::explain() const {
  auto desc = SingleInputNode::explain();
  addDescription("factors", folly::toJson(util::toJson(factorsString())), desc.get());
  return desc;
}

PlanNode* Sort::clone() const {
  auto* newSort = Sort::make(qctx_, nullptr);
  newSort->cloneMembers(*this);
  return newSort;
}

void Sort::cloneMembers(const Sort& p) {
  SingleInputNode::cloneMembers(p);

  std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
  for (const auto& factor : p.factors()) {
    factors.emplace_back(factor);
  }
  factors_ = std::move(factors);
}

// Get constant count value
int64_t Limit::count(QueryContext* qctx) const {
  if (count_ == nullptr) {
    return -1;
  }
  DCHECK(ExpressionUtils::isEvaluableExpr(count_, qctx));
  auto s = count_->eval(QueryExpressionContext(qctx ? qctx->ectx() : nullptr)()).getInt();
  DCHECK_GE(s, 0);
  return s;
}

std::unique_ptr<PlanNodeDescription> Limit::explain() const {
  auto desc = SingleInputNode::explain();
  addDescription("offset", folly::to<std::string>(offset_), desc.get());
  addDescription("count", count_->toString(), desc.get());
  return desc;
}

PlanNode* Limit::clone() const {
  auto* newLimit = Limit::make(qctx_, nullptr, -1, nullptr);
  newLimit->cloneMembers(*this);
  return newLimit;
}

void Limit::cloneMembers(const Limit& l) {
  SingleInputNode::cloneMembers(l);

  offset_ = l.offset_;
  count_ = l.count_;
}

std::unique_ptr<PlanNodeDescription> TopN::explain() const {
  auto desc = SingleInputNode::explain();
  addDescription("factors", folly::toJson(util::toJson(factorsString())), desc.get());
  addDescription("offset", folly::to<std::string>(offset_), desc.get());
  addDescription("count", folly::to<std::string>(count_), desc.get());
  return desc;
}

PlanNode* TopN::clone() const {
  auto* newTopN = TopN::make(qctx_, nullptr);
  newTopN->cloneMembers(*this);
  return newTopN;
}

void TopN::cloneMembers(const TopN& l) {
  SingleInputNode::cloneMembers(l);

  std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
  for (const auto& factor : l.factors()) {
    factors.emplace_back(factor);
  }
  factors_ = std::move(factors);
  offset_ = l.offset_;
  count_ = l.count_;
}

// Get constant count
int64_t Sample::count() const {
  DCHECK(ExpressionUtils::isEvaluableExpr(count_));
  QueryExpressionContext qec;
  auto count = count_->eval(qec).getInt();
  DCHECK_GE(count, 0);
  return count;
}

std::unique_ptr<PlanNodeDescription> Sample::explain() const {
  auto desc = SingleInputNode::explain();
  addDescription("count", count_->toString(), desc.get());
  return desc;
}

PlanNode* Sample::clone() const {
  auto* newSample = Sample::make(qctx_, nullptr, -1);
  newSample->cloneMembers(*this);
  return newSample;
}

void Sample::cloneMembers(const Sample& l) {
  SingleInputNode::cloneMembers(l);

  count_ = l.count_->clone();
}

Aggregate::Aggregate(QueryContext* qctx,
                     PlanNode* input,
                     std::vector<Expression*>&& groupKeys,
                     std::vector<Expression*>&& groupItems)
    : SingleInputNode(qctx, Kind::kAggregate, input) {
  groupKeys_ = std::move(groupKeys);
  groupItems_ = std::move(groupItems);
}

std::unique_ptr<PlanNodeDescription> Aggregate::explain() const {
  auto desc = SingleInputNode::explain();
  addDescription("groupKeys", folly::toJson(util::toJson(groupKeys_)), desc.get());
  folly::dynamic itemArr = folly::dynamic::array();
  for (auto* item : groupItems_) {
    folly::dynamic itemObj = folly::dynamic::object();
    itemObj.insert("expr", item ? item->toString() : "");
    itemArr.push_back(itemObj);
  }
  addDescription("groupItems", folly::toJson(itemArr), desc.get());
  return desc;
}

void Aggregate::accept(PlanNodeVisitor* visitor) {
  visitor->visit(this);
}

PlanNode* Aggregate::clone() const {
  auto* newAggregate = Aggregate::make(qctx_, nullptr);
  newAggregate->cloneMembers(*this);
  return newAggregate;
}

void Aggregate::cloneMembers(const Aggregate& agg) {
  SingleInputNode::cloneMembers(agg);

  std::vector<Expression*> gKeys;
  std::vector<Expression*> gItems;
  for (auto* expr : agg.groupKeys()) {
    gKeys.emplace_back(expr->clone());
  }
  for (auto* expr : agg.groupItems()) {
    gItems.emplace_back(expr->clone());
  }
  groupKeys_ = std::move(gKeys);
  groupItems_ = std::move(gItems);
}

std::unique_ptr<PlanNodeDescription> SwitchSpace::explain() const {
  auto desc = SingleInputNode::explain();
  addDescription("space", spaceName_, desc.get());
  return desc;
}

PlanNode* SwitchSpace::clone() const {
  auto* newSs = SwitchSpace::make(qctx_, nullptr, spaceName_);
  newSs->cloneMembers(*this);
  return newSs;
}

void SwitchSpace::cloneMembers(const SwitchSpace& l) {
  SingleInputNode::cloneMembers(l);
}

Dedup::Dedup(QueryContext* qctx, PlanNode* input) : SingleInputNode(qctx, Kind::kDedup, input) {
  copyInputColNames(input);
}

PlanNode* Dedup::clone() const {
  auto* newDedup = Dedup::make(qctx_, nullptr);
  newDedup->cloneMembers(*this);
  return newDedup;
}

void Dedup::cloneMembers(const Dedup& l) {
  SingleInputNode::cloneMembers(l);
}

std::unique_ptr<PlanNodeDescription> DataCollect::explain() const {
  auto desc = VariableDependencyNode::explain();
  addDescription("inputVar", folly::toJson(util::toJson(inputVars_)), desc.get());
  addDescription("distinct", distinct_ ? "true" : "false", desc.get());
  switch (kind_) {
    case DCKind::kSubgraph: {
      addDescription("kind", "SUBGRAPH", desc.get());
      break;
    }
    case DCKind::kRowBasedMove: {
      addDescription("kind", "ROW", desc.get());
      break;
    }
    case DCKind::kBFSShortest: {
      addDescription("kind", "BFS SHORTEST", desc.get());
      break;
    }
    case DCKind::kAllPaths: {
      addDescription("kind", "ALL PATHS", desc.get());
      break;
    }
    case DCKind::kMultiplePairShortest: {
      addDescription("kind", "Multiple Pair Shortest", desc.get());
      break;
    }
    case DCKind::kPathProp: {
      addDescription("kind", "PathProp", desc.get());
      break;
    }
  }
  return desc;
}

PlanNode* DataCollect::clone() const {
  auto* newDataCollect = DataCollect::make(qctx_, kind_);
  newDataCollect->cloneMembers(*this);
  return newDataCollect;
}

void DataCollect::cloneMembers(const DataCollect& l) {
  VariableDependencyNode::cloneMembers(l);
  distinct_ = l.distinct();
}

std::unique_ptr<PlanNodeDescription> Join::explain() const {
  auto desc = SingleDependencyNode::explain();
  folly::dynamic inputVar = folly::dynamic::object();
  folly::dynamic leftVar = folly::dynamic::object();
  leftVar.insert(leftVar_.first, leftVar_.second);
  inputVar.insert("leftVar", std::move(leftVar));
  folly::dynamic rightVar = folly::dynamic::object();
  rightVar.insert(rightVar_.first, rightVar_.second);
  inputVar.insert("rightVar", std::move(rightVar));
  addDescription("inputVar", folly::toJson(inputVar), desc.get());
  addDescription("hashKeys", folly::toJson(util::toJson(hashKeys_)), desc.get());
  addDescription("probeKeys", folly::toJson(util::toJson(probeKeys_)), desc.get());
  return desc;
}

void Join::cloneMembers(const Join& j) {
  SingleDependencyNode::cloneMembers(j);

  leftVar_ = j.leftVar();
  rightVar_ = j.rightVar();

  std::vector<Expression*> hKeys;
  for (auto* item : j.hashKeys()) {
    hKeys.emplace_back(item->clone());
  }
  hashKeys_ = std::move(hKeys);

  std::vector<Expression*> pKeys;
  for (auto* item : j.probeKeys()) {
    pKeys.emplace_back(item->clone());
  }
  probeKeys_ = std::move(pKeys);
}

Join::Join(QueryContext* qctx,
           Kind kind,
           PlanNode* input,
           std::pair<std::string, int64_t> leftVar,
           std::pair<std::string, int64_t> rightVar,
           std::vector<Expression*> hashKeys,
           std::vector<Expression*> probeKeys)
    : SingleDependencyNode(qctx, kind, input),
      leftVar_(std::move(leftVar)),
      rightVar_(std::move(rightVar)),
      hashKeys_(std::move(hashKeys)),
      probeKeys_(std::move(probeKeys)) {
  inputVars_.clear();
  readVariable(leftVar_.first);
  readVariable(rightVar_.first);
}

std::unique_ptr<PlanNodeDescription> InnerJoin::explain() const {
  auto desc = Join::explain();
  addDescription("kind", "InnerJoin", desc.get());
  return desc;
}

PlanNode* InnerJoin::clone() const {
  auto* newInnerJoin = InnerJoin::make(qctx_, nullptr, leftVar_, rightVar_);
  newInnerJoin->cloneMembers(*this);
  return newInnerJoin;
}

void InnerJoin::cloneMembers(const InnerJoin& l) {
  Join::cloneMembers(l);
}

std::unique_ptr<PlanNodeDescription> Assign::explain() const {
  auto desc = SingleDependencyNode::explain();
  for (const auto& item : items_) {
    addDescription("varName", item.first, desc.get());
    addDescription("value", item.second->toString(), desc.get());
  }
  return desc;
}

PlanNode* Assign::clone() const {
  auto* newAssign = Assign::make(qctx_, nullptr);
  newAssign->cloneMembers(*this);
  return newAssign;
}

void Assign::cloneMembers(const Assign& f) {
  SingleInputNode::cloneMembers(f);

  for (const std::pair<std::string, Expression*>& item : f.items()) {
    std::pair<std::string, Expression*> newItem;
    newItem.first = item.first;
    newItem.second = item.second->clone();
    items_.emplace_back(std::move(newItem));
  }
}

PlanNode* UnionAllVersionVar::clone() const {
  auto* newUv = UnionAllVersionVar::make(qctx_, nullptr);
  newUv->cloneMembers(*this);
  return newUv;
}

void UnionAllVersionVar::cloneMembers(const UnionAllVersionVar& f) {
  SingleInputNode::cloneMembers(f);
}

Traverse* Traverse::clone() const {
  auto newGN = Traverse::make(qctx_, nullptr, space_);
  newGN->cloneMembers(*this);
  return newGN;
}

void Traverse::cloneMembers(const Traverse& g) {
  GetNeighbors::cloneMembers(g);

  setStepRange(g.range_);
  if (g.vFilter_ != nullptr) {
    setVertexFilter(g.vFilter_->clone());
  }
  if (g.eFilter_ != nullptr) {
    setEdgeFilter(g.eFilter_->clone());
  }
  setTrackPrevPath(g.trackPrevPath_);
  if (g.firstStepFilter_ != nullptr) {
    setFirstStepFilter(g.firstStepFilter_->clone());
  }
  if (g.tagFilter_ != nullptr) {
    setTagFilter(g.tagFilter_->clone());
  }
  genPath_ = g.genPath();
}

std::unique_ptr<PlanNodeDescription> Traverse::explain() const {
  auto desc = GetNeighbors::explain();
  addDescription("steps", range_.toString(), desc.get());
  addDescription("vertex filter", vFilter_ != nullptr ? vFilter_->toString() : "", desc.get());
  addDescription("edge filter", eFilter_ != nullptr ? eFilter_->toString() : "", desc.get());
  addDescription("if_track_previous_path", folly::toJson(util::toJson(trackPrevPath_)), desc.get());
  addDescription("first step filter",
                 firstStepFilter_ != nullptr ? firstStepFilter_->toString() : "",
                 desc.get());
  addDescription("tag filter", tagFilter_ != nullptr ? tagFilter_->toString() : "", desc.get());
  return desc;
}

void Traverse::accept(PlanNodeVisitor* visitor) {
  visitor->visit(this);
}

AppendVertices* AppendVertices::clone() const {
  auto newAV = AppendVertices::make(qctx_, nullptr, space_);
  newAV->cloneMembers(*this);
  return newAV;
}

void AppendVertices::cloneMembers(const AppendVertices& a) {
  GetVertices::cloneMembers(a);

  if (a.vFilter_ != nullptr) {
    setVertexFilter(a.vFilter_->clone());
  } else {
    setVertexFilter(nullptr);
  }
  setTrackPrevPath(a.trackPrevPath_);
}

std::unique_ptr<PlanNodeDescription> AppendVertices::explain() const {
  auto desc = GetVertices::explain();
  addDescription("vertex_filter", vFilter_ != nullptr ? vFilter_->toString() : "", desc.get());
  addDescription("if_track_previous_path", folly::toJson(util::toJson(trackPrevPath_)), desc.get());
  return desc;
}

void AppendVertices::accept(PlanNodeVisitor* visitor) {
  visitor->visit(this);
}

std::unique_ptr<PlanNodeDescription> HashJoin::explain() const {
  auto desc = BinaryInputNode::explain();
  addDescription("hashKeys", folly::toJson(util::toJson(hashKeys_)), desc.get());
  addDescription("probeKeys", folly::toJson(util::toJson(probeKeys_)), desc.get());
  return desc;
}

void HashJoin::accept(PlanNodeVisitor* visitor) {
  visitor->visit(this);
}

void HashJoin::cloneMembers(const HashJoin& j) {
  BinaryInputNode::cloneMembers(j);

  std::vector<Expression*> hKeys;
  for (auto* item : j.hashKeys()) {
    hKeys.emplace_back(item->clone());
  }
  hashKeys_ = std::move(hKeys);

  std::vector<Expression*> pKeys;
  for (auto* item : j.probeKeys()) {
    pKeys.emplace_back(item->clone());
  }
  probeKeys_ = std::move(pKeys);
}

HashJoin::HashJoin(QueryContext* qctx,
                   Kind kind,
                   PlanNode* left,
                   PlanNode* right,
                   std::vector<Expression*> hashKeys,
                   std::vector<Expression*> probeKeys)
    : BinaryInputNode(qctx, kind, left, right),
      hashKeys_(std::move(hashKeys)),
      probeKeys_(std::move(probeKeys)) {
  if (left && right) {
    auto lColNames = left->colNames();
    for (auto& rColName : right->colNames()) {
      if (std::find(lColNames.begin(), lColNames.end(), rColName) == lColNames.end()) {
        lColNames.emplace_back(rColName);
      }
    }
    setColNames(lColNames);
  }
}

std::unique_ptr<PlanNodeDescription> HashLeftJoin::explain() const {
  auto desc = HashJoin::explain();
  addDescription("kind", "HashLeftJoin", desc.get());
  return desc;
}

PlanNode* HashLeftJoin::clone() const {
  auto* newLeftJoin = HashLeftJoin::make(qctx_, nullptr, nullptr);
  newLeftJoin->cloneMembers(*this);
  return newLeftJoin;
}

void HashLeftJoin::cloneMembers(const HashLeftJoin& l) {
  HashJoin::cloneMembers(l);
}

std::unique_ptr<PlanNodeDescription> HashInnerJoin::explain() const {
  auto desc = HashJoin::explain();
  addDescription("kind", "InnerJoin", desc.get());
  return desc;
}

PlanNode* HashInnerJoin::clone() const {
  auto* newInnerJoin = HashInnerJoin::make(qctx_, nullptr, nullptr);
  newInnerJoin->cloneMembers(*this);
  return newInnerJoin;
}

void HashInnerJoin::cloneMembers(const HashInnerJoin& l) {
  HashJoin::cloneMembers(l);
}

std::unique_ptr<PlanNodeDescription> CrossJoin::explain() const {
  return BinaryInputNode::explain();
}

PlanNode* CrossJoin::clone() const {
  auto* node = make(qctx_);
  node->cloneMembers(*this);
  return node;
}

void CrossJoin::cloneMembers(const CrossJoin& r) {
  BinaryInputNode::cloneMembers(r);
}

CrossJoin::CrossJoin(QueryContext* qctx, PlanNode* left, PlanNode* right)
    : BinaryInputNode(qctx, Kind::kCrossJoin, left, right) {
  auto lColNames = left->colNames();
  auto rColNames = right->colNames();
  lColNames.insert(lColNames.end(), rColNames.begin(), rColNames.end());
  setColNames(lColNames);
}

void CrossJoin::accept(PlanNodeVisitor* visitor) {
  visitor->visit(this);
}

CrossJoin::CrossJoin(QueryContext* qctx) : BinaryInputNode(qctx, Kind::kCrossJoin) {}

std::unique_ptr<PlanNodeDescription> RollUpApply::explain() const {
  auto desc = BinaryInputNode::explain();
  addDescription("compareCols", folly::toJson(util::toJson(compareCols_)), desc.get());
  addDescription("collectCol", folly::toJson(util::toJson(collectCol_)), desc.get());
  return desc;
}

void RollUpApply::accept(PlanNodeVisitor* visitor) {
  visitor->visit(this);
}

RollUpApply::RollUpApply(QueryContext* qctx,
                         Kind kind,
                         PlanNode* left,
                         PlanNode* right,
                         std::vector<Expression*> compareCols,
                         InputPropertyExpression* collectCol)
    : BinaryInputNode(qctx, kind, left, right),
      compareCols_(std::move(compareCols)),
      collectCol_(collectCol) {}

void RollUpApply::cloneMembers(const RollUpApply& r) {
  BinaryInputNode::cloneMembers(r);
  for (const auto* col : r.compareCols_) {
    compareCols_.emplace_back(col->clone());
  }
  collectCol_ = static_cast<InputPropertyExpression*>(DCHECK_NOTNULL(r.collectCol_)->clone());
}

PlanNode* RollUpApply::clone() const {
  auto* newRollUpApply = RollUpApply::make(qctx_, nullptr, nullptr, {}, nullptr);
  newRollUpApply->cloneMembers(*this);
  return newRollUpApply;
}

std::unique_ptr<PlanNodeDescription> PatternApply::explain() const {
  auto desc = BinaryInputNode::explain();
  addDescription("keyCols", folly::toJson(util::toJson(keyCols_)), desc.get());
  return desc;
}

void PatternApply::accept(PlanNodeVisitor* visitor) {
  visitor->visit(this);
}

PatternApply::PatternApply(QueryContext* qctx,
                           Kind kind,
                           PlanNode* left,
                           PlanNode* right,
                           std::vector<Expression*> keyCols,
                           bool isAntiPred)
    : BinaryInputNode(qctx, kind, left, right),
      keyCols_(std::move(keyCols)),
      isAntiPred_(isAntiPred) {}

void PatternApply::cloneMembers(const PatternApply& r) {
  BinaryInputNode::cloneMembers(r);
  for (const auto* col : r.keyCols_) {
    keyCols_.emplace_back(col->clone());
  }
  isAntiPred_ = r.isAntiPred_;
}

PlanNode* PatternApply::clone() const {
  auto* lnode = left() ? left()->clone() : nullptr;
  auto* rnode = right() ? right()->clone() : nullptr;
  auto* newPatternApply = PatternApply::make(qctx_, lnode, rnode, {});
  newPatternApply->cloneMembers(*this);
  return newPatternApply;
}

PlanNode* FulltextIndexScan::clone() const {
  auto ret = FulltextIndexScan::make(qctx_, searchExpr_, isEdge_);
  ret->cloneMembers(*this);
  return ret;
}

std::unique_ptr<PlanNodeDescription> FulltextIndexScan::explain() const {
  auto desc = Explore::explain();
  addDescription("isEdge", folly::to<std::string>(isEdge_), desc.get());
  addDescription("offset", folly::to<std::string>(offset_), desc.get());
  addDescription("searchExpr", searchExpr_->toString(), desc.get());
  return desc;
}

}  // namespace graph
}  // namespace nebula
