/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/plan/Query.h"

#include <folly/String.h>
#include <folly/dynamic.h>
#include <folly/json.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "util/ToJson.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

std::unique_ptr<PlanNodeDescription> Explore::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("space", folly::to<std::string>(space_), desc.get());
    addDescription("dedup", util::toJson(dedup_), desc.get());
    addDescription("limit", folly::to<std::string>(limit_), desc.get());
    auto filter = filter_.empty() ? filter_ : Expression::decode(filter_)->toString();
    addDescription("filter", filter, desc.get());
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
    addDescription("edgeDirection",
                   apache::thrift::util::enumNameSafe(edgeDirection_),
                   desc.get());
    addDescription(
        "vertexProps", vertexProps_ ? folly::toJson(util::toJson(*vertexProps_)) : "", desc.get());
    addDescription(
        "edgeProps", edgeProps_ ? folly::toJson(util::toJson(*edgeProps_)) : "", desc.get());
    addDescription(
        "statProps", statProps_ ? folly::toJson(util::toJson(*statProps_)) : "", desc.get());
    addDescription("exprs", exprs_ ? folly::toJson(util::toJson(*exprs_)) : "", desc.get());
    addDescription("random", util::toJson(random_), desc.get());
    return desc;
}

PlanNode* GetNeighbors::clone() const {
    auto* newGN = GetNeighbors::make(qctx_, nullptr, space_);
    newGN->cloneMembers(*this);
    return newGN;
}

void GetNeighbors::cloneMembers(const GetNeighbors& g) {
    Explore::cloneMembers(g);

    setSrc(qctx_->objPool()->add(g.src_->clone().release()));
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


std::unique_ptr<PlanNodeDescription> GetVertices::explain() const {
    auto desc = Explore::explain();
    addDescription("src", src_ ? src_->toString() : "", desc.get());
    addDescription("props", folly::toJson(util::toJson(props_)), desc.get());
    addDescription("exprs", folly::toJson(util::toJson(exprs_)), desc.get());
    return desc;
}

PlanNode* GetVertices::clone() const {
    auto* newGV = GetVertices::make(qctx_, nullptr, space_);
    newGV->cloneMembers(*this);
    return newGV;
}

void GetVertices::cloneMembers(const GetVertices& gv) {
    Explore::cloneMembers(gv);

    src_ = qctx_->objPool()->add(gv.src()->clone().release());

    std::vector<storage::cpp2::VertexProp> props;
    const auto& gvProps = gv.props();
    props.reserve(gvProps.size());
    for (const auto& prop : gvProps) {
        props.emplace_back(prop);
    }
    props_ = std::move(props);

    std::vector<storage::cpp2::Expr> exprs;
    const auto& gvExprs = gv.exprs();
    exprs.reserve(gvExprs.size());
    for (const auto& expr : gvExprs) {
        exprs.emplace_back(expr);
    }
    exprs_ = std::move(exprs);
}


std::unique_ptr<PlanNodeDescription> GetEdges::explain() const {
    auto desc = Explore::explain();
    addDescription("src", src_ ? src_->toString() : "", desc.get());
    addDescription("type", util::toJson(type_), desc.get());
    addDescription("ranking", ranking_ ? ranking_->toString() : "", desc.get());
    addDescription("dst", dst_ ? dst_->toString() : "", desc.get());
    addDescription("props", folly::toJson(util::toJson(props_)), desc.get());
    addDescription("exprs", folly::toJson(util::toJson(exprs_)), desc.get());
    return desc;
}

PlanNode* GetEdges::clone() const {
    auto* newGE = GetEdges::make(qctx_, nullptr, space_);
    newGE->cloneMembers(*this);
    return newGE;
}

void GetEdges::cloneMembers(const GetEdges& ge) {
    Explore::cloneMembers(ge);

    src_ = qctx_->objPool()->add(ge.src()->clone().release());
    type_ = qctx_->objPool()->add(ge.type()->clone().release());
    ranking_ = qctx_->objPool()->add(ge.ranking()->clone().release());
    dst_ = qctx_->objPool()->add(ge.dst()->clone().release());

    std::vector<storage::cpp2::EdgeProp> props;
    const auto& geProps = ge.props();
    props.reserve(geProps.size());
    for (const auto& prop : geProps) {
        props.emplace_back(prop);
    }
    props_ = std::move(props);

    std::vector<storage::cpp2::Expr> exprs;
    const auto& geExprs = ge.exprs();
    exprs.reserve(geExprs.size());
    for (const auto& expr : geExprs) {
        exprs.emplace_back(expr);
    }
    exprs_ = std::move(exprs);
}


std::unique_ptr<PlanNodeDescription> IndexScan::explain() const {
    auto desc = Explore::explain();
    addDescription("schemaId", util::toJson(schemaId_), desc.get());
    addDescription("isEdge", util::toJson(isEdge_), desc.get());
    addDescription("returnCols", folly::toJson(util::toJson(*returnCols_)), desc.get());
    addDescription("indexCtx", folly::toJson(util::toJson(*contexts_)), desc.get());
    return desc;
}

PlanNode* IndexScan::clone() const {
    auto* newIndexScan = IndexScan::make(qctx_, nullptr);
    newIndexScan->cloneMembers(*this);
    return newIndexScan;
}

void IndexScan::cloneMembers(const IndexScan &g) {
    Explore::cloneMembers(g);

    if (g.contexts_ != nullptr) {
        contexts_ = std::make_unique<std::vector<storage::cpp2::IndexQueryContext>>(*g.contexts_);
    }
    if (g.returnCols_ != nullptr) {
        returnCols_ = std::make_unique<std::vector<std::string>>(*g.returnCols_);
    }
    isEdge_ = g.isEdge();
    schemaId_ = g.schemaId();
    isEmptyResultSet_ = g.isEmptyResultSet();
}


std::unique_ptr<PlanNodeDescription> Filter::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("condition", condition_ ? condition_->toString() : "", desc.get());
    return desc;
}

PlanNode* Filter::clone() const {
    auto* newFilter = Filter::make(qctx_, nullptr);
    newFilter->cloneMembers(*this);
    return newFilter;
}

void Filter::cloneMembers(const Filter& f) {
    SingleInputNode::cloneMembers(f);

    condition_ = qctx_->objPool()->add(f.condition()->clone().release());
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

PlanNode* Project::clone() const {
    auto* newProj = Project::make(qctx_, nullptr);
    newProj->cloneMembers(*this);
    return newProj;
}

void Project::cloneMembers(const Project &p) {
    SingleInputNode::cloneMembers(p);

    cols_ = qctx_->objPool()->add(new YieldColumns());
    for (const auto &col : p.columns()->columns()) {
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
    newUnwind->cloneMembers(*this);
    return newUnwind;
}

void Unwind::cloneMembers(const Unwind &p) {
    SingleInputNode::cloneMembers(p);

    unwindExpr_ = qctx_->objPool()->add(p.unwindExpr()->clone().release());
    alias_ = p.alias();
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

void Sort::cloneMembers(const Sort &p) {
    SingleInputNode::cloneMembers(p);

    std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
    for (const auto &factor : p.factors()) {
        factors.emplace_back(factor);
    }
    factors_ = std::move(factors);
}


std::unique_ptr<PlanNodeDescription> Limit::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("offset", folly::to<std::string>(offset_), desc.get());
    addDescription("count", folly::to<std::string>(count_), desc.get());
    return desc;
}

PlanNode* Limit::clone() const {
    auto* newLimit = Limit::make(qctx_, nullptr);
    newLimit->cloneMembers(*this);
    return newLimit;
}

void Limit::cloneMembers(const Limit &l) {
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

void TopN::cloneMembers(const TopN &l) {
    SingleInputNode::cloneMembers(l);

    std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
    for (const auto &factor : l.factors()) {
        factors.emplace_back(factor);
    }
    factors_ = std::move(factors);
    offset_ = l.offset_;
    count_ = l.count_;
}


std::unique_ptr<PlanNodeDescription> Aggregate::explain() const {
    auto desc = SingleInputNode::explain();
    addDescription("groupKeys", folly::toJson(util::toJson(groupKeys_)), desc.get());
    folly::dynamic itemArr = folly::dynamic::array();
    for (auto* item : groupItems_) {
        folly::dynamic itemObj = folly::dynamic::object();
        itemObj.insert("expr", item? item->toString() : "");
        itemArr.push_back(itemObj);
    }
    addDescription("groupItems", folly::toJson(itemArr), desc.get());
    return desc;
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
        gKeys.emplace_back(qctx_->objPool()->add(expr->clone().release()));
    }
    for (auto* expr : agg.groupItems()) {
        gItems.emplace_back(qctx_->objPool()->add(expr->clone().release()));
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

void SwitchSpace::cloneMembers(const SwitchSpace &l) {
    SingleInputNode::cloneMembers(l);
}


PlanNode* Dedup::clone() const {
    auto* newDedup = Dedup::make(qctx_, nullptr);
    newDedup->cloneMembers(*this);
    return newDedup;
}

void Dedup::cloneMembers(const Dedup &l) {
    SingleInputNode::cloneMembers(l);
}


std::unique_ptr<PlanNodeDescription> DataCollect::explain() const {
    auto desc = VariableDependencyNode::explain();
    addDescription("inputVar", folly::toJson(util::toJson(inputVars_)), desc.get());
    switch (kind_) {
        case DCKind::kSubgraph: {
            addDescription("kind", "SUBGRAPH", desc.get());
            break;
        }
        case DCKind::kRowBasedMove: {
            addDescription("kind", "ROW", desc.get());
            break;
        }
        case DCKind::kMToN: {
            addDescription("kind", "M TO N", desc.get());
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
    }
    return desc;
}

PlanNode* DataCollect::clone() const {
    auto* newDataCollect = DataCollect::make(qctx_, kind_);
    newDataCollect->cloneMembers(*this);
    return newDataCollect;
}

void DataCollect::cloneMembers(const DataCollect &l) {
    VariableDependencyNode::cloneMembers(l);
    mToN_ = l.mToN();
    distinct_ = l.distinct();
}


std::unique_ptr<PlanNodeDescription> Join::explain() const {
    auto desc = SingleDependencyNode::explain();
    folly::dynamic inputVar = folly::dynamic::object();
    inputVar.insert("leftVar", util::toJson(leftVar_));
    inputVar.insert("rightVar", util::toJson(rightVar_));
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
        hKeys.emplace_back(qctx_->objPool()->add(item->clone().release()));
    }
    hashKeys_ = std::move(hKeys);

    std::vector<Expression*> pKeys;
    for (auto* item : j.probeKeys()) {
        pKeys.emplace_back(qctx_->objPool()->add(item->clone().release()));
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


std::unique_ptr<PlanNodeDescription> LeftJoin::explain() const {
    auto desc = Join::explain();
    addDescription("kind", "LeftJoin", desc.get());
    return desc;
}

PlanNode* LeftJoin::clone() const {
    auto* newLeftJoin = LeftJoin::make(qctx_, nullptr, leftVar_, rightVar_);
    newLeftJoin->cloneMembers(*this);
    return newLeftJoin;
}

void LeftJoin::cloneMembers(const LeftJoin &l) {
    Join::cloneMembers(l);
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

void InnerJoin::cloneMembers(const InnerJoin &l) {
    Join::cloneMembers(l);
}


std::unique_ptr<PlanNodeDescription> Assign::explain() const {
    auto desc = SingleDependencyNode::explain();
    for (size_t i = 0; i < items_.size(); ++i) {
        addDescription("varName", items_[i].first, desc.get());
        addDescription("value", items_[i].second->toString(), desc.get());
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

    for (const std::pair<std::string, std::unique_ptr<Expression>>& item : f.items()) {
        std::pair<std::string, std::unique_ptr<Expression>> newItem;
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

}   // namespace graph
}   // namespace nebula
