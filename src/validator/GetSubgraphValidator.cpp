/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GetSubgraphValidator.h"

#include "common/expression/UnaryExpression.h"
#include "common/expression/VariableExpression.h"
#include "common/expression/VertexExpression.h"
#include "context/QueryExpressionContext.h"
#include "parser/TraverseSentences.h"
#include "planner/Logic.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

Status GetSubgraphValidator::validateImpl() {
    auto* gsSentence = static_cast<GetSubgraphSentence*>(sentence_);

    NG_RETURN_IF_ERROR(validateStep(gsSentence->step(), steps_));
    NG_RETURN_IF_ERROR(validateStarts(gsSentence->from(), from_));
    NG_RETURN_IF_ERROR(validateInBound(gsSentence->in()));
    NG_RETURN_IF_ERROR(validateOutBound(gsSentence->out()));
    NG_RETURN_IF_ERROR(validateBothInOutBound(gsSentence->both()));

    if (!exprProps_.srcTagProps().empty() || !exprProps_.dstTagProps().empty()) {
        return Status::SemanticError("Only support input and variable in Subgraph sentence.");
    }
    if (!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) {
        return Status::SemanticError("Not support both input and variable in Subgraph sentence.");
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateInBound(InBoundClause* in) {
    if (in != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = in->edges();
        edgeTypes_.reserve(edgeTypes_.size() + edges.size());
        for (auto* e : edges) {
            if (e->alias() != nullptr) {
                return Status::SemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            NG_RETURN_IF_ERROR(et);

            auto v = -et.value();
            edgeTypes_.emplace(v);
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateOutBound(OutBoundClause* out) {
    if (out != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = out->edges();
        edgeTypes_.reserve(edgeTypes_.size() + edges.size());
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return Status::SemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            NG_RETURN_IF_ERROR(et);

            edgeTypes_.emplace(et.value());
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateBothInOutBound(BothInOutClause* out) {
    if (out != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = out->edges();
        edgeTypes_.reserve(edgeTypes_.size() + edges.size());
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return Status::SemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            NG_RETURN_IF_ERROR(et);

            auto v = et.value();
            edgeTypes_.emplace(v);
            v = -v;
            edgeTypes_.emplace(v);
        }
    }

    return Status::OK();
}

/*
 * 1 steps   history: collectVid{0}
 * 2 steps   history: collectVid{-1} collectVid{0}
 * 3 steps   history: collectVid{-2} collectVid{-1} collectVid{0}
 * ...
 * n steps   history: collectVid{-n+1} ...  collectVid{-1} collectVid{0}
 */
Expression* GetSubgraphValidator::buildFilterCondition(int64_t step) {
    // where *._dst IN startVids(*._dst IN runtimeStartVar_) OR *._dst IN collectVid{0}[0][0] OR
    // *._dst IN collectVid{-1}[0][0] OR ... *._dst IN collectVid{-step+1}[0][0]
    if (step == 1) {
        Expression* left = nullptr;
        if (!runtimeStartVar_.empty()) {
            auto* startDataSet = new VersionedVariableExpression(new std::string(runtimeStartVar_),
                                                                 new ConstantExpression(0));
            auto* runtimeStartList = new SubscriptExpression(
                new SubscriptExpression(startDataSet, new ConstantExpression(0)),
                new ConstantExpression(0));
            left = new RelationalExpression(Expression::Kind::kRelIn,
                                            new EdgeDstIdExpression(new std::string("*")),
                                            runtimeStartList);
        } else {
            left = new RelationalExpression(Expression::Kind::kRelIn,
                                            new EdgeDstIdExpression(new std::string("*")),
                                            new SetExpression(startVidList_.release()));
        }
        auto* lastestVidsDataSet = new VersionedVariableExpression(new std::string(collectVar_),
                                                                   new ConstantExpression(0));
        auto* lastestVidsList = new SubscriptExpression(
            new SubscriptExpression(lastestVidsDataSet, new ConstantExpression(0)),
            new ConstantExpression(0));
        auto* right = new RelationalExpression(Expression::Kind::kRelIn,
                                               new EdgeDstIdExpression(new std::string("*")),
                                               lastestVidsList);
        return new LogicalExpression(Expression::Kind::kLogicalOr, left, right);
    }
    auto* historyVidsDataSet = new VersionedVariableExpression(new std::string(collectVar_),
                                                               new ConstantExpression(1 - step));
    auto* historyVidsList = new SubscriptExpression(
        new SubscriptExpression(historyVidsDataSet, new ConstantExpression(0)),
        new ConstantExpression(0));
    auto* left = new RelationalExpression(
        Expression::Kind::kRelIn, new EdgeDstIdExpression(new std::string("*")), historyVidsList);
    auto* right = buildFilterCondition(step - 1);
    auto* result = new LogicalExpression(Expression::Kind::kLogicalOr, left, right);
    return result;
}

GetNeighbors::EdgeProps GetSubgraphValidator::buildEdgeProps() {
    GetNeighbors::EdgeProps edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    if (edgeTypes_.empty()) {
        return edgeProps;
    }
    edgeProps->reserve(edgeTypes_.size());
    for (auto& e : edgeTypes_) {
        storage::cpp2::EdgeProp ep;
        ep.set_type(e);
        edgeProps->emplace_back(std::move(ep));
    }
    return edgeProps;
}

Status GetSubgraphValidator::zeroStep(PlanNode* depend, const std::string& inputVar) {
    auto& space = vctx_->whichSpace();
    std::vector<storage::cpp2::Expr> exprs;
    std::vector<storage::cpp2::VertexProp> vertexProps;
    auto* getVertex = GetVertices::make(
        qctx_, depend, space.id, from_.src, std::move(vertexProps), std::move(exprs), true);
    getVertex->setInputVar(inputVar);

    auto var = vctx_->anonVarGen()->getVar();
    auto* column = new YieldColumn(new VertexExpression(), new std::string("_vertices"));
    qctx_->objPool()->add(column);
    column->setAggFunction(new std::string("COLLECT"));
    auto fun = column->getAggFunName();
    auto* collectVertex =
        Aggregate::make(qctx_,
                        getVertex,
                        {},
                        {Aggregate::GroupItem(column->expr(), AggFun::nameIdMap_[fun], false)});
    collectVertex->setInputVar(getVertex->outputVar());
    collectVertex->setColNames({"_vertices"});

    root_ = collectVertex;
    tail_ = projectStartVid_ != nullptr ? projectStartVid_ : getVertex;
    return Status::OK();
}

Status GetSubgraphValidator::toPlan() {
    auto& space = vctx_->whichSpace();
    // gn <- filter <- DataCollect
    //  |
    // loop(step) -> Agg(collect) -> project -> gn -> bodyStart
    auto* bodyStart = StartNode::make(qctx_);

    std::string startVidsVar;
    SingleInputNode* collectRunTimeStartVids = nullptr;
    if (!from_.vids.empty() && from_.originalSrc == nullptr) {
        buildConstantInput(from_, startVidsVar);
    } else {
        PlanNode* dedupStartVid = buildRuntimeInput(from_, projectStartVid_);
        startVidsVar = dedupStartVid->outputVar();
        // collect runtime startVids
        auto var = vctx_->anonVarGen()->getVar();
        auto* column = new YieldColumn(
            new VariablePropertyExpression(new std::string(var), new std::string(kVid)),
            new std::string(kVid));
        qctx_->objPool()->add(column);
        column->setAggFunction(new std::string("COLLECT_SET"));
        auto fun = column->getAggFunName();
        collectRunTimeStartVids =
            Aggregate::make(qctx_,
                            dedupStartVid,
                            {},
                            {Aggregate::GroupItem(column->expr(), AggFun::nameIdMap_[fun], true)});
        collectRunTimeStartVids->setInputVar(dedupStartVid->outputVar());
        collectRunTimeStartVids->setColNames({kVid});
        runtimeStartVar_ = collectRunTimeStartVids->outputVar();
    }

    if (steps_.steps == 0) {
        if (collectRunTimeStartVids == nullptr) {
            return zeroStep(bodyStart, startVidsVar);
        }
        return zeroStep(collectRunTimeStartVids, startVidsVar);
    }

    auto vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    auto* gn = GetNeighbors::make(qctx_, bodyStart, space.id);
    gn->setSrc(from_.src);
    gn->setVertexProps(std::move(vertexProps));
    gn->setEdgeProps(buildEdgeProps());
    gn->setEdgeDirection(storage::cpp2::EdgeDirection::BOTH);
    gn->setInputVar(startVidsVar);

    auto* projectVids = projectDstVidsFromGN(gn, startVidsVar);

    auto var = vctx_->anonVarGen()->getVar();
    auto* column = new YieldColumn(
        new VariablePropertyExpression(new std::string(var), new std::string(kVid)),
        new std::string(kVid));
    qctx_->objPool()->add(column);
    column->setAggFunction(new std::string("COLLECT_SET"));
    auto fun = column->getAggFunName();
    auto* collect =
        Aggregate::make(qctx_,
                        projectVids,
                        {},
                        {Aggregate::GroupItem(column->expr(), AggFun::nameIdMap_[fun], true)});
    collect->setInputVar(projectVids->outputVar());
    collect->setColNames({kVid});
    collectVar_ = collect->outputVar();

    // TODO(jmq) add condition when gn get empty result
    auto* condition = buildNStepLoopCondition(steps_.steps);
    auto* loop = Loop::make(qctx_, collectRunTimeStartVids, collect, condition);

    vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    auto* gn1 = GetNeighbors::make(qctx_, loop, space.id);
    gn1->setSrc(from_.src);
    gn1->setVertexProps(std::move(vertexProps));
    gn1->setEdgeProps(std::move(edgeProps));
    gn1->setEdgeDirection(storage::cpp2::EdgeDirection::BOTH);
    gn1->setInputVar(projectVids->outputVar());

    auto* filter =
        Filter::make(qctx_, gn1, qctx_->objPool()->add(buildFilterCondition(steps_.steps)));
    filter->setInputVar(gn1->outputVar());
    filter->setColNames({kVid});

    // datacollect
    std::vector<std::string> collects = {gn->outputVar(), filter->outputVar()};
    auto* dc =
        DataCollect::make(qctx_, filter, DataCollect::CollectKind::kSubgraph, std::move(collects));
    dc->setColNames({"_vertices", "_edges"});
    root_ = dc;
    tail_ = projectStartVid_ != nullptr ? projectStartVid_ : loop;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
