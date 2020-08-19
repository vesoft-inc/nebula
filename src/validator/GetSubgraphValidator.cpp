/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GetSubgraphValidator.h"

#include "common/expression/UnaryExpression.h"

#include "util/ContainerConv.h"
#include "common/expression/VariableExpression.h"
#include "context/QueryExpressionContext.h"
#include "parser/TraverseSentences.h"
#include "planner/Logic.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {

Status GetSubgraphValidator::validateImpl() {
    Status status;
    auto* gsSentence = static_cast<GetSubgraphSentence*>(sentence_);
    do {
        status = validateStep(gsSentence->step());
        if (!status.ok()) {
            break;
        }

        status = validateFrom(gsSentence->from());
        if (!status.ok()) {
            return status;
        }

        status = validateInBound(gsSentence->in());
        if (!status.ok()) {
            return status;
        }

        status = validateOutBound(gsSentence->out());
        if (!status.ok()) {
            return status;
        }

        status = validateBothInOutBound(gsSentence->both());
        if (!status.ok()) {
            return status;
        }
    } while (false);

    return Status::OK();
}

Status GetSubgraphValidator::validateInBound(InBoundClause* in) {
    if (in != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = in->edges();
        edgeTypes_.reserve(edgeTypes_.size() + edges.size());
        for (auto* e : edges) {
            if (e->alias() != nullptr) {
                return Status::Error("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return et.status();
            }

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
                return Status::Error("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return et.status();
            }

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
                return Status::Error("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return et.status();
            }

            auto v = et.value();
            edgeTypes_.emplace(v);
            v = -v;
            edgeTypes_.emplace(v);
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto& space = vctx_->whichSpace();

    //                           bodyStart->gn->project(dst)
    //                                            |
    // start [->previous] [-> project(input)] -> loop -> collect
    auto* bodyStart = StartNode::make(plan);

    auto vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    auto statProps = std::make_unique<std::vector<storage::cpp2::StatProp>>();
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();

    std::string startVidsVar;
    PlanNode* projectStartVid = nullptr;
    if (!starts_.empty() && srcRef_ == nullptr) {
        startVidsVar = buildConstantInput();
    } else {
        projectStartVid = buildRuntimeInput();
        startVidsVar = projectStartVid->varName();
    }

    auto* gn1 = GetNeighbors::make(
            plan,
            bodyStart,
            space.id,
            src_,
            ContainerConv::to<std::vector>(std::move(edgeTypes_)),
            // TODO(shylock) add syntax like `BOTH *`, `OUT *` ...
            storage::cpp2::EdgeDirection::OUT_EDGE,  // FIXME: make direction right
            std::move(vertexProps),
            std::move(edgeProps),
            std::move(statProps),
            std::move(exprs),
            true /*subgraph not need duplicate*/);
    gn1->setInputVar(startVidsVar);

    auto *projectVids = projectDstVidsFromGN(gn1, startVidsVar);

    // ++counter{0} <= steps
    // TODO(shylock) add condition when gn get empty result
    auto* condition = buildNStepLoopCondition(steps_);
    // The input of loop will set by father validator.
    auto* loop = Loop::make(plan, nullptr, projectVids, condition);

    /*
    // selector -> loop
    // selector -> filter -> gn2 -> ifStrart
    auto* ifStart = StartNode::make(plan);

    std::vector<Row> starts;
    auto* gn2 = GetNeighbors::make(
            plan,
            ifStart,
            space.id,
            std::move(starts),
            vids1,
            std::move(edgeTypes),
            storage::cpp2::EdgeDirection::BOTH,  // FIXME: make edge direction right
            std::move(vertexProps),
            std::move(edgeProps),
            std::move(statProps));

    // collect(gn2._vids) as listofvids
    auto& listOfVids = varGen_->getVar();
    columns = new YieldColumns();
    column = new YieldColumn(
            new VariablePropertyExpression(
                new std::string(gn2->varGenerated()),
                new std::string(kVid)),
            new std::string(listOfVids));
    column->setFunction(new std::string("collect"));
    columns->addColumn(column);
    auto* group = Aggregate::make(plan, gn2, plan->saveObject(columns));

    auto* filter = Filter::make(
                        plan,
                        group,
                        nullptr// TODO: build IN condition.
                        );
    auto* selector = Selector::make(plan, loop, filter, nullptr, nullptr);

    // TODO: A data collector.

    root_ = selector;
    tail_ = loop;
    */
    std::vector<std::string> collects = {gn1->varName()};
    auto* dc = DataCollect::make(plan, loop,
            DataCollect::CollectKind::kSubgraph, std::move(collects));
    dc->setColNames({"_vertices", "_edges"});
    root_ = dc;
    tail_ = loop;
    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
