/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GetSubgraphValidator.h"

#include "common/expression/VariableExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/ConstantExpression.h"

#include "parser/TraverseSentences.h"
#include "planner/Query.h"
#include "context/ExpressionContextImpl.h"

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

Status GetSubgraphValidator::validateStep(StepClause* step) {
    if (step == nullptr) {
        return Status::Error("Step clause was not declared.");
    }

    if (step->isUpto()) {
        return Status::Error("Get Subgraph not support upto.");
    }
    steps_ = step->steps();
    return Status::OK();
}

Status GetSubgraphValidator::validateFrom(FromClause* from) {
    if (from == nullptr) {
        return Status::Error("From clause was not declared.");
    }

    ExpressionContextImpl ctx(nullptr, nullptr);
    if (from->isRef()) {
        srcRef_ = from->ref();
    } else {
        for (auto* expr : from->vidList()) {
            // TODO:
            auto vid = Expression::eval(expr, ctx);
            starts_.emplace_back(std::move(vid));
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateInBound(InBoundClause* in) {
    if (in != nullptr) {
        auto space = vctx_->whichSpace();
        for (auto* e : in->edges()) {
            if (e->alias() != nullptr) {
                return Status::Error("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return et.status();
            }

            auto v = -et.value();
            edgeTypes_.emplace_back(v);
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateOutBound(OutBoundClause* out) {
    if (out != nullptr) {
        auto space = vctx_->whichSpace();
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return Status::Error("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return et.status();
            }

            edgeTypes_.emplace_back(et.value());
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateBothInOutBound(BothInOutClause* out) {
    if (out != nullptr) {
        auto space = vctx_->whichSpace();
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return Status::Error("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            if (!et.ok()) {
                return et.status();
            }

            auto v = et.value();
            edgeTypes_.emplace_back(v);
            v = -v;
            edgeTypes_.emplace_back(v);
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::toPlan() {
    auto* plan = qctx_->plan();
    auto& space = vctx_->whichSpace();

    // TODO:
    // loop -> project -> gn1 -> bodyStart
    auto* bodyStart = StartNode::make(plan);

    std::vector<EdgeType> edgeTypes;
    auto vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>();
    auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    auto statProps = std::make_unique<std::vector<storage::cpp2::StatProp>>();
    auto exprs = std::make_unique<std::vector<storage::cpp2::Expr>>();
    auto vidsToSave = vctx_->varGen()->getVar();
    DataSet ds;
    ds.colNames.emplace_back(kVid);
    for (auto& vid : starts_) {
        Row row;
        row.values.emplace_back(vid);
        ds.rows.emplace_back(std::move(row));
    }

    ResultBuilder builder;
    builder.value(Value(std::move(ds))).iter(Iterator::Kind::kSequential);
    qctx_->ectx()->setResult(vidsToSave, builder.finish());
    auto* vids = new VariablePropertyExpression(
                     new std::string(vidsToSave),
                     new std::string(kVid));
    auto* gn1 = GetNeighbors::make(
            plan,
            bodyStart,
            space.id,
            plan->saveObject(vids),
            std::move(edgeTypes),
            storage::cpp2::EdgeDirection::BOTH,  // FIXME: make direction right
            std::move(vertexProps),
            std::move(edgeProps),
            std::move(statProps),
            std::move(exprs));
    gn1->setInputVar(vidsToSave);

    auto* columns = new YieldColumns();
    auto* column = new YieldColumn(
            new VariablePropertyExpression(
                new std::string("*"),
                new std::string(kDst)),
            new std::string(kVid));
    columns->addColumn(column);
    auto* project = Project::make(plan, gn1, plan->saveObject(columns));
    project->setInputVar(gn1->varName());
    project->setOutputVar(vidsToSave);
    project->setColNames(deduceColNames(columns));

    // ++counter{0} <= steps
    auto counter = vctx_->varGen()->getVar();
    qctx_->ectx()->setValue(counter, 0);
    auto* condition = new RelationalExpression(
                Expression::Kind::kRelLE,
                new UnaryExpression(
                        Expression::Kind::kUnaryIncr,
                        new VersionedVariableExpression(
                                new std::string(counter),
                                new ConstantExpression(0))),
                new ConstantExpression(static_cast<int32_t>(steps_)));
    // The input of loop will set by father validator.
    auto* loop = Loop::make(plan, nullptr, project, plan->saveObject(condition));

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
