/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/TraversalValidator.h"
#include "common/expression/VariableExpression.h"

namespace nebula {
namespace graph {

Status TraversalValidator::validateStep(const StepClause* step) {
    if (step == nullptr) {
        return Status::Error("Step clause nullptr.");
    }
    if (step->isMToN()) {
        auto* mToN = qctx_->objPool()->makeAndAdd<StepClause::MToN>();
        mToN->mSteps = step->mToN()->mSteps;
        mToN->nSteps = step->mToN()->nSteps;

        if (mToN->mSteps == 0 && mToN->nSteps == 0) {
            steps_ = 0;
            return Status::OK();
        }
        if (mToN->mSteps == 0) {
            mToN->mSteps = 1;
        }
        if (mToN->nSteps < mToN->mSteps) {
            return Status::Error("`%s', upper bound steps should be greater than lower bound.",
                                 step->toString().c_str());
        }
        if (mToN->mSteps == mToN->nSteps) {
            steps_ = mToN->mSteps;
            return Status::OK();
        }
        mToN_ = mToN;
    } else {
        auto steps = step->steps();
        steps_ = steps;
    }
    return Status::OK();
}

Status TraversalValidator::validateFrom(const FromClause* from) {
    if (from == nullptr) {
        return Status::Error("From clause nullptr.");
    }
    if (from->isRef()) {
        auto* src = from->ref();
        if (src->kind() != Expression::Kind::kInputProperty
                && src->kind() != Expression::Kind::kVarProperty) {
            return Status::Error(
                    "`%s', Only input and variable expression is acceptable"
                    " when starts are evaluated at runtime.", src->toString().c_str());
        } else {
            fromType_ = src->kind() == Expression::Kind::kInputProperty ? kPipe : kVariable;
            auto type = deduceExprType(src);
            if (!type.ok()) {
                return type.status();
            }
            if (type.value() != Value::Type::STRING) {
                std::stringstream ss;
                ss << "`" << src->toString() << "', the srcs should be type of string, "
                   << "but was`" << type.value() << "'";
                return Status::Error(ss.str());
            }
            srcRef_ = src;
            auto* propExpr = static_cast<PropertyExpression*>(src);
            if (fromType_ == kVariable) {
                userDefinedVarName_ = *(propExpr->sym());
            }
            firstBeginningSrcVidColName_ = *(propExpr->prop());
        }
    } else {
        auto vidList = from->vidList();
        QueryExpressionContext ctx(qctx_->ectx(), nullptr);
        for (auto* expr : vidList) {
            if (!evaluableExpr(expr)) {
                return Status::Error("`%s' is not an evaluable expression.",
                        expr->toString().c_str());
            }
            auto vid = expr->eval(ctx);
            if (!vid.isStr()) {
                return Status::Error("Vid should be a string.");
            }
            starts_.emplace_back(std::move(vid));
        }
    }
    return Status::OK();
}


PlanNode* TraversalValidator::projectDstVidsFromGN(PlanNode* gn, const std::string& outputVar) {
    Project* project = nullptr;
    auto* plan = qctx_->plan();
    auto* columns = new YieldColumns();
    auto* column = new YieldColumn(
        new EdgePropertyExpression(new std::string("*"), new std::string(kDst)),
        new std::string(kVid));
    columns->addColumn(column);

    srcVidColName_ = vctx_->anonColGen()->getCol();
    if (!exprProps_.inputProps().empty() || !exprProps_.varProps().empty()) {
        column =
            new YieldColumn(new InputPropertyExpression(new std::string(kVid)),
                            new std::string(srcVidColName_));
        columns->addColumn(column);
    }

    project = Project::make(plan, gn, plan->saveObject(columns));
    project->setInputVar(gn->varName());
    project->setColNames(deduceColNames(columns));
    VLOG(1) << project->varName();

    auto* dedupDstVids = Dedup::make(plan, project);
    dedupDstVids->setInputVar(project->varName());
    dedupDstVids->setOutputVar(outputVar);
    dedupDstVids->setColNames(project->colNames());
    return dedupDstVids;
}

std::string TraversalValidator::buildConstantInput() {
    auto input = vctx_->anonVarGen()->getVar();
    DataSet ds;
    ds.colNames.emplace_back(kVid);
    for (auto& vid : starts_) {
        Row row;
        row.values.emplace_back(vid);
        ds.rows.emplace_back(std::move(row));
    }
    qctx_->ectx()->setResult(input, ResultBuilder().value(Value(std::move(ds))).finish());

    auto* vids = new VariablePropertyExpression(new std::string(input),
                                                new std::string(kVid));
    qctx_->plan()->saveObject(vids);
    src_ = vids;
    return input;
}

PlanNode* TraversalValidator::buildRuntimeInput() {
    auto* columns = new YieldColumns();
    auto encode = srcRef_->encode();
    auto decode = Expression::decode(encode);
    auto* column = new YieldColumn(decode.release(), new std::string(kVid));
    columns->addColumn(column);
    auto plan = qctx_->plan();
    auto* project = Project::make(plan, nullptr, plan->saveObject(columns));
    if (fromType_ == kVariable) {
        project->setInputVar(userDefinedVarName_);
    }
    project->setColNames({ kVid });
    VLOG(1) << project->varName() << " input: " << project->inputVar();
    src_ = plan->saveObject(new InputPropertyExpression(new std::string(kVid)));

    auto* dedupVids = Dedup::make(plan, project);
    dedupVids->setInputVar(project->varName());
    dedupVids->setColNames(project->colNames());

    projectStartVid_ = project;
    return dedupVids;
}

Expression* TraversalValidator::buildNStepLoopCondition(uint32_t steps) const {
    VLOG(1) << "steps: " << steps;
    // ++loopSteps{0} <= steps
    auto loopSteps = vctx_->anonVarGen()->getVar();
    qctx_->ectx()->setValue(loopSteps, 0);
    auto* condition = new RelationalExpression(
        Expression::Kind::kRelLE,
        new UnaryExpression(
            Expression::Kind::kUnaryIncr,
            new VersionedVariableExpression(new std::string(loopSteps),
                                            new ConstantExpression(0))),
        new ConstantExpression(static_cast<int32_t>(steps)));
    qctx_->plan()->saveObject(condition);
    return condition;
}


}  // namespace graph
}  // namespace nebula
