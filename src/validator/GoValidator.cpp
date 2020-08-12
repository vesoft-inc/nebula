/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GoValidator.h"

#include "util/ExpressionUtils.h"

#include "common/base/Base.h"
#include "common/expression/VariableExpression.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "parser/TraverseSentences.h"
#include "planner/Logic.h"

namespace nebula {
namespace graph {
Status GoValidator::validateImpl() {
    auto* goSentence = static_cast<GoSentence*>(sentence_);
    NG_RETURN_IF_ERROR(validateStep(goSentence->stepClause()));
    NG_RETURN_IF_ERROR(validateFrom(goSentence->fromClause()));
    NG_RETURN_IF_ERROR(validateOver(goSentence->overClause()));
    NG_RETURN_IF_ERROR(validateWhere(goSentence->whereClause()));
    NG_RETURN_IF_ERROR(validateYield(goSentence->yieldClause()));

    if (!exprProps_.inputProps().empty() && fromType_ != kPipe) {
        return Status::Error("$- must be referred in FROM before used in WHERE or YIELD");
    }

    if (!exprProps_.varProps().empty() && fromType_ != kVariable) {
        return Status::Error("A variable must be referred in FROM before used in WHERE or YIELD");
    }

    if ((!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) ||
        exprProps_.varProps().size() > 1) {
        return Status::Error("Only support single input in a go sentence.");
    }

    NG_RETURN_IF_ERROR(buildColumns());

    return Status::OK();
}

Status GoValidator::validateStep(const StepClause* step) {
    if (step == nullptr) {
        return Status::Error("Step clause nullptr.");
    }
    auto steps = step->steps();
    if (steps <= 0) {
        return Status::Error("Only accpet positive number steps.");
    }
    steps_ = steps;
    return Status::OK();
}

Status GoValidator::validateFrom(const FromClause* from) {
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
            auto* symPropExpr = static_cast<SymbolPropertyExpression*>(src);
            if (fromType_ == kVariable) {
                userDefinedVarName_ = *(symPropExpr->sym());
            }
            firstBeginningSrcVidColName_ = *(symPropExpr->prop());
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

Status GoValidator::validateOver(const OverClause* over) {
    if (over == nullptr) {
        return Status::Error("Over clause nullptr.");
    }

    direction_ = over->direction();
    auto* schemaMng = qctx_->schemaMng();
    if (over->isOverAll()) {
        auto allEdgeStatus = schemaMng->getAllEdge(space_.id);
        NG_RETURN_IF_ERROR(allEdgeStatus);
        auto edges = std::move(allEdgeStatus).value();
        if (edges.empty()) {
            return Status::Error("No edge type found in space %s",
                    space_.name.c_str());
        }
        for (auto edge : edges) {
            auto edgeType = schemaMng->toEdgeType(space_.id, edge);
            if (!edgeType.ok()) {
                return Status::Error("%s not found in space [%s].",
                        edge.c_str(), space_.name.c_str());
            }
            VLOG(1) << "et: " << edgeType.value();
            edgeTypes_.emplace_back(edgeType.value());
        }
        allEdges_ = std::move(edges);
        isOverAll_ = true;
    } else {
        auto edges = over->edges();
        for (auto* edge : edges) {
            auto edgeName = *edge->edge();
            auto edgeType = schemaMng->toEdgeType(space_.id, edgeName);
            if (!edgeType.ok()) {
                return Status::Error("%s not found in space [%s].",
                        edgeName.c_str(), space_.name.c_str());
            }
            edgeTypes_.emplace_back(edgeType.value());
        }
    }
    return Status::OK();
}

Status GoValidator::validateWhere(WhereClause* where) {
    if (where == nullptr) {
        return Status::OK();
    }

    filter_ = where->filter();

    if (filter_->kind() == Expression::Kind::kSymProperty) {
        auto symbolExpr = static_cast<SymbolPropertyExpression*>(filter_);
        where->setFilter(ExpressionUtils::transSymbolPropertyExpression<EdgePropertyExpression>(
            symbolExpr));
    } else {
        ExpressionUtils::transAllSymbolPropertyExpr<EdgePropertyExpression>(filter_);
    }

    auto typeStatus = deduceExprType(filter_);
    if (!typeStatus.ok()) {
        return typeStatus.status();
    }

    auto type = typeStatus.value();
    if (type != Value::Type::BOOL && type != Value::Type::NULLVALUE) {
        std::stringstream ss;
        ss << "`" << filter_->toString() << "', Filter only accpet bool/null value, "
           << "but was `" << type << "'";
        return Status::Error(ss.str());
    }

    auto status = deduceProps(filter_, exprProps_);
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}

Status GoValidator::validateYield(YieldClause* yield) {
    if (yield == nullptr) {
        return Status::Error("Yield clause nullptr.");
    }

    distinct_ = yield->isDistinct();
    auto cols = yield->columns();

    if (cols.empty() && isOverAll_) {
        DCHECK(!allEdges_.empty());
        auto* newCols = new YieldColumns();
        qctx_->objPool()->add(newCols);
        for (auto& e : allEdges_) {
            auto* col = new YieldColumn(new EdgeDstIdExpression(new std::string(e)));
            newCols->addColumn(col);
            auto colName = deduceColName(col);
            colNames_.emplace_back(colName);
            outputs_.emplace_back(colName, Value::Type::STRING);
            NG_RETURN_IF_ERROR(deduceProps(col->expr(), exprProps_));
        }

        yields_ = newCols;
    } else {
        for (auto col : cols) {
            if (col->expr()->kind() == Expression::Kind::kSymProperty) {
                auto symbolExpr = static_cast<SymbolPropertyExpression*>(col->expr());
                col->setExpr(ExpressionUtils::transSymbolPropertyExpression<EdgePropertyExpression>(
                    symbolExpr));
            } else {
                ExpressionUtils::transAllSymbolPropertyExpr<EdgePropertyExpression>(col->expr());
            }

            if (!col->getAggFunName().empty()) {
                return Status::Error(
                    "`%s', not support aggregate function in go sentence.",
                    col->toString().c_str());
            }
            auto colName = deduceColName(col);
            colNames_.emplace_back(colName);

            auto typeStatus = deduceExprType(col->expr());
            NG_RETURN_IF_ERROR(typeStatus);
            auto type = typeStatus.value();
            outputs_.emplace_back(colName, type);

            NG_RETURN_IF_ERROR(deduceProps(col->expr(), exprProps_));
        }
        for (auto& e : exprProps_.edgeProps()) {
            auto found = std::find(edgeTypes_.begin(), edgeTypes_.end(), e.first);
            if (found == edgeTypes_.end()) {
                return Status::Error("Edges should be declared first in over clause.");
            }
        }
        yields_ = yield->yields();
    }

    return Status::OK();
}

Status GoValidator::toPlan() {
    if (steps_ > 1) {
        return buildNStepsPlan();
    } else {
        return buildOneStepPlan();
    }
}

Status GoValidator::oneStep(PlanNode* dependencyForGn,
                            const std::string& inputVarNameForGN,
                            PlanNode* projectFromJoin) {
    auto* plan = qctx_->plan();

    auto* gn = GetNeighbors::make(plan, dependencyForGn, space_.id);
    gn->setSrc(src_);
    gn->setVertexProps(buildSrcVertexProps());
    gn->setEdgeProps(buildEdgeProps());
    gn->setInputVar(inputVarNameForGN);
    VLOG(1) << gn->varName();

    PlanNode* dependencyForProjectResult = gn;

    PlanNode* projectSrcEdgeProps = nullptr;
    if (!exprProps_.inputProps().empty() || !exprProps_.varProps().empty() ||
        !exprProps_.dstTagProps().empty()) {
        projectSrcEdgeProps = buildProjectSrcEdgePropsForGN(gn);
    }

    PlanNode* joinDstProps = nullptr;
    if (!exprProps_.dstTagProps().empty() && projectSrcEdgeProps != nullptr) {
        joinDstProps = buildJoinDstProps(projectSrcEdgeProps);
    }
    if (joinDstProps != nullptr) {
        dependencyForProjectResult = joinDstProps;
    }

    PlanNode* joinInput = nullptr;
    if (!exprProps_.inputProps().empty() || !exprProps_.varProps().empty()) {
        joinInput = buildJoinPipeOrVariableInput(
            projectFromJoin,
            joinDstProps == nullptr ? projectSrcEdgeProps : joinDstProps);
    }
    if (joinInput != nullptr) {
        dependencyForProjectResult = joinInput;
    }

    if (filter_ != nullptr) {
        auto* filterNode = Filter::make(plan, dependencyForProjectResult,
                    newFilter_ != nullptr ? newFilter_ : filter_);
        filterNode->setInputVar(dependencyForProjectResult->varName());
        filterNode->setColNames(dependencyForProjectResult->colNames());
        dependencyForProjectResult = filterNode;
    }
    auto* projectResult =
        Project::make(plan, dependencyForProjectResult,
        newYieldCols_ != nullptr ? newYieldCols_ : yields_);
    projectResult->setInputVar(dependencyForProjectResult->varName());
    projectResult->setColNames(std::vector<std::string>(colNames_));
    if (distinct_) {
        Dedup* dedupNode = Dedup::make(plan, projectResult);
        dedupNode->setInputVar(projectResult->varName());
        dedupNode->setColNames(std::move(colNames_));
        root_ = dedupNode;
    } else {
        root_ = projectResult;
    }
    tail_ = gn;
    return Status::OK();
}

Status GoValidator::buildNStepsPlan() {
    auto* plan = qctx_->plan();

    auto* bodyStart = StartNode::make(plan);

    std::string startVidsVar;
    PlanNode* projectStartVid = nullptr;
    if (!starts_.empty() && srcRef_ == nullptr) {
        startVidsVar = buildConstantInput();
    } else {
        projectStartVid = buildRuntimeInput();
        startVidsVar = projectStartVid->varName();
    }

    Project* projectLeftVarForJoin = nullptr;
    if (!exprProps_.inputProps().empty() || !exprProps_.varProps().empty()) {
        projectLeftVarForJoin = buildLeftVarForTraceJoin(projectStartVid);
    }

    auto* gn = GetNeighbors::make(plan, bodyStart, space_.id);
    gn->setSrc(src_);
    gn->setEdgeProps(buildEdgeDst());
    gn->setInputVar(startVidsVar);
    VLOG(1) << gn->varName();

    Project* projectDstFromGN = projectDstVidsFromGN(gn, startVidsVar);

    Project* projectFromJoin = nullptr;
    if ((!exprProps_.inputProps().empty() || !exprProps_.varProps().empty()) &&
        projectLeftVarForJoin != nullptr && projectDstFromGN != nullptr) {
        projectFromJoin = traceToStartVid(projectLeftVarForJoin, projectDstFromGN);
    }

    auto* loop = Loop::make(
        plan,
        projectLeftVarForJoin == nullptr ? projectStartVid
                                         : projectLeftVarForJoin,
        projectFromJoin == nullptr ? projectDstFromGN : projectFromJoin,
        buildNStepLoopCondition(steps_ - 1));
    VLOG(1) << "loop dep: " << projectLeftVarForJoin;

    auto status = oneStep(loop, projectDstFromGN->varName(),
            projectFromJoin == nullptr ? projectDstFromGN : projectFromJoin);
    if (!status.ok()) {
        return status;
    }
    // reset tail_
    if (projectStartVid != nullptr) {
        tail_ = projectStartVid;
    } else if (projectLeftVarForJoin != nullptr) {
        tail_ = projectLeftVarForJoin;
    } else {
        tail_ = loop;
    }
    VLOG(1) << "root: " << root_->kind() << " tail: " << tail_->kind();
    return Status::OK();
}

PlanNode* GoValidator::buildProjectSrcEdgePropsForGN(PlanNode* gn) {
    DCHECK(gn != nullptr);

    auto* plan = qctx_->plan();
    if (!exprProps_.inputProps().empty() || !exprProps_.varProps().empty()) {
        auto* srcVidCol = new YieldColumn(
            new VariablePropertyExpression(new std::string(gn->varName()),
                                        new std::string(kVid)),
            new std::string(kVid));
        srcAndEdgePropCols_->addColumn(srcVidCol);
    }

    VLOG(1) << "build dst cols";
    if (!exprProps_.dstTagProps().empty()) {
        joinDstVidColName_ = vctx_->anonColGen()->getCol();
        auto* dstVidCol = new YieldColumn(
            new EdgePropertyExpression(new std::string("*"),
                                        new std::string(kDst)),
            new std::string(joinDstVidColName_));
        srcAndEdgePropCols_->addColumn(dstVidCol);
    }

    auto* project = Project::make(plan, gn, srcAndEdgePropCols_);
    project->setInputVar(gn->varName());
    project->setColNames(deduceColNames(srcAndEdgePropCols_));
    VLOG(1) << project->varName();

    return project;
}

PlanNode* GoValidator::buildJoinDstProps(PlanNode* projectSrcDstProps) {
    DCHECK(dstPropCols_ != nullptr);
    DCHECK(projectSrcDstProps != nullptr);

    auto* plan = qctx_->plan();

    auto* yieldDsts = plan->makeAndSave<YieldColumns>();
    yieldDsts->addColumn(new YieldColumn(
        new InputPropertyExpression(new std::string(joinDstVidColName_)),
        new std::string(joinDstVidColName_)));
    auto* projectDsts = Project::make(plan, projectSrcDstProps, yieldDsts);
    projectDsts->setInputVar(projectSrcDstProps->varName());
    projectDsts->setColNames(std::vector<std::string>{joinDstVidColName_});

    auto* dedupVids = Dedup::make(plan, projectDsts);
    dedupVids->setInputVar(projectDsts->varName());

    auto* vids = new VariablePropertyExpression(
        new std::string(dedupVids->varName()),
        new std::string(joinDstVidColName_));
    plan->saveObject(vids);
    auto* getDstVertices =
        GetVertices::make(plan, dedupVids, space_.id, vids,
                            buildDstVertexProps(), {});
    getDstVertices->setInputVar(dedupVids->varName());

    auto vidColName = vctx_->anonColGen()->getCol();
    auto* vidCol = new YieldColumn(
        new VariablePropertyExpression(new std::string(getDstVertices->varName()),
                                    new std::string(kVid)),
        new std::string(vidColName));
    dstPropCols_->addColumn(vidCol);
    auto* project = Project::make(plan, getDstVertices, dstPropCols_);
    project->setInputVar(getDstVertices->varName());
    project->setColNames(deduceColNames(dstPropCols_));

    auto* joinHashKey = new VariablePropertyExpression(
        new std::string(projectSrcDstProps->varName()),
        new std::string(joinDstVidColName_));
    plan->saveObject(joinHashKey);
    auto* probeKey = new VariablePropertyExpression(
        new std::string(project->varName()), new std::string(vidColName));
    plan->saveObject(probeKey);
    auto joinDst = DataJoin::make(plan, project,
            {projectSrcDstProps->varName(), ExecutionContext::kLatestVersion},
            {project->varName(), ExecutionContext::kLatestVersion},
            {joinHashKey}, {probeKey});
    VLOG(1) << joinDst->varName() << " hash key: " << joinHashKey->toString()
        << " probe key: " << probeKey->toString();

    return joinDst;
}

PlanNode* GoValidator::buildJoinPipeOrVariableInput(
    PlanNode* projectFromJoin, PlanNode* dependencyForJoinInput) {
    auto* plan = qctx_->plan();

    if (steps_ > 1) {
        DCHECK(projectFromJoin != nullptr);
        auto* joinHashKey = new VariablePropertyExpression(
                new std::string(dependencyForJoinInput->varName()), new std::string(kVid));
        plan->saveObject(joinHashKey);
        auto* probeKey = new VariablePropertyExpression(
                new std::string(projectFromJoin->varName()), new std::string(dstVidColName_));
        plan->saveObject(probeKey);
        auto* join = DataJoin::make(
            plan, dependencyForJoinInput,
            {dependencyForJoinInput->varName(), ExecutionContext::kLatestVersion},
            {projectFromJoin->varName(), ExecutionContext::kLatestVersion},
            {joinHashKey}, {probeKey});
        std::vector<std::string> colNames = dependencyForJoinInput->colNames();
        for (auto& col : projectFromJoin->colNames()) {
            colNames.emplace_back(col);
        }
        join->setColNames(std::move(colNames));
        VLOG(1) << join->varName();
        dependencyForJoinInput = join;
    }

    DCHECK(dependencyForJoinInput != nullptr);
    auto* joinHashKey = new VariablePropertyExpression(
        new std::string(dependencyForJoinInput->varName()),
        new std::string(steps_ > 1 ? firstBeginningSrcVidColName_ : kVid));
    plan->saveObject(joinHashKey);
    auto* joinInput =
        DataJoin::make(plan, dependencyForJoinInput,
                        {dependencyForJoinInput->varName(),
                        ExecutionContext::kLatestVersion},
                        {fromType_ == kPipe ? inputVarName_ : userDefinedVarName_,
                        ExecutionContext::kLatestVersion},
                        {joinHashKey}, {steps_ > 1 ? srcRef_ : src_});
    std::vector<std::string> colNames = dependencyForJoinInput->colNames();
    for (auto& col : outputs_) {
        colNames.emplace_back(col.first);
    }
    joinInput->setColNames(std::move(colNames));
    VLOG(1) << joinInput->varName();

    return joinInput;
}

Project* GoValidator::traceToStartVid(Project* projectLeftVarForJoin,
                                      Project* projectDstFromGN) {
    DCHECK(projectLeftVarForJoin != nullptr);
    DCHECK(projectDstFromGN != nullptr);

    auto* plan = qctx_->plan();
    auto hashKey = new VariablePropertyExpression(
            new std::string(projectLeftVarForJoin->varName()),
            new std::string(dstVidColName_));
    plan->saveObject(hashKey);
    auto probeKey = new VariablePropertyExpression(
        new std::string(projectDstFromGN->varName()), new std::string(srcVidColName_));
    plan->saveObject(probeKey);
    auto* join = DataJoin::make(
        plan, projectDstFromGN,
        {projectLeftVarForJoin->varName(),
            ExecutionContext::kLatestVersion},
        {projectDstFromGN->varName(), ExecutionContext::kLatestVersion},
        {hashKey}, {probeKey});
    std::vector<std::string> colNames = projectLeftVarForJoin->colNames();
    for (auto& col : projectDstFromGN->colNames()) {
        colNames.emplace_back(col);
    }
    join->setColNames(std::move(colNames));
    VLOG(1) << join->varName();

    auto* columns = new YieldColumns();
    auto* column =
        new YieldColumn(new InputPropertyExpression(
                            new std::string(firstBeginningSrcVidColName_)),
                        new std::string(firstBeginningSrcVidColName_));
    columns->addColumn(column);
    column =
        new YieldColumn(new InputPropertyExpression(new std::string(kVid)),
                        new std::string(dstVidColName_));
    columns->addColumn(column);
    auto* projectJoin = Project::make(plan, join, plan->saveObject(columns));
    projectJoin->setInputVar(join->varName());
    projectJoin->setColNames(deduceColNames(columns));
    VLOG(1) << projectJoin->varName();

    return projectJoin;
}

Project* GoValidator::projectDstVidsFromGN(PlanNode* gn, const std::string& outputVar) {
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
    project->setOutputVar(outputVar);
    project->setColNames(deduceColNames(columns));
    VLOG(1) << project->varName();

    return project;
}

Project* GoValidator::buildLeftVarForTraceJoin(PlanNode* projectStartVid) {
    auto* plan = qctx_->plan();
    dstVidColName_ = vctx_->anonColGen()->getCol();
    auto* columns = new YieldColumns();
    auto* column =
        new YieldColumn(Expression::decode(srcRef_->encode()).release(),
                        new std::string(firstBeginningSrcVidColName_));
    columns->addColumn(column);
    column =
        new YieldColumn(Expression::decode(srcRef_->encode()).release(),
                        new std::string(dstVidColName_));
    columns->addColumn(column);
    plan->saveObject(columns);
    // projectStartVid could be nullptr, that means no input for this project.
    auto* projectLeftVarForJoin = Project::make(plan, projectStartVid, columns);
    projectLeftVarForJoin->setInputVar(
        fromType_ == kPipe ? inputVarName_ : userDefinedVarName_);
    projectLeftVarForJoin->setColNames(deduceColNames(columns));

    return projectLeftVarForJoin;
}

Status GoValidator::buildOneStepPlan() {
    std::string inputVarNameForGN;
    if (!starts_.empty() && srcRef_ == nullptr) {
        inputVarNameForGN = buildConstantInput();
    } else {
        src_ = srcRef_;
        if (fromType_ == kVariable) {
            inputVarNameForGN = userDefinedVarName_;
        } else {
            inputVarNameForGN = inputVarName_;
        }
    }

    auto status = oneStep(nullptr, inputVarNameForGN, nullptr);
    if (!status.ok()) {
        return status;
    }

    VLOG(1) << "root: " << root_->kind() << " tail: " << tail_->kind();
    return Status::OK();
}

std::string GoValidator::buildConstantInput() {
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

PlanNode* GoValidator::buildRuntimeInput() {
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
    return project;
}

GetNeighbors::VertexProps GoValidator::buildSrcVertexProps() {
    GetNeighbors::VertexProps vertexProps;
    if (!exprProps_.srcTagProps().empty()) {
        vertexProps = std::make_unique<std::vector<storage::cpp2::VertexProp>>(
            exprProps_.srcTagProps().size());
        std::transform(exprProps_.srcTagProps().begin(), exprProps_.srcTagProps().end(),
                       vertexProps->begin(), [](auto& tag) {
                           storage::cpp2::VertexProp vp;
                           vp.tag = tag.first;
                           std::vector<std::string>props(tag.second.begin(), tag.second.end());
                           vp.props = std::move(props);
                           return vp;
                       });
    }
    return vertexProps;
}

std::vector<storage::cpp2::VertexProp> GoValidator::buildDstVertexProps() {
    std::vector<storage::cpp2::VertexProp> vertexProps(exprProps_.dstTagProps().size());
    if (!exprProps_.dstTagProps().empty()) {
        std::transform(exprProps_.dstTagProps().begin(), exprProps_.dstTagProps().end(),
                       vertexProps.begin(), [](auto& tag) {
                           storage::cpp2::VertexProp vp;
                           vp.tag = tag.first;
                           std::vector<std::string>props(tag.second.begin(), tag.second.end());
                           vp.props = std::move(props);
                           return vp;
                       });
    }
    return vertexProps;
}

GetNeighbors::EdgeProps GoValidator::buildEdgeProps() {
    GetNeighbors::EdgeProps edgeProps;
    if (!exprProps_.edgeProps().empty()) {
        if (direction_ == storage::cpp2::EdgeDirection::IN_EDGE) {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
            buildEdgeProps(edgeProps, true);
        } else if (direction_ == storage::cpp2::EdgeDirection::BOTH) {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
            buildEdgeProps(edgeProps, false);
            buildEdgeProps(edgeProps, true);
        } else {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
            buildEdgeProps(edgeProps, false);
        }
    } else if (!exprProps_.dstTagProps().empty()) {
        return buildEdgeDst();
    }

    return edgeProps;
}

void GoValidator::buildEdgeProps(GetNeighbors::EdgeProps& edgeProps, bool isInEdge) {
    edgeProps->reserve(edgeTypes_.size());
    for (auto& e : edgeTypes_) {
        storage::cpp2::EdgeProp ep;
        if (isInEdge) {
            ep.set_type(-e);
        } else {
            ep.set_type(e);
        }

        const auto& propsFound = exprProps_.edgeProps().find(e);
        if (propsFound == exprProps_.edgeProps().end()) {
            ep.props = {kDst};
        } else {
            std::vector<std::string> props(propsFound->second.begin(),
                                           propsFound->second.end());
            if (propsFound->second.find(kDst) == propsFound->second.end()) {
                props.emplace_back(kDst);
            }
            ep.set_props(std::move(props));
        }
        edgeProps->emplace_back(std::move(ep));
    }
}

GetNeighbors::EdgeProps GoValidator::buildEdgeDst() {
    GetNeighbors::EdgeProps edgeProps;
    if (!exprProps_.edgeProps().empty() || !exprProps_.dstTagProps().empty()) {
        if (direction_ == storage::cpp2::EdgeDirection::IN_EDGE) {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(
                edgeTypes_.size());
            std::transform(edgeTypes_.begin(), edgeTypes_.end(), edgeProps->begin(),
                        [](auto& type) {
                            storage::cpp2::EdgeProp ep;
                            ep.type = -type;
                            VLOG(1) << "edge type: " << ep.type;
                            ep.props = {kDst};
                            return ep;
                        });
        } else if (direction_ == storage::cpp2::EdgeDirection::BOTH) {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(
                edgeTypes_.size() * 2);
            std::transform(edgeTypes_.begin(), edgeTypes_.end(), edgeProps->begin(),
                        [](auto& type) {
                            storage::cpp2::EdgeProp ep;
                            ep.type = type;
                            VLOG(1) << "edge type: " << ep.type;
                            ep.props = {kDst};
                            return ep;
                        });
            std::transform(edgeTypes_.begin(), edgeTypes_.end(),
                           edgeProps->begin() + edgeTypes_.size(),
                           [](auto& type) {
                               storage::cpp2::EdgeProp ep;
                               ep.type = -type;
                               VLOG(1) << "edge type: " << ep.type;
                               ep.props = {kDst};
                               return ep;
                           });
        } else {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(
                edgeTypes_.size());
            std::transform(edgeTypes_.begin(), edgeTypes_.end(), edgeProps->begin(),
                        [](auto& type) {
                            storage::cpp2::EdgeProp ep;
                            ep.type = type;
                            VLOG(1) << "edge type: " << ep.type;
                            ep.props = {kDst};
                            return ep;
                        });
        }
    }
    return edgeProps;
}

Expression* GoValidator::buildNStepLoopCondition(int64_t steps) const {
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

void GoValidator::extractPropExprs(const Expression* expr) {
    switch (expr->kind()) {
        case Expression::Kind::kConstant: {
            break;
        }
        case Expression::Kind::kAdd:
        case Expression::Kind::kMinus:
        case Expression::Kind::kMultiply:
        case Expression::Kind::kDivision:
        case Expression::Kind::kMod:
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelNE:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kContains:
        case Expression::Kind::kLogicalAnd:
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor: {
            auto biExpr = static_cast<const BinaryExpression*>(expr);
            extractPropExprs(biExpr->left());
            extractPropExprs(biExpr->right());
            break;
        }
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot: {
            auto unaryExpr = static_cast<const UnaryExpression*>(expr);
            extractPropExprs(unaryExpr->operand());
            break;
        }
        case Expression::Kind::kTypeCasting: {
            auto typeCastingExpr = static_cast<const TypeCastingExpression*>(expr);
            extractPropExprs(typeCastingExpr->operand());
            break;
        }
        case Expression::Kind::kFunctionCall: {
            auto funcExpr = static_cast<const FunctionCallExpression*>(expr);
            auto& args = funcExpr->args()->args();
            for (auto iter = args.begin(); iter < args.end(); ++iter) {
                extractPropExprs(iter->get());
            }
            break;
        }
        case Expression::Kind::kDstProperty: {
            auto found = propExprColMap_.find(expr->toString());
            if (found == propExprColMap_.end()) {
                auto encode = expr->encode();
                auto newExpr = Expression::decode(encode);
                auto col = new YieldColumn(
                    newExpr.release(), new std::string(vctx_->anonColGen()->getCol()));
                propExprColMap_.emplace(expr->toString(), col);
                dstPropCols_->addColumn(col);
            }
            break;
        }
        case Expression::Kind::kTagProperty:
        case Expression::Kind::kSrcProperty:
        case Expression::Kind::kEdgeProperty:
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType:
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst: {
            auto found = propExprColMap_.find(expr->toString());
            if (found == propExprColMap_.end()) {
                auto encode = expr->encode();
                auto newExpr = Expression::decode(encode);
                auto col = new YieldColumn(
                    newExpr.release(), new std::string(vctx_->anonColGen()->getCol()));
                propExprColMap_.emplace(expr->toString(), col);
                srcAndEdgePropCols_->addColumn(col);
            }
            break;
        }
        case Expression::Kind::kInputProperty:
        case Expression::Kind::kVarProperty: {
            auto* symPropExpr = static_cast<const SymbolPropertyExpression*>(expr);
            auto found = propExprColMap_.find(expr->toString());
            if (found == propExprColMap_.end()) {
                auto encode = expr->encode();
                auto newExpr = Expression::decode(encode);
                auto col = new YieldColumn(
                    newExpr.release(), new std::string(*symPropExpr->prop()));
                propExprColMap_.emplace(expr->toString(), col);
                inputPropCols_->addColumn(col);
            }
            break;
        }
        case Expression::Kind::kUUID:
        case Expression::Kind::kVar:
        case Expression::Kind::kVersionedVar:
        case Expression::Kind::kSymProperty:
        case Expression::Kind::kUnaryIncr:
        case Expression::Kind::kUnaryDecr:
        case Expression::Kind::kList:   // FIXME(dutor)
        case Expression::Kind::kSet:
        case Expression::Kind::kMap:
        case Expression::Kind::kSubscript:
        case Expression::Kind::kLabel:
        case Expression::Kind::kRelIn:
        case Expression::Kind::kRelNotIn: {
            LOG(FATAL) << "Not support " << expr->kind();
            break;
        }
    }
}

std::unique_ptr<Expression> GoValidator::rewriteToInputProp(Expression* expr) {
    switch (expr->kind()) {
        case Expression::Kind::kConstant: {
            break;
        }
        case Expression::Kind::kAdd:
        case Expression::Kind::kMinus:
        case Expression::Kind::kMultiply:
        case Expression::Kind::kDivision:
        case Expression::Kind::kMod:
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelNE:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kContains:
        case Expression::Kind::kLogicalAnd:
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor: {
            auto biExpr = static_cast<BinaryExpression*>(expr);
            auto left = rewriteToInputProp(const_cast<Expression*>(biExpr->left()));
            if (left != nullptr) {
                biExpr->setLeft(left.release());
            }
            auto right = rewriteToInputProp(const_cast<Expression*>(biExpr->right()));
            if (right != nullptr) {
                biExpr->setRight(right.release());
            }
            break;
        }
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot: {
            auto unaryExpr = static_cast<UnaryExpression*>(expr);
            auto rewrite = rewriteToInputProp(const_cast<Expression*>(unaryExpr->operand()));
            if (rewrite != nullptr) {
                unaryExpr->setOperand(rewrite.release());
            }
            break;
        }
        case Expression::Kind::kTypeCasting: {
            auto typeCastingExpr =
                static_cast<TypeCastingExpression*>(expr);
            auto rewrite = rewriteToInputProp(
                const_cast<Expression*>(typeCastingExpr->operand()));
            if (rewrite != nullptr) {
                typeCastingExpr->setOperand(rewrite.release());
            }
            break;
        }
        case Expression::Kind::kFunctionCall: {
            auto funcExpr = static_cast<FunctionCallExpression*>(expr);
            auto* argList = const_cast<ArgumentList*>(funcExpr->args());
            auto args = argList->moveArgs();
            for (auto iter = args.begin(); iter < args.end(); ++iter) {
                auto rewrite = rewriteToInputProp(iter->get());
                if (rewrite != nullptr) {
                    *iter = std::move(rewrite);
                }
            }
            argList->setArgs(std::move(args));
            break;
        }
        case Expression::Kind::kTagProperty:
        case Expression::Kind::kSrcProperty:
        case Expression::Kind::kDstProperty:
        case Expression::Kind::kEdgeProperty:
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType:
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst:
        case Expression::Kind::kVarProperty: {
            auto found = propExprColMap_.find(expr->toString());
            DCHECK(found != propExprColMap_.end());
            auto alias = new std::string(*(found->second->alias()));
            return std::make_unique<InputPropertyExpression>(alias);
        }
        case Expression::Kind::kInputProperty: {
            break;
        }
        case Expression::Kind::kUUID:
        case Expression::Kind::kVar:
        case Expression::Kind::kVersionedVar:
        case Expression::Kind::kSymProperty:
        case Expression::Kind::kUnaryIncr:
        case Expression::Kind::kUnaryDecr:
        case Expression::Kind::kList:   // FIXME(dutor)
        case Expression::Kind::kSet:
        case Expression::Kind::kMap:
        case Expression::Kind::kSubscript:
        case Expression::Kind::kLabel:
        case Expression::Kind::kRelIn:
        case Expression::Kind::kRelNotIn: {
            LOG(FATAL) << "Not support " << expr->kind();
            break;
        }
    }
    return nullptr;
}

Status GoValidator::buildColumns() {
  if (exprProps_.dstTagProps().empty() && exprProps_.inputProps().empty() &&
      exprProps_.varProps().empty()) {
      return Status::OK();
  }

  if (!exprProps_.srcTagProps().empty() || !exprProps_.edgeProps().empty() ||
      !exprProps_.dstTagProps().empty()) {
      srcAndEdgePropCols_ = qctx_->plan()->saveObject(new YieldColumns());
  }

    if (!exprProps_.dstTagProps().empty()) {
        dstPropCols_ = qctx_->plan()->saveObject(new YieldColumns());
    }

    if (!exprProps_.inputProps().empty() || !exprProps_.varProps().empty()) {
        inputPropCols_ = qctx_->plan()->saveObject(new YieldColumns());
    }

    if (filter_ != nullptr) {
        extractPropExprs(filter_);
        auto newFilter = Expression::decode(filter_->encode());
        auto rewriteFilter = rewriteToInputProp(newFilter.get());
        if (rewriteFilter != nullptr) {
            newFilter_ = rewriteFilter.release();
        } else {
            newFilter_ = newFilter.release();
        }
        qctx_->plan()->saveObject(newFilter_);
    }

    newYieldCols_ = qctx_->plan()->saveObject(new YieldColumns());
    for (auto* yield : yields_->columns()) {
        extractPropExprs(yield->expr());
        auto newCol = Expression::decode(yield->expr()->encode());
        auto rewriteCol = rewriteToInputProp(newCol.get());
        auto alias = yield->alias() == nullptr
                         ? nullptr
                         : new std::string(*(yield->alias()));
        if (rewriteCol != nullptr) {
            newYieldCols_->addColumn(
                new YieldColumn(rewriteCol.release(), alias));
        } else {
            newYieldCols_->addColumn(new YieldColumn(newCol.release(), alias));
        }
    }

    return Status::OK();
}

}  // namespace graph
}  // namespace nebula
