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
#include "visitor/ExtractPropExprVisitor.h"
#include "visitor/RewriteInputPropVisitor.h"
#include "parser/TraverseSentences.h"
#include "planner/Logic.h"

namespace nebula {
namespace graph {
Status GoValidator::validateImpl() {
    auto* goSentence = static_cast<GoSentence*>(sentence_);
    NG_RETURN_IF_ERROR(validateStep(goSentence->stepClause(), steps_));
    NG_RETURN_IF_ERROR(validateStarts(goSentence->fromClause(), from_));
    NG_RETURN_IF_ERROR(validateOver(goSentence->overClause(), over_));
    NG_RETURN_IF_ERROR(validateWhere(goSentence->whereClause()));
    NG_RETURN_IF_ERROR(validateYield(goSentence->yieldClause()));

    if (!exprProps_.inputProps().empty() && from_.fromType != kPipe) {
        return Status::Error("$- must be referred in FROM before used in WHERE or YIELD");
    }

    if (!exprProps_.varProps().empty() && from_.fromType != kVariable) {
        return Status::Error("A variable must be referred in FROM before used in WHERE or YIELD");
    }

    if ((!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) ||
        exprProps_.varProps().size() > 1) {
        return Status::Error("Only support single input in a go sentence.");
    }

    NG_RETURN_IF_ERROR(buildColumns());

    return Status::OK();
}

Status GoValidator::validateWhere(WhereClause* where) {
    if (where == nullptr) {
        return Status::OK();
    }

    filter_ = where->filter();

    if (filter_->kind() == Expression::Kind::kLabelAttribute) {
        auto laExpr = static_cast<LabelAttributeExpression*>(filter_);
        where->setFilter(ExpressionUtils::rewriteLabelAttribute<EdgePropertyExpression>(
            laExpr));
    } else {
        ExpressionUtils::rewriteLabelAttribute<EdgePropertyExpression>(filter_);
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

    if (cols.empty() && over_.isOverAll) {
        DCHECK(!over_.allEdges.empty());
        auto* newCols = new YieldColumns();
        qctx_->objPool()->add(newCols);
        for (auto& e : over_.allEdges) {
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
            if (col->expr()->kind() == Expression::Kind::kLabelAttribute) {
                auto laExpr = static_cast<LabelAttributeExpression*>(col->expr());
                col->setExpr(ExpressionUtils::rewriteLabelAttribute<EdgePropertyExpression>(
                    laExpr));
            } else {
                ExpressionUtils::rewriteLabelAttribute<EdgePropertyExpression>(col->expr());
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
            auto found = std::find(over_.edgeTypes.begin(), over_.edgeTypes.end(), e.first);
            if (found == over_.edgeTypes.end()) {
                return Status::Error("Edges should be declared first in over clause.");
            }
        }
        yields_ = yield->yields();
    }

    return Status::OK();
}

Status GoValidator::toPlan() {
    if (steps_.mToN == nullptr) {
        if (steps_.steps == 0) {
            auto* passThrough = PassThroughNode::make(qctx_, nullptr);
            passThrough->setColNames(std::move(colNames_));
            tail_ = passThrough;
            root_ = tail_;
            return Status::OK();
        } else if (steps_.steps == 1) {
            return buildOneStepPlan();
        } else {
            return buildNStepsPlan();
        }
    } else {
        return buildMToNPlan();
    }
}

Status GoValidator::oneStep(PlanNode* dependencyForGn,
                            const std::string& inputVarNameForGN,
                            PlanNode* projectFromJoin) {
    auto* gn = GetNeighbors::make(qctx_, dependencyForGn, space_.id);
    gn->setSrc(src_);
    gn->setVertexProps(buildSrcVertexProps());
    gn->setEdgeProps(buildEdgeProps());
    gn->setInputVar(inputVarNameForGN);
    VLOG(1) << gn->outputVar();

    PlanNode* dependencyForProjectResult = gn;

    PlanNode* projectSrcEdgeProps = nullptr;
    if (!exprProps_.inputProps().empty() || !exprProps_.varProps().empty() ||
        !exprProps_.dstTagProps().empty() || from_.fromType != FromType::kInstantExpr) {
        projectSrcEdgeProps = buildProjectSrcEdgePropsForGN(gn->outputVar(), gn);
    }

    // Join the dst props if $$.tag.prop was declared.
    PlanNode* joinDstProps = nullptr;
    if (!exprProps_.dstTagProps().empty() && projectSrcEdgeProps != nullptr) {
        joinDstProps = buildJoinDstProps(projectSrcEdgeProps);
    }
    if (joinDstProps != nullptr) {
        dependencyForProjectResult = joinDstProps;
    }

    PlanNode* joinInput = nullptr;
    if (from_.fromType != FromType::kInstantExpr) {
        joinInput = buildJoinPipeOrVariableInput(
            projectFromJoin, joinDstProps == nullptr ? projectSrcEdgeProps : joinDstProps);
    }
    if (joinInput != nullptr) {
        dependencyForProjectResult = joinInput;
    }

    if (filter_ != nullptr) {
        auto* filterNode = Filter::make(qctx_, dependencyForProjectResult,
                    newFilter_ != nullptr ? newFilter_ : filter_);
        filterNode->setInputVar(dependencyForProjectResult->outputVar());
        filterNode->setColNames(dependencyForProjectResult->colNames());
        dependencyForProjectResult = filterNode;
    }
    auto* projectResult =
        Project::make(qctx_, dependencyForProjectResult,
        newYieldCols_ != nullptr ? newYieldCols_ : yields_);
    projectResult->setInputVar(dependencyForProjectResult->outputVar());
    projectResult->setColNames(std::vector<std::string>(colNames_));
    if (distinct_) {
        Dedup* dedupNode = Dedup::make(qctx_, projectResult);
        dedupNode->setInputVar(projectResult->outputVar());
        dedupNode->setColNames(std::move(colNames_));
        root_ = dedupNode;
    } else {
        root_ = projectResult;
    }
    tail_ = gn;
    return Status::OK();
}

Status GoValidator::buildNStepsPlan() {
    auto* bodyStart = StartNode::make(qctx_);

    std::string startVidsVar;
    PlanNode* dedupStartVid = nullptr;
    if (!from_.vids.empty() && from_.srcRef == nullptr) {
        buildConstantInput(from_, startVidsVar, src_);
    } else {
        dedupStartVid = buildRuntimeInput();
        startVidsVar = dedupStartVid->outputVar();
    }

    PlanNode* projectLeftVarForJoin = nullptr;
    if (from_.fromType != FromType::kInstantExpr) {
        projectLeftVarForJoin = buildLeftVarForTraceJoin(dedupStartVid);
    }

    auto* gn = GetNeighbors::make(qctx_, bodyStart, space_.id);
    gn->setSrc(src_);
    gn->setEdgeProps(buildEdgeDst());
    gn->setInputVar(startVidsVar);
    VLOG(1) << gn->outputVar();

    PlanNode* dedupDstVids = projectDstVidsFromGN(gn, startVidsVar);

    // Trace to the start vid if starts from a runtime start vid.
    PlanNode* projectFromJoin = nullptr;
    if (from_.fromType != FromType::kInstantExpr  &&
        projectLeftVarForJoin != nullptr && dedupDstVids != nullptr) {
        projectFromJoin = traceToStartVid(projectLeftVarForJoin, dedupDstVids);
    }

    auto* loop = Loop::make(
        qctx_,
        projectLeftVarForJoin == nullptr ? dedupStartVid
                                         : projectLeftVarForJoin,  // dep
        projectFromJoin == nullptr ? dedupDstVids : projectFromJoin,  // body
        buildNStepLoopCondition(steps_.steps - 1));

    auto status = oneStep(loop, dedupDstVids->outputVar(), projectFromJoin);
    if (!status.ok()) {
        return status;
    }
    // reset tail_
    if (projectStartVid_ != nullptr) {
        tail_ = projectStartVid_;
    } else if (projectLeftVarForJoin != nullptr) {
        tail_ = projectLeftVarForJoin;
    } else {
        tail_ = loop;
    }
    VLOG(1) << "root: " << root_->kind() << " tail: " << tail_->kind();
    return Status::OK();
}

Status GoValidator::buildMToNPlan() {
    auto* bodyStart = StartNode::make(qctx_);

    std::string startVidsVar;
    PlanNode* dedupStartVid = nullptr;
    if (!from_.vids.empty() && from_.srcRef == nullptr) {
        buildConstantInput(from_, startVidsVar, src_);
    } else {
        dedupStartVid = buildRuntimeInput();
        startVidsVar = dedupStartVid->outputVar();
    }

    PlanNode* projectLeftVarForJoin = nullptr;
    if (!exprProps_.inputProps().empty() || !exprProps_.varProps().empty()) {
        projectLeftVarForJoin = buildLeftVarForTraceJoin(dedupStartVid);
    }

    auto* gn = GetNeighbors::make(qctx_, bodyStart, space_.id);
    gn->setSrc(src_);
    gn->setVertexProps(buildSrcVertexProps());
    gn->setEdgeProps(buildEdgeProps());
    gn->setInputVar(startVidsVar);
    VLOG(1) << gn->outputVar();

    PlanNode* dedupDstVids = projectDstVidsFromGN(gn, startVidsVar);

    PlanNode* dependencyForProjectResult = dedupDstVids;

    // Trace to the start vid if $-.prop was declared.
    PlanNode* projectFromJoin = nullptr;
    if (!exprProps_.inputProps().empty() || !exprProps_.varProps().empty()) {
        if ((!exprProps_.inputProps().empty() || !exprProps_.varProps().empty()) &&
            projectLeftVarForJoin != nullptr && dedupDstVids != nullptr) {
            projectFromJoin = traceToStartVid(projectLeftVarForJoin, dedupDstVids);
        }
    }

    // Get the src props and edge props if $-.prop, $var.prop, $$.tag.prop were declared.
    PlanNode* projectSrcEdgeProps = nullptr;
    if (!exprProps_.inputProps().empty() || !exprProps_.varProps().empty() ||
        !exprProps_.dstTagProps().empty()) {
        PlanNode* depForProject = dedupDstVids;
        if (projectFromJoin != nullptr) {
            depForProject = projectFromJoin;
        }
        projectSrcEdgeProps = buildProjectSrcEdgePropsForGN(gn->outputVar(), depForProject);
    }

    // Join the dst props if $$.tag.prop was declared.
    PlanNode* joinDstProps = nullptr;
    if (!exprProps_.dstTagProps().empty() && projectSrcEdgeProps != nullptr) {
        joinDstProps = buildJoinDstProps(projectSrcEdgeProps);
    }
    if (joinDstProps != nullptr) {
        dependencyForProjectResult = joinDstProps;
    }

    // Join input props if $-.prop declared.
    PlanNode* joinInput = nullptr;
    if (!exprProps_.inputProps().empty() || !exprProps_.varProps().empty()) {
        joinInput = buildJoinPipeOrVariableInput(
            projectFromJoin, joinDstProps == nullptr ? projectSrcEdgeProps : joinDstProps);
    }
    if (joinInput != nullptr) {
        dependencyForProjectResult = joinInput;
    }

    if (filter_ != nullptr) {
        auto* filterNode = Filter::make(qctx_, dependencyForProjectResult,
                    newFilter_ != nullptr ? newFilter_ : filter_);
        filterNode->setInputVar(
            dependencyForProjectResult == dedupDstVids ?
                gn->outputVar() : dependencyForProjectResult->outputVar());
        filterNode->setColNames(dependencyForProjectResult->colNames());
        dependencyForProjectResult = filterNode;
    }

    SingleInputNode* projectResult =
        Project::make(qctx_, dependencyForProjectResult,
        newYieldCols_ != nullptr ? newYieldCols_ : yields_);
    projectResult->setInputVar(
            dependencyForProjectResult == dedupDstVids ?
                gn->outputVar() : dependencyForProjectResult->outputVar());
    projectResult->setColNames(std::vector<std::string>(colNames_));

    SingleInputNode* dedupNode = nullptr;
    if (distinct_) {
        dedupNode = Dedup::make(qctx_, projectResult);
        dedupNode->setInputVar(projectResult->outputVar());
        dedupNode->setColNames(std::move(colNames_));
    }

    auto* loop = Loop::make(
        qctx_,
        projectLeftVarForJoin == nullptr ? dedupStartVid
                                         : projectLeftVarForJoin,  // dep
        dedupNode == nullptr ? projectResult : dedupNode,  // body
        buildNStepLoopCondition(steps_.mToN->nSteps));

    if (projectStartVid_ != nullptr) {
        tail_ = projectStartVid_;
    } else {
        tail_ = loop;
    }

    std::vector<std::string> collectVars;
    if (dedupNode == nullptr) {
        collectVars = {projectResult->outputVar()};
    } else {
        collectVars = {dedupNode->outputVar()};
    }
    auto* dataCollect =
        DataCollect::make(qctx_, loop, DataCollect::CollectKind::kMToN, collectVars);
    dataCollect->setMToN(steps_.mToN);
    dataCollect->setDistinct(distinct_);
    dataCollect->setColNames(projectResult->colNames());
    root_ = dataCollect;
    return Status::OK();
}

PlanNode* GoValidator::buildProjectSrcEdgePropsForGN(std::string gnVar, PlanNode* dependency) {
    DCHECK(dependency != nullptr);

    // Get _vid for join if $-/$var were declared.
    if (from_.fromType != FromType::kInstantExpr) {
        auto* srcVidCol = new YieldColumn(
            new VariablePropertyExpression(new std::string(gnVar), new std::string(kVid)),
            new std::string(kVid));
        srcAndEdgePropCols_->addColumn(srcVidCol);
    }

    // Get all _dst to a single column.
    if (!exprProps_.dstTagProps().empty()) {
        joinDstVidColName_ = vctx_->anonColGen()->getCol();
        auto* dstVidCol =
            new YieldColumn(new EdgePropertyExpression(new std::string("*"), new std::string(kDst)),
                            new std::string(joinDstVidColName_));
        srcAndEdgePropCols_->addColumn(dstVidCol);
    }

    auto* project = Project::make(qctx_, dependency, srcAndEdgePropCols_);
    project->setInputVar(gnVar);
    project->setColNames(deduceColNames(srcAndEdgePropCols_));
    VLOG(1) << project->outputVar();

    return project;
}

PlanNode* GoValidator::buildJoinDstProps(PlanNode* projectSrcDstProps) {
    DCHECK(dstPropCols_ != nullptr);
    DCHECK(projectSrcDstProps != nullptr);

    auto objPool = qctx_->objPool();

    auto* yieldDsts = objPool->makeAndAdd<YieldColumns>();
    yieldDsts->addColumn(new YieldColumn(
        new InputPropertyExpression(new std::string(joinDstVidColName_)),
        new std::string(joinDstVidColName_)));
    auto* projectDsts = Project::make(qctx_, projectSrcDstProps, yieldDsts);
    projectDsts->setInputVar(projectSrcDstProps->outputVar());
    projectDsts->setColNames(std::vector<std::string>{joinDstVidColName_});

    auto* dedupVids = Dedup::make(qctx_, projectDsts);
    dedupVids->setInputVar(projectDsts->outputVar());

    auto* vids = objPool->makeAndAdd<VariablePropertyExpression>(
        new std::string(dedupVids->outputVar()), new std::string(joinDstVidColName_));
    auto* getDstVertices =
        GetVertices::make(qctx_, dedupVids, space_.id, vids, buildDstVertexProps(), {});
    getDstVertices->setInputVar(dedupVids->outputVar());

    auto vidColName = vctx_->anonColGen()->getCol();
    auto* vidCol = new YieldColumn(
        new VariablePropertyExpression(new std::string(getDstVertices->outputVar()),
                                    new std::string(kVid)),
        new std::string(vidColName));
    dstPropCols_->addColumn(vidCol);
    auto* project = Project::make(qctx_, getDstVertices, dstPropCols_);
    project->setInputVar(getDstVertices->outputVar());
    project->setColNames(deduceColNames(dstPropCols_));

    auto* joinHashKey = objPool->makeAndAdd<VariablePropertyExpression>(
        new std::string(projectSrcDstProps->outputVar()),
        new std::string(joinDstVidColName_));
    auto* probeKey = objPool->makeAndAdd<VariablePropertyExpression>(
        new std::string(project->outputVar()), new std::string(vidColName));
    auto joinDst = DataJoin::make(qctx_, project,
            {projectSrcDstProps->outputVar(), ExecutionContext::kLatestVersion},
            {project->outputVar(), ExecutionContext::kLatestVersion},
            {joinHashKey}, {probeKey});
    VLOG(1) << joinDst->outputVar() << " hash key: " << joinHashKey->toString()
        << " probe key: " << probeKey->toString();

    return joinDst;
}

PlanNode* GoValidator::buildJoinPipeOrVariableInput(PlanNode* projectFromJoin,
                                                    PlanNode* dependencyForJoinInput) {
    auto* pool = qctx_->objPool();

    if (steps_.steps > 1 || steps_.mToN != nullptr) {
        DCHECK(projectFromJoin != nullptr);
        auto* joinHashKey = pool->add(new VariablePropertyExpression(
            new std::string(dependencyForJoinInput->outputVar()), new std::string(kVid)));
        auto* probeKey = pool->add(new VariablePropertyExpression(
            new std::string(projectFromJoin->outputVar()), new std::string(dstVidColName_)));
        auto* join =
            DataJoin::make(qctx_,
                           dependencyForJoinInput,
                           {dependencyForJoinInput->outputVar(), ExecutionContext::kLatestVersion},
                           {projectFromJoin->outputVar(),
                            steps_.mToN != nullptr ? ExecutionContext::kPreviousOneVersion
                                                   : ExecutionContext::kLatestVersion},
                           {joinHashKey},
                           {probeKey});
        std::vector<std::string> colNames = dependencyForJoinInput->colNames();
        for (auto& col : projectFromJoin->colNames()) {
            colNames.emplace_back(col);
        }
        join->setColNames(std::move(colNames));
        VLOG(1) << join->outputVar();
        dependencyForJoinInput = join;
    }

    DCHECK(dependencyForJoinInput != nullptr);
    auto* joinHashKey = pool->add(
        new VariablePropertyExpression(new std::string(dependencyForJoinInput->outputVar()),
                                       new std::string((steps_.steps > 1 || steps_.mToN != nullptr)
                                                           ? from_.firstBeginningSrcVidColName
                                                           : kVid)));
    auto* joinInput =
        DataJoin::make(qctx_, dependencyForJoinInput,
                        {dependencyForJoinInput->outputVar(),
                        ExecutionContext::kLatestVersion},
                        {from_.fromType == kPipe ? inputVarName_ : from_.userDefinedVarName,
                        ExecutionContext::kLatestVersion},
                        {joinHashKey}, {from_.srcRef});
    std::vector<std::string> colNames = dependencyForJoinInput->colNames();
    for (auto& col : outputs_) {
        colNames.emplace_back(col.first);
    }
    joinInput->setColNames(std::move(colNames));
    VLOG(1) << joinInput->outputVar();

    return joinInput;
}

PlanNode* GoValidator::traceToStartVid(PlanNode* projectLeftVarForJoin,
                                       PlanNode* dedupDstVids) {
    DCHECK(projectLeftVarForJoin != nullptr);
    DCHECK(dedupDstVids != nullptr);

    auto* pool = qctx_->objPool();
    auto hashKey = new VariablePropertyExpression(
            new std::string(projectLeftVarForJoin->outputVar()),
            new std::string(dstVidColName_));
    pool->add(hashKey);
    auto probeKey = new VariablePropertyExpression(
        new std::string(dedupDstVids->outputVar()), new std::string(srcVidColName_));
    pool->add(probeKey);
    auto* join = DataJoin::make(
        qctx_, dedupDstVids,
        {projectLeftVarForJoin->outputVar(),
            ExecutionContext::kLatestVersion},
        {dedupDstVids->outputVar(), ExecutionContext::kLatestVersion},
        {hashKey}, {probeKey});
    std::vector<std::string> colNames = projectLeftVarForJoin->colNames();
    for (auto& col : dedupDstVids->colNames()) {
        colNames.emplace_back(col);
    }
    join->setColNames(std::move(colNames));
    VLOG(1) << join->outputVar();

    auto* columns = pool->add(new YieldColumns());
    auto* column = new YieldColumn(
        new InputPropertyExpression(new std::string(from_.firstBeginningSrcVidColName)),
        new std::string(from_.firstBeginningSrcVidColName));
    columns->addColumn(column);
    column =
        new YieldColumn(new InputPropertyExpression(new std::string(kVid)),
                        new std::string(dstVidColName_));
    columns->addColumn(column);
    auto* projectJoin = Project::make(qctx_, join, columns);
    projectJoin->setInputVar(join->outputVar());
    projectJoin->setColNames(deduceColNames(columns));
    VLOG(1) << projectJoin->outputVar();

    auto* dedup = Dedup::make(qctx_, projectJoin);
    dedup->setInputVar(projectJoin->outputVar());
    dedup->setOutputVar(projectLeftVarForJoin->outputVar());
    dedup->setColNames(projectJoin->colNames());
    return dedup;
}

PlanNode* GoValidator::buildLeftVarForTraceJoin(PlanNode* dedupStartVid) {
    auto* pool = qctx_->objPool();
    dstVidColName_ = vctx_->anonColGen()->getCol();
    auto* columns = pool->add(new YieldColumns());
    auto* column = new YieldColumn(from_.srcRef->clone().release(),
                                   new std::string(from_.firstBeginningSrcVidColName));
    columns->addColumn(column);
    column = new YieldColumn(from_.srcRef->clone().release(), new std::string(dstVidColName_));
    columns->addColumn(column);
    // dedupStartVid could be nullptr, that means no input for this project.
    auto* projectLeftVarForJoin = Project::make(qctx_, dedupStartVid, columns);
    projectLeftVarForJoin->setInputVar(
        from_.fromType == kPipe ? inputVarName_ : from_.userDefinedVarName);
    projectLeftVarForJoin->setColNames(deduceColNames(columns));

    auto* dedup = Dedup::make(qctx_, projectLeftVarForJoin);
    dedup->setInputVar(projectLeftVarForJoin->outputVar());
    dedup->setColNames(projectLeftVarForJoin->colNames());
    return dedup;
}

Status GoValidator::buildOneStepPlan() {
    std::string startVidsVar;
    PlanNode* dedupStartVid = nullptr;
    if (!from_.vids.empty() && from_.srcRef == nullptr) {
        buildConstantInput(from_, startVidsVar, src_);
    } else {
        dedupStartVid = buildRuntimeInput();
        startVidsVar = dedupStartVid->outputVar();
    }

    auto status = oneStep(dedupStartVid, startVidsVar, nullptr);
    if (!status.ok()) {
        return status;
    }

    if (projectStartVid_ != nullptr) {
        tail_ = projectStartVid_;
    }
    VLOG(1) << "root: " << root_->kind() << " tail: " << tail_->kind();
    return Status::OK();
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
        if (over_.direction == storage::cpp2::EdgeDirection::IN_EDGE) {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
            buildEdgeProps(edgeProps, true);
        } else if (over_.direction == storage::cpp2::EdgeDirection::BOTH) {
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
    edgeProps->reserve(over_.edgeTypes.size());
    for (auto& e : over_.edgeTypes) {
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
        if (over_.direction == storage::cpp2::EdgeDirection::IN_EDGE) {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(
                over_.edgeTypes.size());
            std::transform(over_.edgeTypes.begin(), over_.edgeTypes.end(), edgeProps->begin(),
                        [](auto& type) {
                            storage::cpp2::EdgeProp ep;
                            ep.type = -type;
                            ep.props = {kDst};
                            return ep;
                        });
        } else if (over_.direction == storage::cpp2::EdgeDirection::BOTH) {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(
                over_.edgeTypes.size() * 2);
            std::transform(over_.edgeTypes.begin(), over_.edgeTypes.end(), edgeProps->begin(),
                        [](auto& type) {
                            storage::cpp2::EdgeProp ep;
                            ep.type = type;
                            ep.props = {kDst};
                            return ep;
                        });
            std::transform(over_.edgeTypes.begin(), over_.edgeTypes.end(),
                           edgeProps->begin() + over_.edgeTypes.size(),
                           [](auto& type) {
                               storage::cpp2::EdgeProp ep;
                               ep.type = -type;
                               ep.props = {kDst};
                               return ep;
                           });
        } else {
            edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(
                over_.edgeTypes.size());
            std::transform(over_.edgeTypes.begin(), over_.edgeTypes.end(), edgeProps->begin(),
                        [](auto& type) {
                            storage::cpp2::EdgeProp ep;
                            ep.type = type;
                            ep.props = {kDst};
                            return ep;
                        });
        }
    }
    return edgeProps;
}

void GoValidator::extractPropExprs(const Expression* expr) {
    ExtractPropExprVisitor visitor(
        vctx_, srcAndEdgePropCols_, dstPropCols_, inputPropCols_, propExprColMap_);
    const_cast<Expression*>(expr)->accept(&visitor);
}

std::unique_ptr<Expression> GoValidator::rewriteToInputProp(Expression* expr) {
    RewriteInputPropVisitor visitor(propExprColMap_);
    const_cast<Expression*>(expr)->accept(&visitor);
    if (visitor.ok()) {
        return std::move(visitor).result();
    }
    return nullptr;
}

Status GoValidator::buildColumns() {
    if (exprProps_.dstTagProps().empty() && exprProps_.inputProps().empty() &&
        exprProps_.varProps().empty() && from_.fromType == FromType::kInstantExpr) {
        return Status::OK();
    }

    auto pool = qctx_->objPool();
    if (!exprProps_.srcTagProps().empty() || !exprProps_.edgeProps().empty() ||
        !exprProps_.dstTagProps().empty()) {
        srcAndEdgePropCols_ = pool->add(new YieldColumns());
    }

    if (!exprProps_.dstTagProps().empty()) {
        dstPropCols_ = pool->add(new YieldColumns());
    }

    if (!exprProps_.inputProps().empty() || !exprProps_.varProps().empty()) {
        inputPropCols_ = pool->add(new YieldColumns());
    }

    if (filter_ != nullptr) {
        extractPropExprs(filter_);
        auto newFilter = filter_->clone();
        auto rewriteFilter = rewriteToInputProp(newFilter.get());
        if (rewriteFilter != nullptr) {
            newFilter_ = rewriteFilter.release();
        } else {
            newFilter_ = newFilter.release();
        }
        pool->add(newFilter_);
    }

    newYieldCols_ = pool->add(new YieldColumns());
    for (auto* yield : yields_->columns()) {
        extractPropExprs(yield->expr());
        auto newCol = yield->expr()->clone();
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
