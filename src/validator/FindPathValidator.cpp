/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FindPathValidator.h"

#include "common/expression/VariableExpression.h"
#include "planner/plan/Algo.h"
#include "planner/plan/Logic.h"

namespace nebula {
namespace graph {
Status FindPathValidator::validateImpl() {
    auto fpSentence = static_cast<FindPathSentence*>(sentence_);
    isShortest_ = fpSentence->isShortest();
    noLoop_ = fpSentence->noLoop();

    NG_RETURN_IF_ERROR(validateStarts(fpSentence->from(), from_));
    NG_RETURN_IF_ERROR(validateStarts(fpSentence->to(), to_));
    NG_RETURN_IF_ERROR(validateOver(fpSentence->over(), over_));
    NG_RETURN_IF_ERROR(validateStep(fpSentence->step(), steps_));

    outputs_.emplace_back("path", Value::Type::PATH);
    return Status::OK();
}

Status FindPathValidator::toPlan() {
    if (!isShortest_ || noLoop_) {
        return allPairPaths();
    }
    if (from_.vids.size() == 1 && to_.vids.size() == 1) {
        return singlePairPlan();
    }
    return multiPairPlan();
}

void FindPathValidator::buildStart(Starts& starts,
                                   std::string& startVidsVar,
                                   bool reverse) {
    if (!starts.vids.empty() && starts.originalSrc == nullptr) {
        buildConstantInput(starts, startVidsVar);
    } else {
        if (reverse) {
            toDedupStartVid_ = buildRuntimeInput(starts, toProjectStartVid_);
            startVidsVar = toDedupStartVid_->outputVar();
        } else {
            fromDedupStartVid_ = buildRuntimeInput(starts, projectStartVid_);
            startVidsVar = fromDedupStartVid_->outputVar();
        }
    }
}

Status FindPathValidator::singlePairPlan() {
    auto* bodyStart = StartNode::make(qctx_);
    auto* passThrough = PassThroughNode::make(qctx_, bodyStart);

    auto* forward = bfs(passThrough, from_, false);
    VLOG(1) << "forward: " << forward->outputVar();

    auto* backward = bfs(passThrough, to_, true);
    VLOG(1) << "backward: " << backward->outputVar();

    auto* conjunct =
        ConjunctPath::make(qctx_, forward, backward, ConjunctPath::PathKind::kBiBFS, steps_.steps);
    conjunct->setColNames({"_path"});

    auto* loop = Loop::make(
        qctx_, nullptr, conjunct, buildBfsLoopCondition(steps_.steps, conjunct->outputVar()));

    auto* dataCollect = DataCollect::make(
        qctx_, loop, DataCollect::CollectKind::kBFSShortest, {conjunct->outputVar()});
    dataCollect->setColNames({"path"});

    root_ = dataCollect;
    tail_ = loop;
    return Status::OK();
}

PlanNode* FindPathValidator::bfs(PlanNode* dep, Starts& starts, bool reverse) {
    std::string startVidsVar;
    buildConstantInput(starts, startVidsVar);
    auto* gn = GetNeighbors::make(qctx_, dep, space_.id);
    gn->setSrc(starts.src);
    gn->setEdgeProps(buildEdgeKey(reverse));
    gn->setDedup();
    gn->setInputVar(startVidsVar);

    auto* bfs = BFSShortestPath::make(qctx_, gn);
    bfs->setOutputVar(startVidsVar);
    bfs->setColNames({nebula::kVid, "edge"});

    DataSet ds;
    ds.colNames = {nebula::kVid, "edge"};
    Row row;
    row.values.emplace_back(starts.vids.front());
    row.values.emplace_back(Value::kEmpty);
    ds.rows.emplace_back(std::move(row));
    qctx_->ectx()->setResult(startVidsVar, ResultBuilder().value(Value(std::move(ds))).finish());

    return bfs;
}

Expression* FindPathValidator::buildBfsLoopCondition(uint32_t steps, const std::string& pathVar) {
    // ++loopSteps{0} <= (steps/2+steps%2) && size(pathVar) == 0
    auto loopSteps = vctx_->anonVarGen()->getVar();
    qctx_->ectx()->setValue(loopSteps, 0);

    auto* nSteps = new RelationalExpression(
        Expression::Kind::kRelLE,
        new UnaryExpression(
            Expression::Kind::kUnaryIncr,
            new VersionedVariableExpression(new std::string(loopSteps), new ConstantExpression(0))),
        new ConstantExpression(static_cast<int32_t>(steps / 2 + steps % 2)));

    auto* args = new ArgumentList();
    args->addArgument(std::make_unique<VariableExpression>(new std::string(pathVar)));
    auto* pathEmpty =
        new RelationalExpression(Expression::Kind::kRelEQ,
                                 new FunctionCallExpression(new std::string("size"), args),
                                 new ConstantExpression(0));
    auto* notFoundPath = new LogicalExpression(
        Expression::Kind::kLogicalOr,
        new RelationalExpression(Expression::Kind::kRelEQ,
                                 new VariableExpression(new std::string(pathVar)),
                                 new ConstantExpression(Value())),
        pathEmpty);
    return qctx_->objPool()->add(
        new LogicalExpression(Expression::Kind::kLogicalAnd, nSteps, notFoundPath));
}

void FindPathValidator::buildEdgeProps(GetNeighbors::EdgeProps& edgeProps,
                                       bool reverse,
                                       bool isInEdge) {
    for (auto& e : over_.edgeTypes) {
        storage::cpp2::EdgeProp ep;
        if (reverse == isInEdge) {
            ep.set_type(e);
        } else {
            ep.set_type(-e);
        }
        ep.set_props({kDst, kType, kRank});
        edgeProps->emplace_back(std::move(ep));
    }
}

GetNeighbors::EdgeProps FindPathValidator::buildEdgeKey(bool reverse) {
    auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>();
    switch (over_.direction) {
        case storage::cpp2::EdgeDirection::IN_EDGE: {
            buildEdgeProps(edgeProps, reverse, true);
            break;
        }
        case storage::cpp2::EdgeDirection::OUT_EDGE: {
            buildEdgeProps(edgeProps, reverse, false);
            break;
        }
        case storage::cpp2::EdgeDirection::BOTH: {
            buildEdgeProps(edgeProps, reverse, true);
            buildEdgeProps(edgeProps, reverse, false);
            break;
        }
    }
    return edgeProps;
}

PlanNode* FindPathValidator::buildAllPairFirstDataSet(PlanNode* dep, const std::string& inputVar) {
    auto* vid =
        new YieldColumn(new VariablePropertyExpression(new std::string("*"), new std::string(kVid)),
                        new std::string(kVid));
    auto* pathExpr = new PathBuildExpression();
    pathExpr->add(
        std::make_unique<VariablePropertyExpression>(new std::string("*"), new std::string(kVid)));
    auto* exprList = new ExpressionList();
    exprList->add(pathExpr);
    auto* listExprssion = new ListExpression(exprList);
    auto* path = new YieldColumn(listExprssion, new std::string("path"));

    auto* columns = qctx_->objPool()->add(new YieldColumns());
    columns->addColumn(vid);
    columns->addColumn(path);

    auto* project = Project::make(qctx_, dep, columns);
    project->setInputVar(inputVar);
    auto* outputVarPtr = qctx_->symTable()->getVar(inputVar);
    outputVarPtr->colNames = {nebula::kVid, "path"};
    project->setOutputVar(inputVar);
    return project;
}

Status FindPathValidator::allPairPaths() {
    auto* bodyStart = StartNode::make(qctx_);
    auto* passThrough = PassThroughNode::make(qctx_, bodyStart);

    std::string fromStartVidsVar;
    buildStart(from_, fromStartVidsVar, false);

    auto* forward = allPaths(passThrough, from_, fromStartVidsVar, false);
    VLOG(1) << "forward: " << forward->outputVar();

    std::string toStartVidsVar;
    buildStart(to_, toStartVidsVar, true);
    auto* backward = allPaths(passThrough, to_, toStartVidsVar, true);
    VLOG(1) << "backward: " << backward->outputVar();

    auto* conjunct = ConjunctPath::make(
        qctx_, forward, backward, ConjunctPath::PathKind::kAllPaths, steps_.steps);
    conjunct->setColNames({"_path"});
    conjunct->setNoLoop(noLoop_);

    PlanNode* projectFromDep = nullptr;
    linkLoopDepFromTo(projectFromDep);

    auto* projectFrom = buildAllPairFirstDataSet(projectFromDep, fromStartVidsVar);
    auto* projectTo = buildAllPairFirstDataSet(projectFrom, toStartVidsVar);

    auto* loop =
        Loop::make(qctx_, projectTo, conjunct, buildAllPathsLoopCondition(steps_.steps));

    auto* dataCollect = DataCollect::make(
        qctx_, loop, DataCollect::CollectKind::kAllPaths, {conjunct->outputVar()});
    dataCollect->setColNames({"path"});

    root_ = dataCollect;
    tail_ = loopDepTail_ == nullptr ? projectFrom : loopDepTail_;
    return Status::OK();
}

PlanNode* FindPathValidator::allPaths(PlanNode* dep,
                                      Starts& starts,
                                      std::string& startVidsVar,
                                      bool reverse) {
    auto* gn = GetNeighbors::make(qctx_, dep, space_.id);
    gn->setSrc(starts.src);
    gn->setEdgeProps(buildEdgeKey(reverse));
    gn->setInputVar(startVidsVar);
    gn->setDedup();

    auto* allPaths = ProduceAllPaths::make(qctx_, gn);
    allPaths->setOutputVar(startVidsVar);
    allPaths->setColNames({nebula::kVid, "path"});
    return allPaths;
}

Expression* FindPathValidator::buildAllPathsLoopCondition(uint32_t steps) {
    // ++loopSteps{0} <= (steps/2+steps%2) && size(pathVar) == 0
    auto loopSteps = vctx_->anonVarGen()->getVar();
    qctx_->ectx()->setValue(loopSteps, 0);

    auto* nSteps = new RelationalExpression(
        Expression::Kind::kRelLE,
        new UnaryExpression(
            Expression::Kind::kUnaryIncr,
            new VersionedVariableExpression(new std::string(loopSteps), new ConstantExpression(0))),
        new ConstantExpression(static_cast<int32_t>(steps / 2 + steps % 2)));

    return qctx_->objPool()->add(nSteps);
}

/*
 * The plan of From subtree's would be:
 * Project(from) <- Dedup(from)
 * and the To would be:
 * Project(to) <- Dedup(to)
 * After connecting, it would be:
 * Project(from) <- Dedup(from) <- Project(to) <- Dedup(to)
 */
void FindPathValidator::linkLoopDepFromTo(PlanNode*& projectDep) {
    if (!from_.vids.empty() && from_.originalSrc == nullptr) {
        if (!to_.vids.empty() && to_.originalSrc == nullptr) {
            return;
        }
        loopDepTail_ = toProjectStartVid_;
        projectDep = toDedupStartVid_;
    } else {
        if (!to_.vids.empty() && to_.originalSrc == nullptr) {
            loopDepTail_ = projectStartVid_;
            projectDep = fromDedupStartVid_;
        } else {
            auto* toProject = static_cast<SingleInputNode*>(toProjectStartVid_);
            toProject->dependsOn(fromDedupStartVid_);
            auto inputVarName = to_.fromType == kPipe ? inputVarName_ : to_.userDefinedVarName;
            toProject->setInputVar(inputVarName);
            loopDepTail_ = projectStartVid_;
            projectDep = toDedupStartVid_;
        }
    }
}

PlanNode* FindPathValidator::buildMultiPairFirstDataSet(PlanNode* dep,
                                                        const std::string& inputVar,
                                                        const std::string& outputVar) {
    auto* dst =
        new YieldColumn(new VariablePropertyExpression(new std::string("*"), new std::string(kVid)),
                        new std::string(kDst));
    auto* src =
        new YieldColumn(new VariablePropertyExpression(new std::string("*"), new std::string(kVid)),
                        new std::string(kSrc));
    auto* cost = new YieldColumn(new ConstantExpression(0), new std::string("cost"));
    auto* pathExpr = new PathBuildExpression();
    pathExpr->add(
        std::make_unique<VariablePropertyExpression>(new std::string("*"), new std::string(kVid)));
    auto* exprList = new ExpressionList();
    exprList->add(pathExpr);
    auto* listExprssion = new ListExpression(exprList);
    auto* paths = new YieldColumn(listExprssion, new std::string("paths"));

    auto* columns = qctx_->objPool()->add(new YieldColumns());
    columns->addColumn(dst);
    columns->addColumn(src);
    columns->addColumn(cost);
    columns->addColumn(paths);

    auto* project = Project::make(qctx_, dep, columns);
    project->setInputVar(inputVar);
    auto* outputVarPtr = qctx_->symTable()->getVar(outputVar);
    outputVarPtr->colNames = {kDst, kSrc, "cost", "paths"};
    project->setOutputVar(outputVar);
    return project;
}

Status FindPathValidator::multiPairPlan() {
    auto* bodyStart = StartNode::make(qctx_);
    auto* passThrough = PassThroughNode::make(qctx_, bodyStart);

    std::string fromStartVidsVar;
    buildStart(from_, fromStartVidsVar, false);
    std::string fromPathVar;
    auto* forward = multiPairShortestPath(passThrough, from_, fromStartVidsVar, fromPathVar, false);
    VLOG(1) << "forward: " << fromPathVar;

    std::string toStartVidsVar;
    buildStart(to_, toStartVidsVar, true);
    std::string toPathVar;
    auto* backward = multiPairShortestPath(passThrough, to_, toStartVidsVar, toPathVar, true);
    VLOG(1) << "backward: " << toPathVar;

    auto* conjunct =
        ConjunctPath::make(qctx_, forward, backward, ConjunctPath::PathKind::kFloyd, steps_.steps);

    conjunct->setLeftVar(fromPathVar);
    conjunct->setRightVar(toPathVar);
    conjunct->setColNames({"_path", "cost"});

    PlanNode* projectFromDep = nullptr;
    linkLoopDepFromTo(projectFromDep);

    auto* projectFrom = buildMultiPairFirstDataSet(projectFromDep, fromStartVidsVar, fromPathVar);
    auto* projectTo = buildMultiPairFirstDataSet(projectFrom, toStartVidsVar, toPathVar);

    auto* cartesianProduct = CartesianProduct::make(qctx_, projectTo);
    NG_RETURN_IF_ERROR(cartesianProduct->addVar(fromStartVidsVar));
    NG_RETURN_IF_ERROR(cartesianProduct->addVar(toStartVidsVar));

    conjunct->setConditionalVar(cartesianProduct->outputVar());

    auto* loop =
        Loop::make(qctx_,
                   cartesianProduct,
                   conjunct,
                   buildMultiPairLoopCondition(steps_.steps, cartesianProduct->outputVar()));

    auto* dataCollect = DataCollect::make(
        qctx_, loop, DataCollect::CollectKind::kMultiplePairShortest, {conjunct->outputVar()});
    dataCollect->setColNames({"path"});

    root_ = dataCollect;
    tail_ = loopDepTail_ == nullptr ? projectFrom : loopDepTail_;
    return Status::OK();
}

PlanNode* FindPathValidator::multiPairShortestPath(PlanNode* dep,
                                                   Starts& starts,
                                                   std::string& startVidsVar,
                                                   std::string& pathVar,
                                                   bool reverse) {
    auto* gn = GetNeighbors::make(qctx_, dep, space_.id);
    gn->setSrc(starts.src);
    gn->setEdgeProps(buildEdgeKey(reverse));
    gn->setInputVar(startVidsVar);
    gn->setDedup();

    auto* pssp = ProduceSemiShortestPath::make(qctx_, gn);
    pssp->setColNames({nebula::kDst, nebula::kSrc, "cost", "paths"});
    pathVar = pssp->outputVar();

    auto* columns = qctx_->objPool()->add(new YieldColumns());
    auto* column =
        new YieldColumn(new VariablePropertyExpression(new std::string("*"), new std::string(kDst)),
                        new std::string(kVid));
    columns->addColumn(column);
    auto* project = Project::make(qctx_, pssp, columns);
    project->setOutputVar(startVidsVar);
    project->setColNames(deduceColNames(columns));

    return project;
}

Expression* FindPathValidator::buildMultiPairLoopCondition(uint32_t steps,
                                                           std::string conditionalVar) {
    // (++loopSteps{0} <= steps/2+steps%2) && (isWeight_ == true || size(conditionalVar) == 0)
    auto loopSteps = vctx_->anonVarGen()->getVar();
    qctx_->ectx()->setValue(loopSteps, 0);

    auto* nSteps = new RelationalExpression(
        Expression::Kind::kRelLE,
        new UnaryExpression(
            Expression::Kind::kUnaryIncr,
            new VersionedVariableExpression(new std::string(loopSteps), new ConstantExpression(0))),
        new ConstantExpression(static_cast<int32_t>(steps / 2 + steps % 2)));

    auto* args = new ArgumentList();
    args->addArgument(std::make_unique<VariableExpression>(new std::string(conditionalVar)));
    auto* notAllPathFind =
        new RelationalExpression(Expression::Kind::kRelGT,
                                 new FunctionCallExpression(new std::string("size"), args),
                                 new ConstantExpression(0));

    auto* terminationCondition =
        new LogicalExpression(Expression::Kind::kLogicalOr,
                              new RelationalExpression(Expression::Kind::kRelEQ,
                                                       new ConstantExpression(isWeight_),
                                                       new ConstantExpression(true)),
                              notAllPathFind);
    auto* condition =
        new LogicalExpression(Expression::Kind::kLogicalAnd, nSteps, terminationCondition);

    return qctx_->objPool()->add(condition);
}

}  // namespace graph
}  // namespace nebula
