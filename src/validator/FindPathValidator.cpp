/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/FindPathValidator.h"

#include "common/expression/VariableExpression.h"
#include "planner/Algo.h"
#include "planner/Logic.h"

namespace nebula {
namespace graph {
Status FindPathValidator::validateImpl() {
    auto fpSentence = static_cast<FindPathSentence*>(sentence_);
    isShortest_ = fpSentence->isShortest();

    NG_RETURN_IF_ERROR(validateStarts(fpSentence->from(), from_));
    NG_RETURN_IF_ERROR(validateStarts(fpSentence->to(), to_));
    NG_RETURN_IF_ERROR(validateOver(fpSentence->over(), over_));
    NG_RETURN_IF_ERROR(validateStep(fpSentence->step(), steps_));
    return Status::OK();
}

Status FindPathValidator::toPlan() {
    // TODO: Implement the path plan.
    if (from_.vids.size() == 1 && to_.vids.size() == 1) {
        return singlePairPlan();
    } else {
        auto* passThrough = PassThroughNode::make(qctx_, nullptr);
        tail_ = passThrough;
        root_ = tail_;
    }
    return Status::OK();
}

Status FindPathValidator::singlePairPlan() {
    auto* bodyStart = StartNode::make(qctx_);
    auto* passThrough = PassThroughNode::make(qctx_, bodyStart);

    auto* forward = bfs(passThrough, from_, false);
    VLOG(1) << "forward: " << forward->outputVar();

    auto* backward = bfs(passThrough, to_, true);
    VLOG(1) << "backward: " << backward->outputVar();

    auto* conjunct =
        ConjunctPath::make(qctx_, forward, backward, ConjunctPath::PathKind::kBiBFS);
    conjunct->setLeftVar(forward->outputVar());
    conjunct->setRightVar(backward->outputVar());
    conjunct->setColNames({"_path"});

    auto* loop = Loop::make(
        qctx_, nullptr, conjunct, buildBfsLoopCondition(steps_.steps, conjunct->outputVar()));

    auto* dataCollect = DataCollect::make(
        qctx_, loop, DataCollect::CollectKind::kBFSShortest, {conjunct->outputVar()});
    dataCollect->setColNames({"_path"});

    root_ = dataCollect;
    tail_ = loop;
    return Status::OK();
}

void FindPathValidator::buildStart(const Starts& starts,
                                   std::string& startVidsVar,
                                   PlanNode* dedupStartVid,
                                   Expression* src) {
    if (!starts.vids.empty() && starts.srcRef == nullptr) {
        buildConstantInput(starts, startVidsVar, src);
    } else {
        dedupStartVid = buildRuntimeInput();
        startVidsVar = dedupStartVid->outputVar();
    }
}

PlanNode* FindPathValidator::bfs(PlanNode* dep, const Starts& starts, bool reverse) {
    std::string startVidsVar;
    Expression* vids = nullptr;
    buildConstantInput(starts, startVidsVar, vids);

    DCHECK(!!vids);
    auto* gn = GetNeighbors::make(qctx_, dep, space_.id);
    gn->setSrc(vids);
    gn->setEdgeProps(buildEdgeKey(reverse));
    gn->setInputVar(startVidsVar);

    auto* bfs = BFSShortestPath::make(qctx_, gn);
    bfs->setInputVar(gn->outputVar());
    bfs->setColNames({"_vid", "edge"});
    bfs->setOutputVar(startVidsVar);

    DataSet ds;
    ds.colNames = {"_vid", "edge"};
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

GetNeighbors::EdgeProps FindPathValidator::buildEdgeKey(bool reverse) {
    auto edgeProps = std::make_unique<std::vector<storage::cpp2::EdgeProp>>(
                over_.edgeTypes.size());
    std::transform(over_.edgeTypes.begin(), over_.edgeTypes.end(), edgeProps->begin(),
                [reverse](auto& type) {
                    storage::cpp2::EdgeProp ep;
                    ep.type = reverse ? -type : type;
                    ep.props = {kDst, kType, kRank};
                    return ep;
                });
    return edgeProps;
}
}  // namespace graph
}  // namespace nebula
