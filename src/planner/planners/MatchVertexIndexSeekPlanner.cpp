/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/planners/MatchVertexIndexSeekPlanner.h"

#include "planner/planners/MatchSolver.h"
#include "util/ExpressionUtils.h"
#include "visitor/RewriteMatchLabelVisitor.h"

namespace nebula {
namespace graph {
bool MatchVertexIndexSeekPlanner::match(AstContext *astCtx) {
    return MatchSolver::match(astCtx);
}

StatusOr<SubPlan> MatchVertexIndexSeekPlanner::transform(AstContext* astCtx) {
    matchCtx_ = static_cast<MatchAstContext*>(astCtx);

    NG_RETURN_IF_ERROR(buildScanNode());
    if (!matchCtx_->edgeInfos.empty()) {
        NG_RETURN_IF_ERROR(buildSteps());
    }
    NG_RETURN_IF_ERROR(buildGetTailVertices());
    if (!matchCtx_->edgeInfos.empty()) {
        NG_RETURN_IF_ERROR(buildTailJoin());
    }
    NG_RETURN_IF_ERROR(MatchSolver::buildFilter(matchCtx_, &subPlan_));
    NG_RETURN_IF_ERROR(MatchSolver::buildReturn(matchCtx_, subPlan_));
    return subPlan_;
}

Status MatchVertexIndexSeekPlanner::buildScanNode() {
    // FIXME: Move following validation to MatchValidator
    if (!startFromNode_) {
        return Status::SemanticError("Scan from edge not supported now");
    }
    if (startIndex_ != 0) {
        return Status::SemanticError("Only support scan from the head node");
    }

    using IQC = nebula::storage::cpp2::IndexQueryContext;
    auto contexts = std::make_unique<std::vector<IQC>>();
    contexts->emplace_back();
    contexts->back().set_filter(Expression::encode(*matchCtx_->scanInfo.filter));
    auto columns = std::make_unique<std::vector<std::string>>();
    auto scan = IndexScan::make(matchCtx_->qctx,
                                nullptr,
                                matchCtx_->space.id,
                                std::move(contexts),
                                std::move(columns),
                                false,
                                matchCtx_->scanInfo.schemaId);
    subPlan_.tail = scan;
    subPlan_.root = scan;

    return Status::OK();
}


Status MatchVertexIndexSeekPlanner::buildSteps() {
    gnSrcExpr_ = new VariablePropertyExpression(new std::string(),
                                                new std::string(kVid));
    saveObject(gnSrcExpr_);
    NG_RETURN_IF_ERROR(buildStep());
    for (auto i = 1u; i < matchCtx_->edgeInfos.size(); i++) {
        NG_RETURN_IF_ERROR(buildStep());
        NG_RETURN_IF_ERROR(buildStepJoin());
    }

    return Status::OK();
}

Status MatchVertexIndexSeekPlanner::buildStep() {
    curStep_++;

    auto &srcNodeInfo = matchCtx_->nodeInfos[curStep_];
    auto &edgeInfo = matchCtx_->edgeInfos[curStep_];
    auto *gn = GetNeighbors::make(matchCtx_->qctx, subPlan_.root, matchCtx_->space.id);
    gn->setSrc(gnSrcExpr_);
    auto vertexProps = std::make_unique<std::vector<VertexProp>>();
    if (srcNodeInfo.label != nullptr) {
        VertexProp vertexProp;
        vertexProp.set_tag(srcNodeInfo.tid);
        vertexProps->emplace_back(std::move(vertexProp));
    }
    gn->setVertexProps(std::move(vertexProps));
    auto edgeProps = std::make_unique<std::vector<EdgeProp>>();
    if (!edgeInfo.edgeTypes.empty()) {
        for (auto edgeType : edgeInfo.edgeTypes) {
            if (edgeInfo.direction == MatchValidator::Direction::IN_EDGE) {
                edgeType = -edgeType;
            } else if (edgeInfo.direction == MatchValidator::Direction::BOTH) {
                EdgeProp edgeProp;
                edgeProp.set_type(-edgeType);
                edgeProps->emplace_back(std::move(edgeProp));
            }
            EdgeProp edgeProp;
            edgeProp.set_type(edgeType);
            edgeProps->emplace_back(std::move(edgeProp));
        }
    }
    gn->setEdgeProps(std::move(edgeProps));
    gn->setEdgeDirection(edgeInfo.direction);

    auto *yields = saveObject(new YieldColumns());
    yields->addColumn(new YieldColumn(new VertexExpression()));
    yields->addColumn(new YieldColumn(new EdgeExpression()));
    auto *project = Project::make(matchCtx_->qctx, gn, yields);
    project->setInputVar(gn->outputVar());
    project->setColNames({*srcNodeInfo.alias, *edgeInfo.alias});

    subPlan_.root = project;

    auto rewriter = [] (const Expression *expr) {
        DCHECK_EQ(expr->kind(), Expression::Kind::kLabelAttribute);
        return MatchSolver::rewrite(static_cast<const LabelAttributeExpression*>(expr));
    };

    if (srcNodeInfo.filter != nullptr) {
        RewriteMatchLabelVisitor visitor(rewriter);
        srcNodeInfo.filter->accept(&visitor);
        auto *node = Filter::make(matchCtx_->qctx, subPlan_.root, srcNodeInfo.filter);
        node->setInputVar(subPlan_.root->outputVar());
        node->setColNames(subPlan_.root->colNames());
        subPlan_.root = node;
    }

    if (edgeInfo.filter != nullptr) {
        RewriteMatchLabelVisitor visitor(rewriter);
        edgeInfo.filter->accept(&visitor);
        auto *node = Filter::make(matchCtx_->qctx, subPlan_.root, edgeInfo.filter);
        node->setInputVar(subPlan_.root->outputVar());
        node->setColNames(subPlan_.root->colNames());
        subPlan_.root = node;
    }

    gnSrcExpr_ = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(project->outputVar()),
                new std::string(*edgeInfo.alias)),
            new LabelExpression("_dst"));
    saveObject(gnSrcExpr_);

    prevStepRoot_ = thisStepRoot_;
    thisStepRoot_ = subPlan_.root;

    return Status::OK();
}

Status MatchVertexIndexSeekPlanner::buildGetTailVertices() {
    Expression *src = nullptr;
    if (!matchCtx_->edgeInfos.empty()) {
        src = new AttributeExpression(
                new VariablePropertyExpression(
                    new std::string(),
                    new std::string(*matchCtx_->edgeInfos[curStep_].alias)),
                new LabelExpression("_dst"));
    } else {
        src = new VariablePropertyExpression(new std::string(),
                new std::string(kVid));
    }
    saveObject(src);

    auto &nodeInfo = matchCtx_->nodeInfos[curStep_ + 1];
    std::vector<VertexProp> props;
    if (nodeInfo.label != nullptr) {
        VertexProp prop;
        prop.set_tag(nodeInfo.tid);
        props.emplace_back(prop);
    }

    auto *gv = GetVertices::make(
        matchCtx_->qctx, subPlan_.root, matchCtx_->space.id, src, std::move(props), {}, true);
    if (thisStepRoot_ != nullptr) {
        gv->setInputVar(thisStepRoot_->outputVar());
    }

    auto *yields = saveObject(new YieldColumns());
    yields->addColumn(new YieldColumn(new VertexExpression()));
    auto *project = Project::make(matchCtx_->qctx, gv, yields);
    project->setInputVar(gv->outputVar());
    project->setColNames({*nodeInfo.alias});
    subPlan_.root = project;

    if (nodeInfo.filter != nullptr) {
        auto newFilter = nodeInfo.filter->clone();
        RewriteMatchLabelVisitor visitor([](auto *expr) {
                DCHECK(expr->kind() == Expression::Kind::kLabelAttribute);
                return MatchSolver::rewrite(static_cast<const LabelAttributeExpression*>(expr));
            });
        newFilter->accept(&visitor);
        auto *filter =
            Filter::make(matchCtx_->qctx, subPlan_.root, saveObject(newFilter.release()));
        filter->setInputVar(subPlan_.root->outputVar());
        filter->setColNames(subPlan_.root->colNames());
        subPlan_.root = filter;
    }

    auto *dedup = Dedup::make(matchCtx_->qctx, subPlan_.root);
    dedup->setInputVar(subPlan_.root->outputVar());
    dedup->setColNames(subPlan_.root->colNames());
    subPlan_.root = dedup;

    return Status::OK();
}


Status MatchVertexIndexSeekPlanner::buildStepJoin() {
    auto prevStep = curStep_ - 1;
    auto key = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(prevStepRoot_->outputVar()),
                new std::string(*matchCtx_->edgeInfos[prevStep].alias)),
            new LabelExpression("_dst"));
    auto probe = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(thisStepRoot_->outputVar()),
                new std::string(*matchCtx_->nodeInfos[curStep_].alias)),
            new LabelExpression("_vid"));
    auto *join = DataJoin::make(matchCtx_->qctx,
                                subPlan_.root,
                                {prevStepRoot_->outputVar(), 0},
                                {thisStepRoot_->outputVar(), 0},
                                {key},
                                {probe});
    auto leftColNames = prevStepRoot_->colNames();
    auto rightColNames = thisStepRoot_->colNames();
    std::vector<std::string> colNames;
    colNames.reserve(leftColNames.size() + rightColNames.size());
    for (auto &name : leftColNames) {
        colNames.emplace_back(std::move(name));
    }
    for (auto &name : rightColNames) {
        colNames.emplace_back(std::move(name));
    }
    join->setColNames(std::move(colNames));
    subPlan_.root = join;
    thisStepRoot_ = subPlan_.root;

    return Status::OK();
}


Status MatchVertexIndexSeekPlanner::buildTailJoin() {
    auto key = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(thisStepRoot_->outputVar()),
                new std::string(*matchCtx_->edgeInfos[curStep_].alias)),
            new LabelExpression("_dst"));
    auto probe = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(subPlan_.root->outputVar()),
                new std::string(*matchCtx_->nodeInfos[curStep_ + 1].alias)),
            new LabelExpression("_vid"));
    auto *join = DataJoin::make(matchCtx_->qctx,
                                subPlan_.root,
                                {thisStepRoot_->outputVar(), 0},
                                {subPlan_.root->outputVar(), 0},
                                {key},
                                {probe});
    auto colNames = thisStepRoot_->colNames();
    colNames.emplace_back(*matchCtx_->nodeInfos[curStep_ + 1].alias);
    join->setColNames(std::move(colNames));
    subPlan_.root = join;

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
