/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/MatchSolver.h"

#include "context/ast/AstContext.h"
#include "context/ast/QueryAstContext.h"
#include "util/ExpressionUtils.h"
#include "visitor/RewriteMatchLabelVisitor.h"
#include "planner/Planner.h"
#include "planner/Query.h"

namespace nebula {
namespace graph {
Expression* MatchSolver::rewrite(const LabelExpression* label) {
    auto* expr = new VariablePropertyExpression(
            new std::string(),
            new std::string(*label->name()));
    return expr;
}

Expression* MatchSolver::rewrite(const LabelAttributeExpression *la) {
    const auto &value = la->right()->value();
    auto *expr = new AttributeExpression(
            new VariablePropertyExpression(
                new std::string(),
                new std::string(*la->left()->name())),
            new ConstantExpression(value));
    return expr;
}

Expression* MatchSolver::doRewrite(const std::unordered_map<std::string, AliasType>& aliases,
                                   const Expression* expr) {
    if (expr->kind() != Expression::Kind::kLabel) {
        return rewrite(static_cast<const LabelAttributeExpression*>(expr));
    }

    auto* labelExpr = static_cast<const LabelExpression*>(expr);
    auto alias = aliases.find(*labelExpr->name());
    DCHECK(alias != aliases.end());
    return rewrite(labelExpr);
}

Expression* MatchSolver::makeIndexFilter(const std::string& label,
                                         const MapExpression* map,
                                         QueryContext* qctx) {
    auto& items = map->items();
    Expression* root = new RelationalExpression(
        Expression::Kind::kRelEQ,
        new TagPropertyExpression(new std::string(label), new std::string(*items[0].first)),
        items[0].second->clone().release());
    for (auto i = 1u; i < items.size(); i++) {
        auto* left = root;
        auto* right = new RelationalExpression(
            Expression::Kind::kRelEQ,
            new TagPropertyExpression(new std::string(label), new std::string(*items[i].first)),
            items[i].second->clone().release());
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, right);
    }
    return qctx->objPool()->add(root);
}

Expression* MatchSolver::makeIndexFilter(const std::string& label,
                                         const std::string& alias,
                                         Expression* filter,
                                         QueryContext* qctx) {
    static const std::unordered_set<Expression::Kind> kinds = {
        Expression::Kind::kRelEQ,
        Expression::Kind::kRelLT,
        Expression::Kind::kRelLE,
        Expression::Kind::kRelGT,
        Expression::Kind::kRelGE,
    };

    std::vector<const Expression*> ands;
    auto kind = filter->kind();
    if (kinds.count(kind) == 1) {
        ands.emplace_back(filter);
    } else if (kind == Expression::Kind::kLogicalAnd) {
        auto* logic = static_cast<LogicalExpression*>(filter);
        ExpressionUtils::pullAnds(logic);
        for (auto& operand : logic->operands()) {
            ands.emplace_back(operand.get());
        }
    } else {
        return nullptr;
    }

    std::vector<Expression*> relationals;
    for (auto* item : ands) {
        if (kinds.count(item->kind()) != 1) {
            continue;
        }

        auto* binary = static_cast<const BinaryExpression*>(item);
        auto* left = binary->left();
        auto* right = binary->right();
        const LabelAttributeExpression* la = nullptr;
        const ConstantExpression* constant = nullptr;
        if (left->kind() == Expression::Kind::kLabelAttribute &&
            right->kind() == Expression::Kind::kConstant) {
            la = static_cast<const LabelAttributeExpression*>(left);
            constant = static_cast<const ConstantExpression*>(right);
        } else if (right->kind() == Expression::Kind::kLabelAttribute &&
                   left->kind() == Expression::Kind::kConstant) {
            la = static_cast<const LabelAttributeExpression*>(right);
            constant = static_cast<const ConstantExpression*>(left);
        } else {
            continue;
        }

        if (*la->left()->name() != alias) {
            continue;
        }

        const auto &value = la->right()->value();
        auto *tpExpr = new TagPropertyExpression(new std::string(label),
                                                 new std::string(value.getStr()));
        auto *newConstant = constant->clone().release();
        if (left->kind() == Expression::Kind::kLabelAttribute) {
            auto* rel = new RelationalExpression(item->kind(), tpExpr, newConstant);
            relationals.emplace_back(rel);
        } else {
            auto* rel = new RelationalExpression(item->kind(), newConstant, tpExpr);
            relationals.emplace_back(rel);
        }
    }

    if (relationals.empty()) {
        return nullptr;
    }

    auto* root = relationals[0];
    for (auto i = 1u; i < relationals.size(); i++) {
        auto* left = root;
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, relationals[i]);
    }

    return qctx->objPool()->add(root);
}

Status MatchSolver::buildFilter(const MatchClauseContext* mctx, SubPlan* plan) {
    if (mctx->where->filter == nullptr) {
        return Status::OK();
    }
    auto newFilter = mctx->where->filter->clone();
    auto rewriter = [mctx](const Expression* expr) {
        return MatchSolver::doRewrite(mctx->aliasesGenerated, expr);
    };
    RewriteMatchLabelVisitor visitor(std::move(rewriter));
    newFilter->accept(&visitor);
    auto cond = mctx->qctx->objPool()->add(newFilter.release());
    auto input = plan->root;
    auto *node = Filter::make(mctx->qctx, input, cond);
    node->setInputVar(input->outputVar());
    node->setColNames(input->colNames());
    plan->root = node;
    return Status::OK();
}

void MatchSolver::extractAndDedupVidColumn(QueryContext* qctx,
                                           Expression* initialExpr,
                                           PlanNode* dep,
                                           const std::string& inputVar,
                                           SubPlan& plan) {
    auto columns = qctx->objPool()->add(new YieldColumns);
    auto* var = qctx->symTable()->getVar(inputVar);
    Expression* vidExpr = initialExprOrEdgeDstExpr(initialExpr, var->colNames.back());
    columns->addColumn(new YieldColumn(vidExpr));
    auto project = Project::make(qctx, dep, columns);
    project->setInputVar(inputVar);
    project->setColNames({kVid});
    auto dedup = Dedup::make(qctx, project);
    dedup->setColNames({kVid});

    plan.root = dedup;
}

Expression* MatchSolver::initialExprOrEdgeDstExpr(Expression* initialExpr,
                                                  const std::string& vidCol) {
    if (initialExpr != nullptr) {
        return initialExpr;
    } else {
        return getEndVidInPath(vidCol);
    }
}

Expression* MatchSolver::getEndVidInPath(const std::string& colName) {
    // expr: __Project_2[-1] => path
    auto columnExpr = ExpressionUtils::inputPropExpr(colName);
    // expr: endNode(path) => vn
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(std::move(columnExpr));
    auto fn = std::make_unique<std::string>("endNode");
    auto endNode = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    // expr: en[_dst] => dst vid
    auto vidExpr = std::make_unique<ConstantExpression>(kVid);
    return new AttributeExpression(endNode.release(), vidExpr.release());
}

Expression* MatchSolver::getStartVidInPath(const std::string& colName) {
    // expr: __Project_2[0] => path
    auto columnExpr = ExpressionUtils::inputPropExpr(colName);
    // expr: startNode(path) => v1
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(std::move(columnExpr));
    auto fn = std::make_unique<std::string>("startNode");
    auto firstVertexExpr = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    // expr: v1[_vid] => vid
    return new AttributeExpression(firstVertexExpr.release(), new ConstantExpression(kVid));
}

PlanNode* MatchSolver::filtPathHasSameEdge(PlanNode* input,
                                           const std::string& column,
                                           QueryContext* qctx) {
    auto args = std::make_unique<ArgumentList>();
    args->addArgument(ExpressionUtils::inputPropExpr(column));
    auto fn = std::make_unique<std::string>("hasSameEdgeInPath");
    auto fnCall = std::make_unique<FunctionCallExpression>(fn.release(), args.release());
    auto falseConst = std::make_unique<ConstantExpression>(false);
    auto cond = std::make_unique<RelationalExpression>(
        Expression::Kind::kRelEQ, fnCall.release(), falseConst.release());
    auto filter = Filter::make(qctx, input, qctx->objPool()->add(cond.release()));
    filter->setColNames(input->colNames());
    return filter;
}

Status MatchSolver::appendFetchVertexPlan(const Expression* nodeFilter,
                                          const SpaceInfo& space,
                                          QueryContext* qctx,
                                          Expression* initialExpr,
                                          SubPlan& plan) {
    return appendFetchVertexPlan(
        nodeFilter, space, qctx, initialExpr, plan.root->outputVar(), plan);
}

Status MatchSolver::appendFetchVertexPlan(const Expression* nodeFilter,
                                          const SpaceInfo& space,
                                          QueryContext* qctx,
                                          Expression* initialExpr,
                                          std::string inputVar,
                                          SubPlan& plan) {
    // [Project && Dedup]
    extractAndDedupVidColumn(qctx, initialExpr, plan.root, inputVar, plan);
    auto srcExpr = ExpressionUtils::inputPropExpr(kVid);
    // [Get vertices]
    auto props = flattenTags(qctx, space);
    NG_RETURN_IF_ERROR(props);
    auto gv = GetVertices::make(qctx,
                                plan.root,
                                space.id,
                                qctx->objPool()->add(srcExpr.release()),
                                std::move(props).value(),
                                {});

    PlanNode* root = gv;
    if (nodeFilter != nullptr) {
        auto filter = qctx->objPool()->add(nodeFilter->clone().release());
        RewriteMatchLabelVisitor visitor(
            [](const Expression* expr) -> Expression *{
            DCHECK(expr->kind() == Expression::Kind::kLabelAttribute ||
                expr->kind() == Expression::Kind::kLabel);
            // filter prop
            if (expr->kind() == Expression::Kind::kLabelAttribute) {
                auto la = static_cast<const LabelAttributeExpression*>(expr);
                return new AttributeExpression(
                    new VertexExpression(), la->right()->clone().release());
            }
            // filter tag
            return new VertexExpression();
        });
        filter->accept(&visitor);
        root = Filter::make(qctx, root, filter);
    }
    // [Project]
    // Normalize all columns to one
    auto columns = qctx->objPool()->add(new YieldColumns);
    auto pathExpr = std::make_unique<PathBuildExpression>();
    pathExpr->add(std::make_unique<VertexExpression>());
    columns->addColumn(new YieldColumn(pathExpr.release()));
    plan.root = Project::make(qctx, root, columns);
    plan.root->setColNames({kPathStr});
    return Status::OK();
}

StatusOr<std::vector<storage::cpp2::VertexProp>> MatchSolver::flattenTags(QueryContext* qctx,
                                                                          const SpaceInfo& space) {
    // Get all tags in the space
    const auto allTagsResult = qctx->schemaMng()->getAllLatestVerTagSchema(space.id);
    NG_RETURN_IF_ERROR(allTagsResult);
    // allTags: std::unordered_map<TagID, std::shared_ptr<const meta::NebulaSchemaProvider>>
    const auto allTags = std::move(allTagsResult).value();

    std::vector<storage::cpp2::VertexProp> props;
    props.reserve(allTags.size());
    // Retrieve prop names of each tag and append "_tag" to the name list to query empty tags
    for (const auto& tag : allTags) {
        // tag: pair<TagID, std::shared_ptr<const meta::NebulaSchemaProvider>>
        std::vector<std::string> propNames;
        storage::cpp2::VertexProp vProp;

        const auto tagId = tag.first;
        vProp.set_tag(tagId);
        const auto tagSchema = tag.second;   // nebulaSchemaProvider
        for (size_t i = 0; i < tagSchema->getNumFields(); i++) {
            const auto propName = tagSchema->getFieldName(i);
            propNames.emplace_back(propName);
        }
        propNames.emplace_back(nebula::kTag);   // "_tag"
        vProp.set_props(std::move(propNames));
        props.emplace_back(std::move(vProp));
    }
    return props;
}

}  // namespace graph
}  // namespace nebula
