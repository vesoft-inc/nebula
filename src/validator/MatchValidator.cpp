/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/MatchValidator.h"
#include "visitor/RewriteMatchLabelVisitor.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {
MatchValidator::MatchValidator(Sentence *sentence, QueryContext *context)
    : TraversalValidator(sentence, context) {
    matchCtx_ = std::make_unique<MatchAstContext>();
    matchCtx_->sentence = sentence;
    matchCtx_->qctx = context;
}


AstContext* MatchValidator::getAstContext() {
    return matchCtx_.get();
}


Status MatchValidator::validateImpl() {
    auto *sentence = static_cast<MatchSentence*>(sentence_);
    auto &clauses = sentence->clauses();

    if (clauses.size() > 1) {
        return Status::SemanticError("Multi clause MATCH not supported");
    }

    if (!clauses[0]->isMatch()) {
        return Status::SemanticError("First clause must be a MATCH");
    }

    auto *matchClause = static_cast<MatchClause*>(clauses[0].get());

    if (matchClause->isOptional()) {
        return Status::SemanticError("OPTIONAL MATCH not supported");
    }

    NG_RETURN_IF_ERROR(validatePath(matchClause->path()));
    if (matchClause->where() != nullptr) {
        NG_RETURN_IF_ERROR(validateFilter(matchClause->where()->filter()));
    }
    NG_RETURN_IF_ERROR(validateReturn(sentence->ret()));
    return analyzeStartPoint();
}


Status MatchValidator::validatePath(const MatchPath *path) {
    auto *sm = qctx_->schemaMng();
    auto steps = path->steps();

    auto& nodeInfos = matchCtx_->nodeInfos;
    auto& edgeInfos = matchCtx_->edgeInfos;

    nodeInfos.resize(steps + 1);
    edgeInfos.resize(steps);
    for (auto i = 0u; i <= steps; i++) {
        auto *node = path->node(i);
        auto *label = node->label();
        auto *alias = node->alias();
        auto *props = node->props();
        auto anonymous = false;
        if (label != nullptr) {
            auto tid = sm->toTagID(space_.id, *label);
            if (!tid.ok()) {
                return Status::Error("`%s': Unknown tag", label->c_str());
            }
            nodeInfos[i].tid = tid.value();
        }
        if (alias == nullptr) {
            anonymous = true;
            alias = saveObject(new std::string(vctx_->anonVarGen()->getVar()));
        }
        if (!matchCtx_->aliases.emplace(*alias, kNode).second) {
            return Status::Error("`%s': Redefined alias", alias->c_str());
        }
        Expression *filter = nullptr;
        if (props != nullptr) {
            auto result = makeSubFilter(*alias, props);
            NG_RETURN_IF_ERROR(result);
            filter = result.value();
        }
        nodeInfos[i].anonymous = anonymous;
        nodeInfos[i].label = label;
        nodeInfos[i].alias = alias;
        nodeInfos[i].props = props;
        nodeInfos[i].filter = filter;
    }

    for (auto i = 0u; i < steps; i++) {
        auto *edge = path->edge(i);
        auto &types = edge->types();
        auto *alias = edge->alias();
        auto *props = edge->props();
        auto direction = edge->direction();
        auto anonymous = false;
        if (!types.empty()) {
            for (auto &type : types) {
                auto etype = sm->toEdgeType(space_.id, *type);
                if (!etype.ok()) {
                    return Status::SemanticError("`%s': Unknown edge type", type->c_str());
                }
                edgeInfos[i].edgeTypes.emplace_back(etype.value());
                edgeInfos[i].types.emplace_back(*type);
            }
        }
        auto *stepRange = edge->range();
        if (stepRange != nullptr) {
            if (stepRange->min() != stepRange->max() ||
                    stepRange->min() != 1) {
                return Status::SemanticError("Variable steps not supported");
            }
        }
        if (alias == nullptr) {
            anonymous = true;
            alias = saveObject(new std::string(vctx_->anonVarGen()->getVar()));
        }
        if (!matchCtx_->aliases.emplace(*alias, kEdge).second) {
            return Status::SemanticError("`%s': Redefined alias", alias->c_str());
        }
        Expression *filter = nullptr;
        if (props != nullptr) {
            auto result = makeSubFilter(*alias, props);
            NG_RETURN_IF_ERROR(result);
            filter = result.value();
        }
        edgeInfos[i].anonymous = anonymous;
        edgeInfos[i].direction = direction;
        edgeInfos[i].alias = alias;
        edgeInfos[i].props = props;
        edgeInfos[i].filter = filter;
    }

    return Status::OK();
}


Status MatchValidator::validateFilter(const Expression *filter) {
    // TODO: validate filter result type.
    matchCtx_->filter = ExpressionUtils::foldConstantExpr(filter);
    NG_RETURN_IF_ERROR(validateAliases({matchCtx_->filter.get()}));

    return Status::OK();
}


Status MatchValidator::validateReturn(MatchReturn *ret) {
    // `RETURN *': return all named nodes or edges
    YieldColumns *columns = nullptr;
    if (ret->isAll()) {
        auto makeColumn = [] (const std::string &name) {
            auto *expr = new LabelExpression(name);
            auto *alias = new std::string(name);
            return new YieldColumn(expr, alias);
        };

        columns = new YieldColumns();
        saveObject(columns);
        auto steps = matchCtx_->edgeInfos.size();

        if (!matchCtx_->nodeInfos[0].anonymous) {
            columns->addColumn(makeColumn(*matchCtx_->nodeInfos[0].alias));
        }

        for (auto i = 0u; i < steps; i++) {
            if (!matchCtx_->edgeInfos[i].anonymous) {
                columns->addColumn(makeColumn(*matchCtx_->edgeInfos[i].alias));
            }
            if (!matchCtx_->nodeInfos[i+1].anonymous) {
                columns->addColumn(makeColumn(*matchCtx_->nodeInfos[i+1].alias));
            }
        }

        if (columns->empty()) {
            return Status::SemanticError("`RETURN *' not allowed if there is no alias");
        }
    }

    if (columns == nullptr) {
        matchCtx_->yieldColumns = ret->columns();
    } else {
        matchCtx_->yieldColumns = columns;
    }
    // Check all referencing expressions are valid
    std::vector<const Expression*> exprs;
    exprs.reserve(matchCtx_->yieldColumns->size());
    for (auto *col : matchCtx_->yieldColumns->columns()) {
        exprs.push_back(col->expr());
    }
    NG_RETURN_IF_ERROR(validateAliases(exprs));

    auto *skipExpr = ret->skip();
    auto *limitExpr = ret->limit();
    int64_t skip = 0;
    int64_t limit = std::numeric_limits<int64_t>::max();
    if (skipExpr != nullptr) {
        if (!evaluableExpr(skipExpr)) {
            return Status::SemanticError("SKIP should be instantly evaluable");
        }
        QueryExpressionContext ctx;
        auto value = const_cast<Expression*>(skipExpr)->eval(ctx);
        if (!value.isInt()) {
            return Status::SemanticError("SKIP should be of type integer");
        }
        if (value.getInt() < 0) {
            return Status::SemanticError("SKIP should not be negative");
        }
        skip = value.getInt();
    }

    if (limitExpr != nullptr) {
        if (!evaluableExpr(limitExpr)) {
            return Status::SemanticError("SKIP should be instantly evaluable");
        }
        QueryExpressionContext ctx;
        auto value = const_cast<Expression*>(limitExpr)->eval(ctx);
        if (!value.isInt()) {
            return Status::SemanticError("LIMIT should be of type integer");
        }
        if (value.getInt() < 0) {
            return Status::SemanticError("LIMIT should not be negative");
        }
        limit = value.getInt();
    }
    matchCtx_->skip = skip;
    matchCtx_->limit = limit;

    if (ret->orderFactors() != nullptr) {
        std::vector<std::string> inputColList;
        inputColList.reserve(matchCtx_->yieldColumns->columns().size());
        for (auto *col : matchCtx_->yieldColumns->columns()) {
            if (col->alias() != nullptr) {
                inputColList.emplace_back(*col->alias());
            } else {
                inputColList.emplace_back(col->expr()->toString());
            }
        }
        std::unordered_map<std::string, size_t> inputColIndices;
        for (auto i = 0u; i < inputColList.size(); i++) {
            if (!inputColIndices.emplace(inputColList[i], i).second) {
                return Status::SemanticError("Duplicated columns not allowed: %s",
                        inputColList[i].c_str());
            }
        }

        auto *factors = ret->orderFactors();
        for (auto &factor : factors->factors()) {
            if (factor->expr()->kind() != Expression::Kind::kLabel) {
                return Status::SemanticError("Only column name can be used as sort item");
            }
            auto *name = static_cast<const LabelExpression*>(factor->expr())->name();
            auto iter = inputColIndices.find(*name);
            if (iter == inputColIndices.end()) {
                return Status::SemanticError("Column `%s' not found", name->c_str());
            }
            matchCtx_->indexedOrderFactors.emplace_back(iter->second, factor->orderType());
        }
    }

    return Status::OK();
}


Status MatchValidator::validateAliases(const std::vector<const Expression*> &exprs) const {
    static const std::unordered_set<Expression::Kind> kinds = {
        Expression::Kind::kLabel,
        Expression::Kind::kLabelAttribute
    };

    for (auto *expr : exprs) {
        auto refExprs = ExpressionUtils::collectAll(expr, kinds);
        if (refExprs.empty()) {
            continue;
        }
        for (auto *refExpr : refExprs) {
            auto kind = refExpr->kind();
            const std::string *name = nullptr;
            if (kind == Expression::Kind::kLabel) {
                name = static_cast<const LabelExpression*>(refExpr)->name();
            } else {
                DCHECK(kind == Expression::Kind::kLabelAttribute);
                name = static_cast<const LabelAttributeExpression*>(refExpr)->left()->name();
            }
            DCHECK(name != nullptr);
            if (matchCtx_->aliases.count(*name) != 1) {
                return Status::SemanticError("Alias used but not defined: `%s'", name->c_str());
            }
        }
    }
    return Status::OK();
}

Status MatchValidator::analyzeStartPoint() {
    return Status::OK();
}


StatusOr<Expression*>
MatchValidator::makeSubFilter(const std::string &alias,
                              const MapExpression *map) const {
    DCHECK(map != nullptr);
    auto &items = map->items();
    DCHECK(!items.empty());
    Expression *root = nullptr;

    // TODO(dutor) Check if evaluable and evaluate
    if (items[0].second->kind() != Expression::Kind::kConstant) {
        return Status::SemanticError("Props must be constant: `%s'",
                items[0].second->toString().c_str());
    }
    root = new RelationalExpression(Expression::Kind::kRelEQ,
            new LabelAttributeExpression(
                new LabelExpression(alias),
                new LabelExpression(*items[0].first)),
            items[0].second->clone().release());
    for (auto i = 1u; i < items.size(); i++) {
        if (items[i].second->kind() != Expression::Kind::kConstant) {
            return Status::SemanticError("Props must be constant: `%s'",
                    items[i].second->toString().c_str());
        }
        auto *left = root;
        auto *right = new RelationalExpression(Expression::Kind::kRelEQ,
            new LabelAttributeExpression(
                new LabelExpression(alias),
                new LabelExpression(*items[i].first)),
            items[i].second->clone().release());
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, right);
    }

    saveObject(root);

    return root;
}
}   // namespace graph
}   // namespace nebula
