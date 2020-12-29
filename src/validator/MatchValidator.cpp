/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/MatchValidator.h"

#include "util/ExpressionUtils.h"
#include "visitor/RewriteMatchLabelVisitor.h"

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
    auto *sentence = static_cast<MatchSentence *>(sentence_);
    auto &clauses = sentence->clauses();

    std::unordered_map<std::string, AliasType> *aliasesUsed = nullptr;
    YieldColumns *prevYieldColumns = nullptr;
    auto retClauseCtx = getContext<ReturnClauseContext>();
    for (size_t i = 0; i < clauses.size(); ++i) {
        switch (clauses[i]->kind()) {
            case ReadingClause::Kind::kMatch: {
                auto *matchClause = static_cast<MatchClause *>(clauses[i].get());

                if (matchClause->isOptional()) {
                    return Status::SemanticError("OPTIONAL MATCH not supported");
                }

                auto matchClauseCtx = getContext<MatchClauseContext>();
                matchClauseCtx->aliasesUsed = aliasesUsed;
                NG_RETURN_IF_ERROR(validatePath(matchClause->path(), *matchClauseCtx));
                if (matchClause->where() != nullptr) {
                    auto whereClauseCtx = getContext<WhereClauseContext>();
                    whereClauseCtx->aliasesUsed = &matchClauseCtx->aliasesGenerated;
                    NG_RETURN_IF_ERROR(
                        validateFilter(matchClause->where()->filter(), *whereClauseCtx));
                    matchClauseCtx->where = std::move(whereClauseCtx);
                }

                if (aliasesUsed) {
                    NG_RETURN_IF_ERROR(
                        combineAliases(matchClauseCtx->aliasesGenerated, *aliasesUsed));
                }
                aliasesUsed = &matchClauseCtx->aliasesGenerated;

                if (i == clauses.size() - 1) {
                    retClauseCtx->aliasesUsed = aliasesUsed;
                    NG_RETURN_IF_ERROR(
                        validateReturn(sentence->ret(), matchClauseCtx.get(), *retClauseCtx));
                }
                matchCtx_->clauses.emplace_back(std::move(matchClauseCtx));

                break;
            }
            case ReadingClause::Kind::kUnwind: {
                auto *unwindClause = static_cast<UnwindClause *>(clauses[i].get());
                auto unwindClauseCtx = getContext<UnwindClauseContext>();
                unwindClauseCtx->aliasesUsed = aliasesUsed;
                NG_RETURN_IF_ERROR(validateUnwind(unwindClause, *unwindClauseCtx));

                if (aliasesUsed) {
                    NG_RETURN_IF_ERROR(
                        combineAliases(unwindClauseCtx->aliasesGenerated, *aliasesUsed));
                }
                aliasesUsed = &unwindClauseCtx->aliasesGenerated;
                if (prevYieldColumns) {
                    NG_RETURN_IF_ERROR(combineYieldColumns(
                        const_cast<YieldColumns *>(unwindClauseCtx->yieldColumns),
                        prevYieldColumns));
                }
                prevYieldColumns = const_cast<YieldColumns *>(unwindClauseCtx->yieldColumns);

                if (i == clauses.size() - 1) {
                    retClauseCtx->aliasesUsed = aliasesUsed;
                    NG_RETURN_IF_ERROR(
                        validateReturn(sentence->ret(), unwindClauseCtx.get(), *retClauseCtx));
                }
                matchCtx_->clauses.emplace_back(std::move(unwindClauseCtx));

                break;
            }
            case ReadingClause::Kind::kWith: {
                auto *withClause = static_cast<WithClause *>(clauses[i].get());
                auto withClauseCtx = getContext<WithClauseContext>();
                withClauseCtx->aliasesUsed = aliasesUsed;
                NG_RETURN_IF_ERROR(validateWith(withClause, *withClauseCtx));
                if (withClause->where() != nullptr) {
                    auto whereClauseCtx = getContext<WhereClauseContext>();
                    whereClauseCtx->aliasesUsed = &withClauseCtx->aliasesGenerated;
                    NG_RETURN_IF_ERROR(
                        validateFilter(withClause->where()->filter(), *whereClauseCtx));
                    withClauseCtx->where = std::move(whereClauseCtx);
                }

                aliasesUsed = &withClauseCtx->aliasesGenerated;
                prevYieldColumns = const_cast<YieldColumns *>(withClauseCtx->yieldColumns);

                if (i == clauses.size() - 1) {
                    retClauseCtx->aliasesUsed = aliasesUsed;
                    NG_RETURN_IF_ERROR(
                        validateReturn(sentence->ret(), withClauseCtx.get(), *retClauseCtx));
                }
                matchCtx_->clauses.emplace_back(std::move(withClauseCtx));

                break;
            }
        }
    }
    matchCtx_->clauses.emplace_back(std::move(retClauseCtx));

    return Status::OK();
}

Status MatchValidator::validatePath(const MatchPath *path,
                                    MatchClauseContext &matchClauseCtx) const {
    NG_RETURN_IF_ERROR(
        buildNodeInfo(path, matchClauseCtx.nodeInfos, matchClauseCtx.aliasesGenerated));
    NG_RETURN_IF_ERROR(
        buildEdgeInfo(path, matchClauseCtx.edgeInfos, matchClauseCtx.aliasesGenerated));
    NG_RETURN_IF_ERROR(buildPathExpr(path, matchClauseCtx));
    return Status::OK();
}

Status MatchValidator::buildPathExpr(const MatchPath *path,
                                     MatchClauseContext &matchClauseCtx) const {
    auto* pathAlias = path->alias();
    if (pathAlias == nullptr) {
        return Status::OK();
    }
    if (!matchClauseCtx.aliasesGenerated.emplace(*pathAlias, AliasType::kPath).second) {
        return Status::SemanticError("`%s': Redefined alias", pathAlias->c_str());
    }

    auto& nodeInfos = matchClauseCtx.nodeInfos;
    auto& edgeInfos = matchClauseCtx.edgeInfos;


    auto pathBuild = std::make_unique<PathBuildExpression>();
    for (size_t i = 0; i < edgeInfos.size(); ++i) {
        pathBuild->add(std::make_unique<VariablePropertyExpression>(
            new std::string(), new std::string(*nodeInfos[i].alias)));
        pathBuild->add(std::make_unique<VariablePropertyExpression>(
            new std::string(), new std::string(*edgeInfos[i].alias)));
    }
    pathBuild->add(std::make_unique<VariablePropertyExpression>(
        new std::string(), new std::string(*nodeInfos.back().alias)));
    matchClauseCtx.pathBuild = std::move(pathBuild);
    return Status::OK();
}

Status MatchValidator::buildNodeInfo(const MatchPath *path,
                                     std::vector<NodeInfo> &nodeInfos,
                                     std::unordered_map<std::string, AliasType> &aliases) const {
    auto *sm = qctx_->schemaMng();
    auto steps = path->steps();
    nodeInfos.resize(steps + 1);

    for (auto i = 0u; i <= steps; i++) {
        auto *node = path->node(i);
        auto *label = node->label();
        auto *alias = node->alias();
        auto *props = node->props();
        auto anonymous = false;
        if (label != nullptr) {
            auto tid = sm->toTagID(space_.id, *label);
            if (!tid.ok()) {
                return Status::SemanticError("`%s': Unknown tag", label->c_str());
            }
            nodeInfos[i].tid = tid.value();
        }
        if (alias == nullptr) {
            anonymous = true;
            alias = saveObject(new std::string(vctx_->anonVarGen()->getVar()));
        }
        if (!aliases.emplace(*alias, AliasType::kNode).second) {
            return Status::SemanticError("`%s': Redefined alias", alias->c_str());
        }
        Expression *filter = nullptr;
        if (props != nullptr) {
            auto result = makeSubFilter(*alias, props);
            NG_RETURN_IF_ERROR(result);
            filter = result.value();
        } else if (label != nullptr) {
            auto result = makeSubFilter(*alias, props, *label);
            NG_RETURN_IF_ERROR(result);
            filter = result.value();
        }
        nodeInfos[i].anonymous = anonymous;
        nodeInfos[i].label = label;
        nodeInfos[i].alias = alias;
        nodeInfos[i].props = props;
        nodeInfos[i].filter = filter;
    }

    return Status::OK();
}

Status MatchValidator::buildEdgeInfo(const MatchPath *path,
                                     std::vector<EdgeInfo> &edgeInfos,
                                     std::unordered_map<std::string, AliasType> &aliases) const {
    auto *sm = qctx_->schemaMng();
    auto steps = path->steps();
    edgeInfos.resize(steps);

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
        } else {
            const auto allEdgesResult =
                matchCtx_->qctx->schemaMng()->getAllVerEdgeSchema(space_.id);
            NG_RETURN_IF_ERROR(allEdgesResult);
            const auto allEdges = std::move(allEdgesResult).value();
            for (const auto &edgeSchema : allEdges) {
                edgeInfos[i].edgeTypes.emplace_back(edgeSchema.first);
                // TODO:
                // edgeInfos[i].types.emplace_back(*type);
            }
        }
        auto *stepRange = edge->range();
        if (stepRange != nullptr) {
            NG_RETURN_IF_ERROR(validateStepRange(stepRange));
            edgeInfos[i].range = stepRange;
        }
        if (alias == nullptr) {
            anonymous = true;
            alias = saveObject(new std::string(vctx_->anonVarGen()->getVar()));
        }
        if (!aliases.emplace(*alias, AliasType::kEdge).second) {
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

Status MatchValidator::validateFilter(const Expression *filter,
                                      WhereClauseContext &whereClauseCtx) const {
    whereClauseCtx.filter = ExpressionUtils::foldConstantExpr(filter);

    auto typeStatus = deduceExprType(whereClauseCtx.filter.get());
    NG_RETURN_IF_ERROR(typeStatus);
    auto type = typeStatus.value();
    if (type != Value::Type::BOOL && type != Value::Type::NULLVALUE &&
        type != Value::Type::__EMPTY__) {
        std::stringstream ss;
        ss << "`" << filter->toString() << "', expected Boolean, "
           << "but was `" << type << "'";
        return Status::SemanticError(ss.str());
    }

    NG_RETURN_IF_ERROR(validateAliases({whereClauseCtx.filter.get()}, whereClauseCtx.aliasesUsed));

    return Status::OK();
}

Status MatchValidator::validateReturn(MatchReturn *ret,
                                      const CypherClauseContextBase *cypherClauseCtx,
                                      ReturnClauseContext &retClauseCtx) const {
    auto kind = cypherClauseCtx->kind;
    if (kind != CypherClauseKind::kMatch && kind != CypherClauseKind::kUnwind &&
        kind != CypherClauseKind::kWith) {
        return Status::SemanticError("Must be a MATCH/UNWIND/WITH");
    }
    // `RETURN *': return all named nodes or edges
    YieldColumns *columns = nullptr;
    if (ret->isAll()) {
        auto makeColumn = [] (const std::string &name) {
            auto *expr = new LabelExpression(name);
            auto *alias = new std::string(name);
            return new YieldColumn(expr, alias);
        };
        if (kind == CypherClauseKind::kMatch) {
            auto matchClauseCtx = static_cast<const MatchClauseContext*>(cypherClauseCtx);

            columns = saveObject(new YieldColumns());
            auto steps = matchClauseCtx->edgeInfos.size();

            if (!matchClauseCtx->nodeInfos[0].anonymous) {
                columns->addColumn(makeColumn(*matchClauseCtx->nodeInfos[0].alias));
            }

            for (auto i = 0u; i < steps; i++) {
                if (!matchClauseCtx->edgeInfos[i].anonymous) {
                    columns->addColumn(makeColumn(*matchClauseCtx->edgeInfos[i].alias));
                }
                if (!matchClauseCtx->nodeInfos[i+1].anonymous) {
                    columns->addColumn(makeColumn(*matchClauseCtx->nodeInfos[i+1].alias));
                }
            }

            for (auto &aliasPair : matchClauseCtx->aliasesGenerated) {
                if (aliasPair.second == AliasType::kPath) {
                    columns->addColumn(makeColumn(aliasPair.first));
                }
            }
        } else if (kind == CypherClauseKind::kUnwind) {
            auto unwindClauseCtx = static_cast<const UnwindClauseContext*>(cypherClauseCtx);
            columns = saveObject(new YieldColumns());
            columns->addColumn(makeColumn(unwindClauseCtx->aliasesGenerated.begin()->first));
        } else {
            // kWith
            auto withClauseCtx = static_cast<const WithClauseContext*>(cypherClauseCtx);
            columns = saveObject(new YieldColumns());
            for (auto &aliasPair : withClauseCtx->aliasesGenerated) {
                columns->addColumn(makeColumn(aliasPair.first));
            }
        }

        if (columns->empty()) {
            return Status::SemanticError("`RETURN *' not allowed if there is no alias");
        }
    }

    if (columns == nullptr) {
        retClauseCtx.yieldColumns = ret->columns();
    } else {
        retClauseCtx.yieldColumns = columns;
    }
    // Check all referencing expressions are valid
    std::vector<const Expression*> exprs;
    exprs.reserve(retClauseCtx.yieldColumns->size());
    for (auto *col : retClauseCtx.yieldColumns->columns()) {
        exprs.push_back(col->expr());
    }
    NG_RETURN_IF_ERROR(validateAliases(exprs, retClauseCtx.aliasesUsed));

    retClauseCtx.distinct = ret->isDistinct();

    auto paginationCtx = getContext<PaginationContext>();
    NG_RETURN_IF_ERROR(validatePagination(ret->skip(), ret->limit(), *paginationCtx));
    retClauseCtx.pagination = std::move(paginationCtx);

    if (ret->orderFactors() != nullptr) {
        auto orderByCtx = getContext<OrderByClauseContext>();
        NG_RETURN_IF_ERROR(validateOrderBy(ret->orderFactors(), ret->columns(), *orderByCtx));
        retClauseCtx.order = std::move(orderByCtx);
    }

    return Status::OK();
}

Status MatchValidator::validateAliases(
    const std::vector<const Expression *> &exprs,
    const std::unordered_map<std::string, AliasType> *aliasesUsed) const {
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
            if (!aliasesUsed || aliasesUsed->count(*name) != 1) {
                return Status::SemanticError("Alias used but not defined: `%s'", name->c_str());
            }
        }
    }
    return Status::OK();
}

Status MatchValidator::validateStepRange(const MatchStepRange *range) const {
    auto min = range->min();
    auto max = range->max();
    if (min > max) {
        return Status::SemanticError(
            "Max hop must be greater equal than min hop: %ld vs. %ld", max, min);
    }
    if (max == std::numeric_limits<int64_t>::max()) {
        return Status::SemanticError("Not set maximum hop for variable length relationships");
    }
    if (min == 0) {
        return Status::SemanticError("Zero steps implementation coming soon.");
    }
    return Status::OK();
}

Status MatchValidator::validateWith(const WithClause *with,
                                    WithClauseContext &withClauseCtx) const {
    // Check all referencing expressions are valid
    std::vector<const Expression *> exprs;
    exprs.reserve(with->columns()->size());
    for (auto *col : with->columns()->columns()) {
        if (col->alias() == nullptr) {
            return Status::SemanticError("Expression in WITH must be aliased (use AS)");
        }
        if (!withClauseCtx.aliasesGenerated.emplace(*col->alias(), AliasType::kDefault).second) {
            return Status::SemanticError("`%s': Redefined alias", col->alias()->c_str());
        }
        exprs.push_back(col->expr());
    }
    withClauseCtx.yieldColumns = with->columns();
    NG_RETURN_IF_ERROR(validateAliases(exprs, withClauseCtx.aliasesUsed));

    withClauseCtx.distinct = with->isDistinct();

    auto paginationCtx = getContext<PaginationContext>();
    NG_RETURN_IF_ERROR(validatePagination(with->skip(), with->limit(), *paginationCtx));
    withClauseCtx.pagination = std::move(paginationCtx);

    if (with->orderFactors() != nullptr) {
        auto orderByCtx = getContext<OrderByClauseContext>();
        NG_RETURN_IF_ERROR(validateOrderBy(with->orderFactors(), with->columns(), *orderByCtx));
        withClauseCtx.order = std::move(orderByCtx);
    }

    return Status::OK();
}

Status MatchValidator::validateUnwind(const UnwindClause *unwind,
                                      UnwindClauseContext &unwindClauseCtx) const {
    if (unwind->alias() == nullptr) {
        return Status::SemanticError("Expression in UNWIND must be aliased (use AS)");
    }
    YieldColumns *columns = saveObject(new YieldColumns());
    auto *expr = unwind->expr()->clone().release();
    auto *alias = new std::string(*unwind->alias());
    columns->addColumn(new YieldColumn(expr, alias));
    NG_RETURN_IF_ERROR(
        validateAliases(std::vector<const Expression *>{expr}, unwindClauseCtx.aliasesUsed));

    unwindClauseCtx.yieldColumns = columns;

    if (!unwindClauseCtx.aliasesGenerated.emplace(*unwind->alias(), AliasType::kDefault).second) {
        return Status::SemanticError("`%s': Redefined alias", unwind->alias()->c_str());
    }

    return Status::OK();
}

StatusOr<Expression*>
MatchValidator::makeSubFilter(const std::string &alias,
                              const MapExpression *map,
                              const std::string& label) const {
    // Node has tag without property
    if (!label.empty() && map == nullptr) {
        auto *left = new ConstantExpression(label);

        auto* args = new ArgumentList();
        args->addArgument(std::make_unique<LabelExpression>(alias));
        auto *right = new FunctionCallExpression(
                    new std::string("tags"),
                    args);
        Expression *root = new RelationalExpression(Expression::Kind::kRelIn, left, right);

        return saveObject(root);
    }

    DCHECK(map != nullptr);
    auto &items = map->items();
    DCHECK(!items.empty());

    // TODO(dutor) Check if evaluable and evaluate
    if (items[0].second->kind() != Expression::Kind::kConstant) {
        return Status::SemanticError("Props must be constant: `%s'",
                items[0].second->toString().c_str());
    }
    Expression *root = new RelationalExpression(Expression::Kind::kRelEQ,
            new LabelAttributeExpression(
                new LabelExpression(alias),
                new ConstantExpression(*items[0].first)),
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
                new ConstantExpression(*items[i].first)),
            items[i].second->clone().release());
        root = new LogicalExpression(Expression::Kind::kLogicalAnd, left, right);
    }

    return saveObject(root);
}

Status MatchValidator::combineAliases(
    std::unordered_map<std::string, AliasType> &curAliases,
    const std::unordered_map<std::string, AliasType> &lastAliases) const {
    for (auto &aliasPair : lastAliases) {
        if (!curAliases.emplace(aliasPair).second) {
            return Status::SemanticError("`%s': Redefined alias", aliasPair.first.c_str());
        }
    }

    return Status::OK();
}

Status MatchValidator::combineYieldColumns(YieldColumns *yieldColumns,
                                           YieldColumns *prevYieldColumns) const {
    const auto &prevColumns = prevYieldColumns->columns();
    for (auto &column : prevColumns) {
        DCHECK(column->alias() != nullptr);
        auto *newColumn = new YieldColumn(
            new VariablePropertyExpression(new std::string(), new std::string(*column->alias())),
            new std::string(*column->alias()));
        yieldColumns->addColumn(newColumn);
    }

    return Status::OK();
}

Status MatchValidator::validatePagination(const Expression *skipExpr,
                                          const Expression *limitExpr,
                                          PaginationContext &paginationCtx) const {
    int64_t skip = 0;
    int64_t limit = std::numeric_limits<int64_t>::max();
    if (skipExpr != nullptr) {
        if (!evaluableExpr(skipExpr)) {
            return Status::SemanticError("SKIP should be instantly evaluable");
        }
        QueryExpressionContext ctx;
        auto value = const_cast<Expression *>(skipExpr)->eval(ctx);
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
        auto value = const_cast<Expression *>(limitExpr)->eval(ctx);
        if (!value.isInt()) {
            return Status::SemanticError("LIMIT should be of type integer");
        }
        if (value.getInt() < 0) {
            return Status::SemanticError("LIMIT should not be negative");
        }
        limit = value.getInt();
    }
    paginationCtx.skip = skip;
    paginationCtx.limit = limit;

    return Status::OK();
}

Status MatchValidator::validateOrderBy(const OrderFactors *factors,
                                          const YieldColumns *yieldColumns,
                                          OrderByClauseContext &orderByCtx) const {
    if (factors != nullptr) {
        std::vector<std::string> inputColList;
        inputColList.reserve(yieldColumns->columns().size());
        for (auto *col : yieldColumns->columns()) {
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

        for (auto &factor : factors->factors()) {
            if (factor->expr()->kind() != Expression::Kind::kLabel) {
                return Status::SemanticError("Only column name can be used as sort item");
            }
            auto *name = static_cast<const LabelExpression *>(factor->expr())->name();
            auto iter = inputColIndices.find(*name);
            if (iter == inputColIndices.end()) {
                return Status::SemanticError("Column `%s' not found", name->c_str());
            }
            orderByCtx.indexedOrderFactors.emplace_back(iter->second, factor->orderType());
        }
    }

    return Status::OK();
}
}   // namespace graph
}   // namespace nebula
