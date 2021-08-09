/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/MatchValidator.h"

#include "planner/match/MatchSolver.h"
#include "util/ExpressionUtils.h"
#include "visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {
MatchValidator::MatchValidator(Sentence *sentence, QueryContext *context)
    : TraversalValidator(sentence, context) {
    matchCtx_ = getContext<MatchAstContext>();
}

AstContext *MatchValidator::getAstContext() {
    return matchCtx_.get();
}

Status MatchValidator::validateImpl() {
    auto *sentence = static_cast<MatchSentence *>(sentence_);
    auto &clauses = sentence->clauses();

    std::unordered_map<std::string, AliasType> *aliasesUsed = nullptr;
    YieldColumns *prevYieldColumns = nullptr;
    auto retClauseCtx = getContext<ReturnClauseContext>();
    auto retYieldCtx = getContext<YieldClauseContext>();
    retClauseCtx->yield = std::move(retYieldCtx);

    for (size_t i = 0; i < clauses.size(); ++i) {
        auto kind = clauses[i]->kind();
        if (i > 0 && kind == ReadingClause::Kind::kMatch) {
            return Status::SemanticError(
                "Match clause is not supported to be followed by other cypher clauses");
        }
        switch (kind) {
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

                matchCtx_->clauses.emplace_back(std::move(matchClauseCtx));

                break;
            }
            case ReadingClause::Kind::kUnwind: {
                auto *unwindClause = static_cast<UnwindClause *>(clauses[i].get());
                auto unwindClauseCtx = getContext<UnwindClauseContext>();
                unwindClauseCtx->aliasesUsed = aliasesUsed;
                NG_RETURN_IF_ERROR(validateUnwind(unwindClause, *unwindClauseCtx));

                aliasesUsed = unwindClauseCtx->aliasesUsed;

                matchCtx_->clauses.emplace_back(std::move(unwindClauseCtx));

                // TODO: delete prevYieldColumns
                UNUSED(prevYieldColumns);
                break;
            }
            case ReadingClause::Kind::kWith: {
                auto *withClause = static_cast<WithClause *>(clauses[i].get());
                auto withClauseCtx = getContext<WithClauseContext>();
                auto withYieldCtx = getContext<YieldClauseContext>();
                withClauseCtx->yield = std::move(withYieldCtx);
                withClauseCtx->yield->aliasesUsed = aliasesUsed;
                NG_RETURN_IF_ERROR(validateWith(
                    withClause,
                    matchCtx_->clauses.empty() ? nullptr : matchCtx_->clauses.back().get(),
                    *withClauseCtx));
                if (withClause->where() != nullptr) {
                    auto whereClauseCtx = getContext<WhereClauseContext>();
                    whereClauseCtx->aliasesUsed = &withClauseCtx->aliasesGenerated;
                    NG_RETURN_IF_ERROR(
                        validateFilter(withClause->where()->filter(), *whereClauseCtx));
                    withClauseCtx->where = std::move(whereClauseCtx);
                }

                aliasesUsed = &withClauseCtx->aliasesGenerated;
                prevYieldColumns = const_cast<YieldColumns *>(withClauseCtx->yield->yieldColumns);

                matchCtx_->clauses.emplace_back(std::move(withClauseCtx));

                break;
            }
        }
    }

    retClauseCtx->yield->aliasesUsed = aliasesUsed;
    NG_RETURN_IF_ERROR(
        validateReturn(sentence->ret(), matchCtx_->clauses.back().get(), *retClauseCtx));

    NG_RETURN_IF_ERROR(buildOutputs(retClauseCtx->yield->yieldColumns));
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
    auto *pathAlias = path->alias();
    if (pathAlias == nullptr) {
        return Status::OK();
    }
    if (!matchClauseCtx.aliasesGenerated.emplace(*pathAlias, AliasType::kPath).second) {
        return Status::SemanticError("`%s': Redefined alias", pathAlias->c_str());
    }

    auto &nodeInfos = matchClauseCtx.nodeInfos;
    auto &edgeInfos = matchClauseCtx.edgeInfos;

    auto *pool = qctx_->objPool();
    auto pathBuild = PathBuildExpression::make(pool);
    for (size_t i = 0; i < edgeInfos.size(); ++i) {
        pathBuild->add(VariablePropertyExpression::make(pool, "", nodeInfos[i].alias));
        pathBuild->add(VariablePropertyExpression::make(pool, "", edgeInfos[i].alias));
    }
    pathBuild->add(VariablePropertyExpression::make(pool, "", nodeInfos.back().alias));
    matchClauseCtx.pathBuild = std::move(pathBuild);
    return Status::OK();
}

Status MatchValidator::buildNodeInfo(const MatchPath *path,
                                     std::vector<NodeInfo> &nodeInfos,
                                     std::unordered_map<std::string, AliasType> &aliases) const {
    auto *sm = qctx_->schemaMng();
    auto steps = path->steps();
    auto *pool = qctx_->objPool();
    nodeInfos.resize(steps + 1);

    for (auto i = 0u; i <= steps; i++) {
        auto *node = path->node(i);
        auto alias = node->alias();
        auto *props = node->props();
        auto anonymous = false;
        if (node->labels() != nullptr) {
            auto &labels = node->labels()->labels();
            for (const auto &label : labels) {
                if (label != nullptr) {
                    auto tid = sm->toTagID(space_.id, *label->label());
                    if (!tid.ok()) {
                        return Status::SemanticError("`%s': Unknown tag", label->label()->c_str());
                    }
                    nodeInfos[i].tids.emplace_back(tid.value());
                    nodeInfos[i].labels.emplace_back(label->label());
                    nodeInfos[i].labelProps.emplace_back(label->props());
                }
            }
        }
        if (alias.empty()) {
            anonymous = true;
            alias = vctx_->anonVarGen()->getVar();
        }
        if (!aliases.emplace(alias, AliasType::kNode).second) {
            return Status::SemanticError("`%s': Redefined alias", alias.c_str());
        }
        Expression *filter = nullptr;
        if (props != nullptr) {
            auto result = makeSubFilter(alias, props);
            NG_RETURN_IF_ERROR(result);
            filter = result.value();
        } else if (node->labels() != nullptr && !node->labels()->labels().empty()) {
            const auto &labels = node->labels()->labels();
            for (const auto &label : labels) {
                auto result = makeSubFilter(alias, label->props(), *label->label());
                NG_RETURN_IF_ERROR(result);
                filter = andConnect(pool, filter, result.value());
            }
        }
        nodeInfos[i].anonymous = anonymous;
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
        auto alias = edge->alias();
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
                edgeInfos[i].types.emplace_back(type.get());
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
        if (alias.empty()) {
            anonymous = true;
            alias = vctx_->anonVarGen()->getVar();
        }
        if (!aliases.emplace(alias, AliasType::kEdge).second) {
            return Status::SemanticError("`%s': Redefined alias", alias.c_str());
        }
        Expression *filter = nullptr;
        if (props != nullptr) {
            auto result = makeSubFilter(alias, props);
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
    auto transformRes =  ExpressionUtils::filterTransform(filter);
    NG_RETURN_IF_ERROR(transformRes);
    whereClauseCtx.filter = transformRes.value();

    auto typeStatus = deduceExprType(whereClauseCtx.filter);
    NG_RETURN_IF_ERROR(typeStatus);
    auto type = typeStatus.value();
    if (type != Value::Type::BOOL && type != Value::Type::NULLVALUE &&
        type != Value::Type::__EMPTY__) {
        std::stringstream ss;
        ss << "`" << filter->toString() << "', expected Boolean, "
           << "but was `" << type << "'";
        return Status::SemanticError(ss.str());
    }

    NG_RETURN_IF_ERROR(validateAliases({whereClauseCtx.filter}, whereClauseCtx.aliasesUsed));

    return Status::OK();
}

Status MatchValidator::includeExisting(const CypherClauseContextBase *cypherClauseCtx,
                                       YieldColumns *columns) const {
    if (cypherClauseCtx == nullptr) {
        return Status::OK();
    }
    auto kind = cypherClauseCtx->kind;
    if (kind != CypherClauseKind::kMatch && kind != CypherClauseKind::kUnwind &&
        kind != CypherClauseKind::kWith) {
        return Status::SemanticError("Must be a MATCH/UNWIND/WITH");
    }
    auto *pool = qctx_->objPool();
    auto makeColumn = [&pool](const std::string &name) {
        auto *expr = LabelExpression::make(pool, name);
        return new YieldColumn(expr, name);
    };
    if (kind == CypherClauseKind::kMatch) {
        auto matchClauseCtx = static_cast<const MatchClauseContext *>(cypherClauseCtx);

        auto steps = matchClauseCtx->edgeInfos.size();

        if (!matchClauseCtx->nodeInfos[0].anonymous) {
            columns->addColumn(makeColumn(matchClauseCtx->nodeInfos[0].alias));
        }

        for (auto i = 0u; i < steps; i++) {
            if (!matchClauseCtx->edgeInfos[i].anonymous) {
                columns->addColumn(makeColumn(matchClauseCtx->edgeInfos[i].alias));
            }
            if (!matchClauseCtx->nodeInfos[i + 1].anonymous) {
                columns->addColumn(makeColumn(matchClauseCtx->nodeInfos[i + 1].alias));
            }
        }

        for (auto &aliasPair : matchClauseCtx->aliasesGenerated) {
            if (aliasPair.second == AliasType::kPath) {
                columns->addColumn(makeColumn(aliasPair.first));
            }
        }
    } else if (kind == CypherClauseKind::kUnwind) {
        auto unwindClauseCtx = static_cast<const UnwindClauseContext *>(cypherClauseCtx);
        columns->addColumn(makeColumn(unwindClauseCtx->alias));
    } else {
        // kWith
        auto withClauseCtx = static_cast<const WithClauseContext *>(cypherClauseCtx);
        for (auto &aliasPair : withClauseCtx->aliasesGenerated) {
            columns->addColumn(makeColumn(aliasPair.first));
        }
    }

    return Status::OK();
}

Status MatchValidator::validateReturn(MatchReturn *ret,
                                      const CypherClauseContextBase *cypherClauseCtx,
                                      ReturnClauseContext &retClauseCtx) const {
    YieldColumns *columns = saveObject(new YieldColumns());
    if (ret->returnItems()->includeExisting()) {
        auto status = includeExisting(cypherClauseCtx, columns);
        if (!status.ok()) {
            return status;
        }
        if (columns->empty() && !ret->returnItems()->columns()) {
            return Status::SemanticError(
                "RETURN * is not allowed when there are no variables in scope");
        }
    }
    if (ret->returnItems()->columns()) {
        for (auto *column : ret->returnItems()->columns()->columns()) {
            columns->addColumn(column->clone().release());
        }
    }
    DCHECK(!columns->empty());
    retClauseCtx.yield->yieldColumns = columns;

    // Check all referencing expressions are valid
    std::vector<const Expression *> exprs;
    exprs.reserve(retClauseCtx.yield->yieldColumns->size());
    for (auto *col : retClauseCtx.yield->yieldColumns->columns()) {
        if (!retClauseCtx.yield->hasAgg_ &&
            ExpressionUtils::hasAny(col->expr(), {Expression::Kind::kAggregate})) {
            retClauseCtx.yield->hasAgg_ = true;
        }
        exprs.push_back(col->expr());
    }
    NG_RETURN_IF_ERROR(validateAliases(exprs, retClauseCtx.yield->aliasesUsed));
    NG_RETURN_IF_ERROR(validateYield(*retClauseCtx.yield));

    retClauseCtx.yield->distinct = ret->isDistinct();

    auto paginationCtx = getContext<PaginationContext>();
    NG_RETURN_IF_ERROR(validatePagination(ret->skip(), ret->limit(), *paginationCtx));
    retClauseCtx.pagination = std::move(paginationCtx);

    if (ret->orderFactors() != nullptr) {
        auto orderByCtx = getContext<OrderByClauseContext>();
        NG_RETURN_IF_ERROR(
            validateOrderBy(ret->orderFactors(), retClauseCtx.yield->yieldColumns, *orderByCtx));
        retClauseCtx.order = std::move(orderByCtx);
    }

    return Status::OK();
}

Status MatchValidator::validateAliases(
    const std::vector<const Expression *> &exprs,
    const std::unordered_map<std::string, AliasType> *aliasesUsed) const {
    static const std::unordered_set<Expression::Kind> kinds = {Expression::Kind::kLabel,
                                                               Expression::Kind::kLabelAttribute,
                                                               // primitive props
                                                               Expression::Kind::kEdgeSrc,
                                                               Expression::Kind::kEdgeDst,
                                                               Expression::Kind::kEdgeRank,
                                                               Expression::Kind::kEdgeType};

    for (auto *expr : exprs) {
        auto refExprs = ExpressionUtils::collectAll(expr, kinds);
        if (refExprs.empty()) {
            continue;
        }
        for (auto *refExpr : refExprs) {
            NG_RETURN_IF_ERROR(checkAlias(refExpr, aliasesUsed));
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
        return Status::SemanticError("Cannot set maximum hop for variable length relationships");
    }
    if (min < 0) {
        return Status::SemanticError(
            "Cannot set negative steps minumum hop for variable length relationships");
    }
    return Status::OK();
}

Status MatchValidator::validateWith(const WithClause *with,
                                    const CypherClauseContextBase *cypherClauseCtx,
                                    WithClauseContext &withClauseCtx) const {
    YieldColumns *columns = saveObject(new YieldColumns());
    if (with->returnItems()->includeExisting()) {
        auto status = includeExisting(cypherClauseCtx, columns);
        if (!status.ok()) {
            return status;
        }
    }
    if (with->returnItems()->columns()) {
        for (auto* column : with->returnItems()->columns()->columns()) {
            columns->addColumn(column->clone().release());
        }
    }
    withClauseCtx.yield->yieldColumns = columns;

    // Check all referencing expressions are valid
    std::vector<const Expression *> exprs;
    exprs.reserve(withClauseCtx.yield->yieldColumns->size());
    for (auto *col : withClauseCtx.yield->yieldColumns->columns()) {
        auto labelExprs = ExpressionUtils::collectAll(col->expr(), {Expression::Kind::kLabel});
        for (auto *labelExpr : labelExprs) {
            DCHECK_EQ(labelExpr->kind(), Expression::Kind::kLabel);
            auto label = static_cast<const LabelExpression *>(labelExpr)->name();
            if (!withClauseCtx.yield->aliasesUsed ||
                !withClauseCtx.yield->aliasesUsed->count(label)) {
                return Status::SemanticError("Variable `%s` not defined", label.c_str());
            }
        }
        if (col->alias().empty()) {
            if (col->expr()->kind() == Expression::Kind::kLabel) {
                col->setAlias(col->toString());
            } else {
                return Status::SemanticError("Expression in WITH must be aliased (use AS)");
            }
        }
        if (!withClauseCtx.aliasesGenerated.emplace(col->alias(), AliasType::kDefault).second) {
            return Status::SemanticError("`%s': Redefined alias", col->alias().c_str());
        }
        if (!withClauseCtx.yield->hasAgg_ &&
            ExpressionUtils::hasAny(col->expr(), {Expression::Kind::kAggregate})) {
            withClauseCtx.yield->hasAgg_ = true;
        }
        exprs.push_back(col->expr());
    }


    NG_RETURN_IF_ERROR(validateAliases(exprs, withClauseCtx.yield->aliasesUsed));
    NG_RETURN_IF_ERROR(validateYield(*withClauseCtx.yield));

    withClauseCtx.yield->distinct = with->isDistinct();

    auto paginationCtx = getContext<PaginationContext>();
    NG_RETURN_IF_ERROR(validatePagination(with->skip(), with->limit(), *paginationCtx));
    withClauseCtx.pagination = std::move(paginationCtx);

    if (with->orderFactors() != nullptr) {
        auto orderByCtx = getContext<OrderByClauseContext>();
        NG_RETURN_IF_ERROR(
            validateOrderBy(with->orderFactors(), withClauseCtx.yield->yieldColumns, *orderByCtx));
        withClauseCtx.order = std::move(orderByCtx);
    }

    return Status::OK();
}

Status MatchValidator::validateUnwind(const UnwindClause *unwindClause,
                                      UnwindClauseContext &unwindCtx) const {
    if (unwindClause->alias().empty()) {
        return Status::SemanticError("Expression in UNWIND must be aliased (use AS)");
    }
    unwindCtx.alias = unwindClause->alias();
    unwindCtx.unwindExpr = unwindClause->expr()->clone();

    auto labelExprs = ExpressionUtils::collectAll(unwindCtx.unwindExpr, {Expression::Kind::kLabel});
    for (auto *labelExpr : labelExprs) {
        DCHECK_EQ(labelExpr->kind(), Expression::Kind::kLabel);
        auto label = static_cast<const LabelExpression *>(labelExpr)->name();
        if (!unwindCtx.aliasesUsed || !unwindCtx.aliasesUsed->count(label)) {
            return Status::SemanticError("Variable `%s` not defined", label.c_str());
        }
    }
    unwindCtx.aliasesGenerated.emplace(unwindCtx.alias, AliasType::kDefault);
    if (!unwindCtx.aliasesUsed) {
        unwindCtx.aliasesUsed = &unwindCtx.aliasesGenerated;
    } else if (!unwindCtx.aliasesUsed->emplace(unwindCtx.alias, AliasType::kDefault).second) {
        return Status::SemanticError("Variable `%s` already declared", unwindCtx.alias.c_str());
    }

    return Status::OK();
}

StatusOr<Expression *> MatchValidator::makeSubFilter(const std::string &alias,
                                                     const MapExpression *map,
                                                     const std::string &label) const {
    auto *pool = qctx_->objPool();
    // Node has tag without property
    if (!label.empty() && map == nullptr) {
        auto *left = ConstantExpression::make(pool, label);

        auto *args = ArgumentList::make(pool);
        args->addArgument(LabelExpression::make(pool, alias));
        auto *right = FunctionCallExpression::make(pool, "tags", args);
        Expression *root = RelationalExpression::makeIn(pool, left, right);

        return root;
    }

    DCHECK(map != nullptr);
    auto &items = map->items();
    DCHECK(!items.empty());

    // TODO(dutor) Check if evaluable and evaluate
    if (items[0].second->kind() != Expression::Kind::kConstant) {
        return Status::SemanticError("Props must be constant: `%s'",
                                     items[0].second->toString().c_str());
    }
    Expression *root = RelationalExpression::makeEQ(
        pool,
        LabelAttributeExpression::make(pool,
                                       LabelExpression::make(pool, alias),
                                       ConstantExpression::make(pool, items[0].first)),
        items[0].second->clone());
    for (auto i = 1u; i < items.size(); i++) {
        if (items[i].second->kind() != Expression::Kind::kConstant) {
            return Status::SemanticError("Props must be constant: `%s'",
                                         items[i].second->toString().c_str());
        }
        auto *left = root;
        auto *right = RelationalExpression::makeEQ(
            pool,
            LabelAttributeExpression::make(pool,
                                           LabelExpression::make(pool, alias),
                                           ConstantExpression::make(pool, items[i].first)),
            items[i].second->clone());
        root = LogicalExpression::makeAnd(pool, left, right);
    }
    return root;
}

/*static*/ Expression *MatchValidator::andConnect(ObjectPool *pool,
                                                  Expression *left,
                                                  Expression *right) {
    if (left == nullptr) {
        return right;
    }
    if (right == nullptr) {
        return left;
    }
    return LogicalExpression::makeAnd(pool, left, right);
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
    auto *pool = qctx_->objPool();
    const auto &prevColumns = prevYieldColumns->columns();
    for (auto &column : prevColumns) {
        DCHECK(!column->alias().empty());
        auto *newColumn = new YieldColumn(
            VariablePropertyExpression::make(pool, "", column->alias()), column->alias());
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
            inputColList.emplace_back(col->name());
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
            auto &name = static_cast<const LabelExpression *>(factor->expr())->name();
            auto iter = inputColIndices.find(name);
            if (iter == inputColIndices.end()) {
                return Status::SemanticError("Column `%s' not found", name.c_str());
            }
            orderByCtx.indexedOrderFactors.emplace_back(iter->second, factor->orderType());
        }
    }

    return Status::OK();
}

Status MatchValidator::validateGroup(YieldClauseContext &yieldCtx) const {
    auto cols = yieldCtx.yieldColumns->columns();
    auto *pool = qctx_->objPool();

    DCHECK(!cols.empty());
    for (auto *col : cols) {
        auto *colExpr = col->expr();
        auto colOldName = col->name();
        if (colExpr->kind() != Expression::Kind::kAggregate) {
            auto collectAggCol = colExpr->clone();
            auto aggs =
                ExpressionUtils::collectAll(collectAggCol, {Expression::Kind::kAggregate});
            for (auto *agg : aggs) {
                DCHECK_EQ(agg->kind(), Expression::Kind::kAggregate);
                if (!ExpressionUtils::checkAggExpr(static_cast<const AggregateExpression *>(agg))
                         .ok()) {
                    return Status::SemanticError("Aggregate function nesting is not allowed: `%s'",
                                                 colExpr->toString().c_str());
                }

                yieldCtx.groupItems_.emplace_back(agg->clone());

                yieldCtx.needGenProject_ = true;
                yieldCtx.aggOutputColumnNames_.emplace_back(agg->toString());
            }
            if (!aggs.empty()) {
                auto *rewritedExpr = ExpressionUtils::rewriteAgg2VarProp(colExpr);
                yieldCtx.projCols_->addColumn(new YieldColumn(rewritedExpr, colOldName));
                yieldCtx.projOutputColumnNames_.emplace_back(colOldName);
                continue;
            }
        }

        if (colExpr->kind() == Expression::Kind::kAggregate) {
            auto *aggExpr = static_cast<AggregateExpression *>(colExpr);
            NG_RETURN_IF_ERROR(ExpressionUtils::checkAggExpr(aggExpr));
        } else if (!ExpressionUtils::isEvaluableExpr(colExpr)) {
            yieldCtx.groupKeys_.emplace_back(colExpr);
        }

        yieldCtx.groupItems_.emplace_back(colExpr);

        yieldCtx.projCols_->addColumn(
            new YieldColumn(VariablePropertyExpression::make(pool, "", colOldName), colOldName));
        yieldCtx.projOutputColumnNames_.emplace_back(colOldName);
        yieldCtx.aggOutputColumnNames_.emplace_back(colOldName);
    }

    return Status::OK();
}

Status MatchValidator::validateYield(YieldClauseContext &yieldCtx) const {
    auto cols = yieldCtx.yieldColumns->columns();
    if (cols.empty()) {
        return Status::OK();
    }

    yieldCtx.projCols_ = yieldCtx.qctx->objPool()->add(new YieldColumns());
    if (!yieldCtx.hasAgg_) {
        for (auto &col : yieldCtx.yieldColumns->columns()) {
            yieldCtx.projCols_->addColumn(col->clone().release());
            yieldCtx.projOutputColumnNames_.emplace_back(col->name());
        }
        return Status::OK();
    } else {
        return validateGroup(yieldCtx);
    }
}

StatusOr<AliasType> MatchValidator::getAliasType(
    const std::unordered_map<std::string, AliasType> *aliasesUsed,
    const std::string &name) const {
    auto iter = aliasesUsed->find(name);
    if (iter == aliasesUsed->end()) {
        return Status::SemanticError("Alias used but not defined: `%s'", name.c_str());
    }
    return iter->second;
}

Status MatchValidator::checkAlias(
    const Expression *refExpr,
    const std::unordered_map<std::string, AliasType> *aliasesUsed) const {
    auto kind = refExpr->kind();
    AliasType aliasType = AliasType::kDefault;

    switch (kind) {
        case Expression::Kind::kLabel: {
            auto name = static_cast<const LabelExpression *>(refExpr)->name();
            auto res = getAliasType(aliasesUsed, name);
            if (!res.ok()) {
                return res.status();
            }
            return Status::OK();
        }
        case Expression::Kind::kLabelAttribute: {
            auto name = static_cast<const LabelAttributeExpression *>(refExpr)->left()->name();
            auto res = getAliasType(aliasesUsed, name);
            if (!res.ok()) {
                return res.status();
            }
            return Status::OK();
        }
        case Expression::Kind::kEdgeSrc: {
            auto name = static_cast<const EdgeSrcIdExpression *>(refExpr)->sym();
            auto res = getAliasType(aliasesUsed, name);
            if (!res.ok()) {
                return res.status();
            }
            aliasType = res.value();
            switch (aliasType) {
                case AliasType::kNode:
                    return Status::SemanticError("Vertex `%s' does not have the src attribute",
                                                 name.c_str());
                case AliasType::kEdge:
                    return Status::SemanticError("To get the src vid of the edge, use src(%s)",
                                                 name.c_str());
                case AliasType::kPath:
                    return Status::SemanticError(
                        "To get the start node of the path, use startNode(%s)", name.c_str());
                default:
                    return Status::SemanticError("Alias `%s' does not have the edge property src",
                                                 name.c_str());
            }
        }
        case Expression::Kind::kEdgeDst: {
            auto name = static_cast<const EdgeDstIdExpression *>(refExpr)->sym();
            auto res = getAliasType(aliasesUsed, name);
            if (!res.ok()) {
                return res.status();
            }
            aliasType = res.value();
            switch (aliasType) {
                case AliasType::kNode:
                    return Status::SemanticError("Vertex `%s' does not have the dst attribute",
                                                 name.c_str());
                case AliasType::kEdge:
                    return Status::SemanticError("To get the dst vid of the edge, use dst(%s)",
                                                 name.c_str());
                case AliasType::kPath:
                    return Status::SemanticError("To get the end node of the path, use endNode(%s)",
                                                 name.c_str());
                default:
                    return Status::SemanticError("Alias `%s' does not have the edge property dst",
                                                 name.c_str());
            }
        }
        case Expression::Kind::kEdgeRank: {
            auto name = static_cast<const EdgeRankExpression *>(refExpr)->sym();
            auto res = getAliasType(aliasesUsed, name);
            if (!res.ok()) {
                return res.status();
            }
            aliasType = res.value();
            switch (aliasType) {
                case AliasType::kNode:
                    return Status::SemanticError("Vertex `%s' does not have the ranking attribute",
                                                 name.c_str());
                case AliasType::kEdge:
                    return Status::SemanticError("To get the ranking of the edge, use rank(%s)",
                                                 name.c_str());
                case AliasType::kPath:
                    return Status::SemanticError("Path `%s' does not have the ranking attribute",
                                                 name.c_str());
                default:
                    return Status::SemanticError(
                        "Alias `%s' does not have the edge property ranking", name.c_str());
            }
        }
        case Expression::Kind::kEdgeType: {
            auto name = static_cast<const EdgeTypeExpression *>(refExpr)->sym();
            auto res = getAliasType(aliasesUsed, name);
            if (!res.ok()) {
                return res.status();
            }
            aliasType = res.value();
            switch (aliasType) {
                case AliasType::kNode:
                    return Status::SemanticError("Vertex `%s' does not have the type attribute",
                                                 name.c_str());
                case AliasType::kEdge:
                    return Status::SemanticError("To get the type of the edge, use type(%s)",
                                                 name.c_str());
                case AliasType::kPath:
                    return Status::SemanticError("Path `%s' does not have the type attribute",
                                                 name.c_str());
                default:
                    return Status::SemanticError(
                        "Alias `%s' does not have the edge property ranking", name.c_str());
            }
        }
        default:   // refExpr must satisfy one of cases and should never hit this branch
            break;
    }
    return Status::SemanticError("Invalid expression `%s' does not contain alias",
                                 refExpr->toString().c_str());
}

Status MatchValidator::buildOutputs(const YieldColumns *yields) {
    for (auto *col : yields->columns()) {
        auto colName = col->name();
        auto typeStatus = deduceExprType(col->expr());
        NG_RETURN_IF_ERROR(typeStatus);
        auto type = typeStatus.value();
        outputs_.emplace_back(colName, type);
    }
    return Status::OK();
}
}   // namespace graph
}   // namespace nebula
