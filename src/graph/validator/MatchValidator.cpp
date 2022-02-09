/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/validator/MatchValidator.h"

#include "graph/planner/match/MatchSolver.h"
#include "graph/util/ExpressionUtils.h"
#include "graph/visitor/RewriteVisitor.h"

namespace nebula {
namespace graph {
MatchValidator::MatchValidator(Sentence *sentence, QueryContext *context)
    : Validator(sentence, context) {
  cypherCtx_ = getContext<CypherContext>();
}

AstContext *MatchValidator::getAstContext() {
  return cypherCtx_.get();
}

Status MatchValidator::validateImpl() {
  auto *sentence = static_cast<MatchSentence *>(sentence_);
  auto &clauses = sentence->clauses();

  std::unordered_map<std::string, AliasType> aliasesAvailable;
  auto retClauseCtx = getContext<ReturnClauseContext>();
  auto retYieldCtx = getContext<YieldClauseContext>();
  retClauseCtx->yield = std::move(retYieldCtx);

  cypherCtx_->queryParts.emplace_back();
  for (size_t i = 0; i < clauses.size(); ++i) {
    auto kind = clauses[i]->kind();
    switch (kind) {
      case ReadingClause::Kind::kMatch: {
        auto *matchClause = static_cast<MatchClause *>(clauses[i].get());

        auto matchClauseCtx = getContext<MatchClauseContext>();
        matchClauseCtx->isOptional = matchClause->isOptional();

        matchClauseCtx->aliasesAvailable = aliasesAvailable;
        for (size_t j = 0; j < matchClause->path()->pathSize(); ++j) {
          NG_RETURN_IF_ERROR(validatePath(matchClause->path()->path(j), *matchClauseCtx));
        }

        // Available aliases include the aliases pass from the with/unwind
        // And previous aliases are all available to next match
        auto aliasesTmp = matchClauseCtx->aliasesGenerated;
        aliasesAvailable.merge(aliasesTmp);

        if (matchClause->where() != nullptr) {
          auto whereClauseCtx = getContext<WhereClauseContext>();
          whereClauseCtx->aliasesAvailable = aliasesAvailable;
          NG_RETURN_IF_ERROR(validateFilter(matchClause->where()->filter(), *whereClauseCtx));
          matchClauseCtx->where = std::move(whereClauseCtx);
        }

        // Copy the aliases without delete the origins.
        aliasesTmp = matchClauseCtx->aliasesGenerated;
        cypherCtx_->queryParts.back().aliasesGenerated.merge(aliasesTmp);
        cypherCtx_->queryParts.back().matchs.emplace_back(std::move(matchClauseCtx));

        break;
      }
      case ReadingClause::Kind::kUnwind: {
        auto *unwindClause = static_cast<UnwindClause *>(clauses[i].get());
        auto unwindClauseCtx = getContext<UnwindClauseContext>();
        unwindClauseCtx->aliasesAvailable = aliasesAvailable;
        NG_RETURN_IF_ERROR(validateUnwind(unwindClause, *unwindClauseCtx));

        // An unwind pass all available aliases.
        aliasesAvailable.insert(unwindClauseCtx->aliasesGenerated.begin(),
                                unwindClauseCtx->aliasesGenerated.end());
        cypherCtx_->queryParts.back().boundary = std::move(unwindClauseCtx);
        cypherCtx_->queryParts.emplace_back();
        cypherCtx_->queryParts.back().aliasesAvailable = aliasesAvailable;

        break;
      }
      case ReadingClause::Kind::kWith: {
        auto *withClause = static_cast<WithClause *>(clauses[i].get());
        auto withClauseCtx = getContext<WithClauseContext>();
        auto withYieldCtx = getContext<YieldClauseContext>();
        withClauseCtx->yield = std::move(withYieldCtx);
        withClauseCtx->yield->aliasesAvailable = aliasesAvailable;
        NG_RETURN_IF_ERROR(validateWith(withClause, cypherCtx_->queryParts, *withClauseCtx));
        if (withClause->where() != nullptr) {
          auto whereClauseCtx = getContext<WhereClauseContext>();
          whereClauseCtx->aliasesAvailable = withClauseCtx->aliasesGenerated;
          NG_RETURN_IF_ERROR(validateFilter(withClause->where()->filter(), *whereClauseCtx));
          withClauseCtx->where = std::move(whereClauseCtx);
        }

        // A with pass all named aliases to the next query part.
        aliasesAvailable = withClauseCtx->aliasesGenerated;
        cypherCtx_->queryParts.back().boundary = std::move(withClauseCtx);
        cypherCtx_->queryParts.emplace_back();
        cypherCtx_->queryParts.back().aliasesAvailable = aliasesAvailable;

        break;
      }
    }
  }

  retClauseCtx->yield->aliasesAvailable = aliasesAvailable;
  NG_RETURN_IF_ERROR(validateReturn(sentence->ret(), cypherCtx_->queryParts, *retClauseCtx));

  NG_RETURN_IF_ERROR(buildOutputs(retClauseCtx->yield->yieldColumns));
  cypherCtx_->queryParts.back().boundary = std::move(retClauseCtx);
  return Status::OK();
}

Status MatchValidator::validatePath(const MatchPath *path, MatchClauseContext &matchClauseCtx) {
  matchClauseCtx.paths.emplace_back();
  NG_RETURN_IF_ERROR(
      buildNodeInfo(path, matchClauseCtx.paths.back().nodeInfos, matchClauseCtx.aliasesGenerated));
  NG_RETURN_IF_ERROR(
      buildEdgeInfo(path, matchClauseCtx.paths.back().edgeInfos, matchClauseCtx.aliasesGenerated));
  NG_RETURN_IF_ERROR(buildPathExpr(path, matchClauseCtx));
  return Status::OK();
}

Status MatchValidator::buildPathExpr(const MatchPath *path, MatchClauseContext &matchClauseCtx) {
  auto *pathAlias = path->alias();
  if (pathAlias == nullptr) {
    return Status::OK();
  }
  if (!matchClauseCtx.aliasesGenerated.emplace(*pathAlias, AliasType::kPath).second) {
    return Status::SemanticError("`%s': Redefined alias", pathAlias->c_str());
  }

  auto &pathInfo = matchClauseCtx.paths.back();
  auto &nodeInfos = pathInfo.nodeInfos;
  auto &edgeInfos = pathInfo.edgeInfos;

  auto *pool = qctx_->objPool();
  auto pathBuild = PathBuildExpression::make(pool);
  for (size_t i = 0; i < edgeInfos.size(); ++i) {
    pathBuild->add(InputPropertyExpression::make(pool, nodeInfos[i].alias));
    pathBuild->add(InputPropertyExpression::make(pool, edgeInfos[i].alias));
  }
  pathBuild->add(InputPropertyExpression::make(pool, nodeInfos.back().alias));
  pathInfo.pathBuild = std::move(pathBuild);
  return Status::OK();
}

Status MatchValidator::buildNodeInfo(const MatchPath *path,
                                     std::vector<NodeInfo> &nodeInfos,
                                     std::unordered_map<std::string, AliasType> &aliases) {
  auto *sm = qctx_->schemaMng();
  auto steps = path->steps();
  auto *pool = qctx_->objPool();
  nodeInfos.resize(steps + 1);
  std::unordered_map<std::string, AliasType> nodeAliases;

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
          nodeInfos[i].labels.emplace_back(*label->label());
          nodeInfos[i].labelProps.emplace_back(label->props());
        }
      }
    }
    if (alias.empty()) {
      anonymous = true;
      alias = vctx_->anonVarGen()->getVar();
    } else {
      if (!nodeAliases.emplace(alias, AliasType::kNode).second) {
        return Status::SemanticError("`%s': Redefined alias in a single path pattern.",
                                     alias.c_str());
      }
    }
    Expression *filter = nullptr;
    if (props != nullptr) {
      auto result = makeNodeSubFilter(const_cast<MapExpression *>(props), "*");
      NG_RETURN_IF_ERROR(result);
      filter = result.value();
    } else if (node->labels() != nullptr && !node->labels()->labels().empty()) {
      const auto &labels = node->labels()->labels();
      for (const auto &label : labels) {
        auto result = makeNodeSubFilter(label->props(), *label->label());
        NG_RETURN_IF_ERROR(result);
        filter = andConnect(pool, filter, result.value());
      }
    }
    nodeInfos[i].anonymous = anonymous;
    nodeInfos[i].alias = alias;
    nodeInfos[i].props = props;
    nodeInfos[i].filter = filter;
  }
  aliases.merge(nodeAliases);

  return Status::OK();
}

Status MatchValidator::buildEdgeInfo(const MatchPath *path,
                                     std::vector<EdgeInfo> &edgeInfos,
                                     std::unordered_map<std::string, AliasType> &aliases) {
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
        edgeInfos[i].types.emplace_back(*type);
      }
    } else {
      const auto allEdgesResult = cypherCtx_->qctx->schemaMng()->getAllVerEdgeSchema(space_.id);
      NG_RETURN_IF_ERROR(allEdgesResult);
      const auto allEdges = std::move(allEdgesResult).value();
      for (const auto &edgeSchema : allEdges) {
        edgeInfos[i].edgeTypes.emplace_back(edgeSchema.first);
        auto typeName = cypherCtx_->qctx->schemaMng()->toEdgeName(space_.id, edgeSchema.first);
        NG_RETURN_IF_ERROR(typeName);
        edgeInfos[i].types.emplace_back(typeName.value());
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
    } else {
      if (!aliases.emplace(alias, AliasType::kEdge).second) {
        return Status::SemanticError("`%s': Redefined alias", alias.c_str());
      }
    }
    Expression *filter = nullptr;
    if (props != nullptr) {
      auto result = makeEdgeSubFilter(const_cast<MapExpression *>(props));
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
  auto transformRes = ExpressionUtils::filterTransform(filter);
  NG_RETURN_IF_ERROR(transformRes);
  // rewrite Attribute to LabelTagProperty
  whereClauseCtx.filter = ExpressionUtils::rewriteAttr2LabelTagProp(
      transformRes.value(), whereClauseCtx.aliasesAvailable);

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

  NG_RETURN_IF_ERROR(validateAliases({whereClauseCtx.filter}, whereClauseCtx.aliasesAvailable));

  return Status::OK();
}

Status MatchValidator::buildColumnsForAllNamedAliases(const std::vector<QueryPart> &queryParts,
                                                      YieldColumns *columns) const {
  if (queryParts.empty()) {
    return Status::Error("No alias declared.");
  }

  auto *pool = qctx_->objPool();
  auto makeColumn = [&pool](const std::string &name) {
    auto *expr = LabelExpression::make(pool, name);
    return new YieldColumn(expr, name);
  };
  auto &currQueryPart = queryParts.back();
  if (queryParts.size() > 1) {
    auto &prevQueryPart = *(queryParts.end() - 2);
    auto &boundary = prevQueryPart.boundary;
    switch (boundary->kind) {
      case CypherClauseKind::kUnwind: {
        auto unwindCtx = static_cast<const UnwindClauseContext *>(boundary.get());
        columns->addColumn(makeColumn(unwindCtx->alias));
        break;
      }
      case CypherClauseKind::kWith: {
        auto withCtx = static_cast<const WithClauseContext *>(boundary.get());
        auto yieldColumns = withCtx->yield->yieldColumns;
        if (yieldColumns == nullptr) {
          break;
        }
        for (auto &col : yieldColumns->columns()) {
          if (!col->alias().empty()) {
            columns->addColumn(makeColumn(col->alias()));
          }
        }
        break;
      }
      default: {
        DCHECK(false) << "Could not be a return or match.";
      }
    }
  }

  for (auto &match : currQueryPart.matchs) {
    for (auto &path : match->paths) {
      for (size_t i = 0; i < path.edgeInfos.size(); ++i) {
        if (!path.nodeInfos[i].anonymous) {
          columns->addColumn(makeColumn(path.nodeInfos[i].alias));
        }
        if (!path.edgeInfos[i].anonymous) {
          columns->addColumn(makeColumn(path.edgeInfos[i].alias));
        }
      }
      if (!path.nodeInfos.back().anonymous) {
        columns->addColumn(makeColumn(path.nodeInfos.back().alias));
      }
    }

    for (auto &aliasPair : match->aliasesGenerated) {
      if (aliasPair.second == AliasType::kPath) {
        columns->addColumn(makeColumn(aliasPair.first));
      }
    }
  }

  return Status::OK();
}

Status MatchValidator::validateReturn(MatchReturn *ret,
                                      const std::vector<QueryPart> &queryParts,
                                      ReturnClauseContext &retClauseCtx) const {
  YieldColumns *columns = saveObject(new YieldColumns());
  if (ret->returnItems()->allNamedAliases() && !queryParts.empty()) {
    auto status = buildColumnsForAllNamedAliases(queryParts, columns);
    if (!status.ok()) {
      return status;
    }
    if (columns->empty() && !ret->returnItems()->columns()) {
      return Status::SemanticError("RETURN * is not allowed when there are no variables in scope");
    }
  }
  std::vector<const Expression *> exprs;
  if (ret->returnItems()->columns()) {
    exprs.reserve(ret->returnItems()->columns()->size());
    for (auto *column : ret->returnItems()->columns()->columns()) {
      if (ExpressionUtils::hasAny(column->expr(),
                                  {Expression::Kind::kVertex, Expression::Kind::kEdge})) {
        return Status::SemanticError(
            "keywords: vertex and edge are not supported in return clause `%s'",
            column->toString().c_str());
      }
      if (!retClauseCtx.yield->hasAgg_ &&
          ExpressionUtils::hasAny(column->expr(), {Expression::Kind::kAggregate})) {
        retClauseCtx.yield->hasAgg_ = true;
      }
      column->setExpr(ExpressionUtils::rewriteAttr2LabelTagProp(
          column->expr(), retClauseCtx.yield->aliasesAvailable));
      exprs.push_back(column->expr());
      columns->addColumn(column->clone().release());
    }
  }
  DCHECK(!columns->empty());
  retClauseCtx.yield->yieldColumns = columns;

  NG_RETURN_IF_ERROR(validateAliases(exprs, retClauseCtx.yield->aliasesAvailable));
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
    const std::unordered_map<std::string, AliasType> &aliasesAvailable) const {
  static const std::unordered_set<Expression::Kind> kinds = {Expression::Kind::kLabel,
                                                             Expression::Kind::kLabelAttribute,
                                                             Expression::Kind::kLabelTagProperty,
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
      NG_RETURN_IF_ERROR(checkAlias(refExpr, aliasesAvailable));
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
  if (max == std::numeric_limits<size_t>::max()) {
    return Status::SemanticError("Cannot set maximum hop for variable length relationships");
  }
  return Status::OK();
}

Status MatchValidator::validateWith(const WithClause *with,
                                    const std::vector<QueryPart> &queryParts,
                                    WithClauseContext &withClauseCtx) const {
  YieldColumns *columns = saveObject(new YieldColumns());
  if (with->returnItems()->allNamedAliases() && !queryParts.empty()) {
    auto status = buildColumnsForAllNamedAliases(queryParts, columns);
    if (!status.ok()) {
      return status;
    }
  }
  if (with->returnItems()->columns()) {
    for (auto *column : with->returnItems()->columns()->columns()) {
      column->setExpr(ExpressionUtils::rewriteAttr2LabelTagProp(
          column->expr(), withClauseCtx.yield->aliasesAvailable));
      columns->addColumn(column->clone().release());
    }
  }
  withClauseCtx.yield->yieldColumns = columns;

  // Check all referencing expressions are valid
  std::vector<const Expression *> exprs;
  exprs.reserve(withClauseCtx.yield->yieldColumns->size());
  for (auto *col : withClauseCtx.yield->yieldColumns->columns()) {
    auto labelExprs = ExpressionUtils::collectAll(col->expr(), {Expression::Kind::kLabel});
    auto aliasType = AliasType::kDefault;
    for (auto *labelExpr : labelExprs) {
      auto label = static_cast<const LabelExpression *>(labelExpr)->name();
      if (!withClauseCtx.yield->aliasesAvailable.count(label)) {
        return Status::SemanticError("Alias `%s` not defined", label.c_str());
      }
      aliasType = withClauseCtx.yield->aliasesAvailable.at(label);
    }
    if (col->alias().empty()) {
      if (col->expr()->kind() == Expression::Kind::kLabel) {
        col->setAlias(col->toString());
      } else {
        return Status::SemanticError("Expression in WITH must be aliased (use AS)");
      }
    }
    if (col->expr()->kind() == Expression::Kind::kLabel) {
      auto label = static_cast<const LabelExpression *>(col->expr())->name();
      auto found = withClauseCtx.yield->aliasesAvailable.find(label);
      DCHECK(found != withClauseCtx.yield->aliasesAvailable.end());
      if (!withClauseCtx.aliasesGenerated.emplace(col->alias(), found->second).second) {
        return Status::SemanticError("`%s': Redefined alias", col->alias().c_str());
      }
    } else {
      if (!withClauseCtx.aliasesGenerated.emplace(col->alias(), aliasType).second) {
        return Status::SemanticError("`%s': Redefined alias", col->alias().c_str());
      }
    }

    if (!withClauseCtx.yield->hasAgg_ &&
        ExpressionUtils::hasAny(col->expr(), {Expression::Kind::kAggregate})) {
      withClauseCtx.yield->hasAgg_ = true;
    }
    exprs.push_back(col->expr());
  }

  NG_RETURN_IF_ERROR(validateAliases(exprs, withClauseCtx.yield->aliasesAvailable));
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
  if (ExpressionUtils::hasAny(unwindCtx.unwindExpr, {Expression::Kind::kAggregate})) {
    return Status::SemanticError("Can't use aggregating expressions in unwind clause, `%s'",
                                 unwindCtx.unwindExpr->toString().c_str());
  }

  auto labelExprs = ExpressionUtils::collectAll(unwindCtx.unwindExpr, {Expression::Kind::kLabel});
  for (auto *labelExpr : labelExprs) {
    DCHECK_EQ(labelExpr->kind(), Expression::Kind::kLabel);
    auto label = static_cast<const LabelExpression *>(labelExpr)->name();
    if (!unwindCtx.aliasesAvailable.count(label)) {
      return Status::SemanticError("Variable `%s` not defined", label.c_str());
    }
  }
  unwindCtx.aliasesGenerated.emplace(unwindCtx.alias, AliasType::kDefault);
  if (unwindCtx.aliasesAvailable.count(unwindCtx.alias) > 0) {
    return Status::SemanticError("Variable `%s` already declared", unwindCtx.alias.c_str());
  }

  return Status::OK();
}

StatusOr<Expression *> MatchValidator::makeEdgeSubFilter(MapExpression *map) const {
  auto *pool = qctx_->objPool();
  DCHECK(map != nullptr);
  auto &items = map->items();
  DCHECK(!items.empty());

  auto foldStatus = ExpressionUtils::foldConstantExpr(items[0].second);
  NG_RETURN_IF_ERROR(foldStatus);
  auto foldExpr = foldStatus.value();
  if (!ExpressionUtils::isEvaluableExpr(foldExpr, qctx_)) {
    return Status::SemanticError("Props must be evaluable: `%s'",
                                 items[0].second->toString().c_str());
  }
  map->setItem(0, std::make_pair(items[0].first, foldExpr));
  Expression *root = RelationalExpression::makeEQ(
      pool, EdgePropertyExpression::make(pool, "*", items[0].first), foldExpr);
  for (auto i = 1u; i < items.size(); i++) {
    foldStatus = ExpressionUtils::foldConstantExpr(items[i].second);
    NG_RETURN_IF_ERROR(foldStatus);
    foldExpr = foldStatus.value();
    if (!ExpressionUtils::isEvaluableExpr(foldExpr, qctx_)) {
      return Status::SemanticError("Props must be evaluable: `%s'",
                                   items[i].second->toString().c_str());
    }
    map->setItem(0, std::make_pair(items[i].first, foldExpr));
    auto *left = root;
    auto *right = RelationalExpression::makeEQ(
        pool, EdgePropertyExpression::make(pool, "*", items[i].first), foldExpr);
    root = LogicalExpression::makeAnd(pool, left, right);
  }
  return root;
}

StatusOr<Expression *> MatchValidator::makeNodeSubFilter(MapExpression *map,
                                                         const std::string &label) const {
  auto *pool = qctx_->objPool();
  // Node has tag without property
  if (!label.empty() && map == nullptr) {
    // label._tag IS NOT EMPTY
    auto *tagExpr = TagPropertyExpression::make(pool, label, kTag);
    auto *root = UnaryExpression::makeIsNotEmpty(pool, tagExpr);

    return root;
  }

  DCHECK(map != nullptr);
  auto &items = map->items();
  DCHECK(!items.empty());

  auto foldStatus = ExpressionUtils::foldConstantExpr(items[0].second);
  NG_RETURN_IF_ERROR(foldStatus);
  auto foldExpr = foldStatus.value();
  if (!ExpressionUtils::isEvaluableExpr(foldExpr, qctx_)) {
    return Status::SemanticError("Props must be evaluable: `%s'",
                                 items[0].second->toString().c_str());
  }
  map->setItem(0, std::make_pair(items[0].first, foldExpr));
  Expression *root = RelationalExpression::makeEQ(
      pool, TagPropertyExpression::make(pool, label, items[0].first), foldExpr);
  for (auto i = 1u; i < items.size(); i++) {
    foldStatus = ExpressionUtils::foldConstantExpr(items[i].second);
    NG_RETURN_IF_ERROR(foldStatus);
    foldExpr = foldStatus.value();
    if (!ExpressionUtils::isEvaluableExpr(foldExpr, qctx_)) {
      return Status::SemanticError("Props must be evaluable: `%s'",
                                   items[i].second->toString().c_str());
    }
    map->setItem(i, std::make_pair(items[i].first, foldExpr));
    auto *left = root;
    auto *right = RelationalExpression::makeEQ(
        pool, TagPropertyExpression::make(pool, label, items[i].first), foldExpr);
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

Status MatchValidator::validatePagination(const Expression *skipExpr,
                                          const Expression *limitExpr,
                                          PaginationContext &paginationCtx) const {
  int64_t skip = 0;
  int64_t limit = std::numeric_limits<int64_t>::max();
  if (skipExpr != nullptr) {
    if (!ExpressionUtils::isEvaluableExpr(skipExpr, qctx_)) {
      return Status::SemanticError("SKIP should be instantly evaluable");
    }
    auto value = const_cast<Expression *>(skipExpr)->eval(QueryExpressionContext(qctx_->ectx())());
    if (!value.isInt()) {
      return Status::SemanticError("SKIP should be of type integer");
    }
    if (value.getInt() < 0) {
      return Status::SemanticError("SKIP should not be negative");
    }
    skip = value.getInt();
  }

  if (limitExpr != nullptr) {
    if (!ExpressionUtils::isEvaluableExpr(limitExpr, qctx_)) {
      return Status::SemanticError("SKIP should be instantly evaluable");
    }
    auto value = const_cast<Expression *>(limitExpr)->eval(QueryExpressionContext(qctx_->ectx())());
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
        return Status::SemanticError("Duplicated columns not allowed: %s", inputColList[i].c_str());
      }
    }

    for (auto &factor : factors->factors()) {
      auto factorExpr = factor->expr();
      if (ExpressionUtils::isEvaluableExpr(factorExpr, qctx_)) continue;
      if (factorExpr->kind() != Expression::Kind::kLabel) {
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
      auto aggs = ExpressionUtils::collectAll(collectAggCol, {Expression::Kind::kAggregate});
      for (auto *agg : aggs) {
        DCHECK_EQ(agg->kind(), Expression::Kind::kAggregate);
        if (!ExpressionUtils::checkAggExpr(static_cast<const AggregateExpression *>(agg)).ok()) {
          return Status::SemanticError("Aggregate function nesting is not allowed: `%s'",
                                       colExpr->toString().c_str());
        }

        yieldCtx.groupItems_.emplace_back(agg->clone());

        yieldCtx.needGenProject_ = true;
        yieldCtx.aggOutputColumnNames_.emplace_back(agg->toString());
      }
      if (!aggs.empty()) {
        auto *rewritedExpr = ExpressionUtils::rewriteAgg2VarProp(colExpr->clone());
        yieldCtx.projCols_->addColumn(new YieldColumn(rewritedExpr, colOldName));
        yieldCtx.projOutputColumnNames_.emplace_back(colOldName);
        continue;
      }
    }

    if (colExpr->kind() == Expression::Kind::kAggregate) {
      auto *aggExpr = static_cast<AggregateExpression *>(colExpr);
      NG_RETURN_IF_ERROR(ExpressionUtils::checkAggExpr(aggExpr));
    } else if (!ExpressionUtils::isEvaluableExpr(colExpr, qctx_)) {
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
    const std::unordered_map<std::string, AliasType> &aliasesAvailable,
    const std::string &name) const {
  auto iter = aliasesAvailable.find(name);
  if (iter == aliasesAvailable.end()) {
    return Status::SemanticError("Alias used but not defined: `%s'", name.c_str());
  }
  return iter->second;
}

Status MatchValidator::checkAlias(
    const Expression *refExpr,
    const std::unordered_map<std::string, AliasType> &aliasesAvailable) const {
  auto kind = refExpr->kind();
  AliasType aliasType = AliasType::kDefault;

  switch (kind) {
    case Expression::Kind::kLabel: {
      auto name = static_cast<const LabelExpression *>(refExpr)->name();
      auto res = getAliasType(aliasesAvailable, name);
      NG_RETURN_IF_ERROR(res);
      return Status::OK();
    }
    case Expression::Kind::kLabelTagProperty: {
      auto labelExpr = static_cast<const LabelTagPropertyExpression *>(refExpr)->label();
      auto name = static_cast<const VariablePropertyExpression *>(labelExpr)->prop();
      auto res = getAliasType(aliasesAvailable, name);
      NG_RETURN_IF_ERROR(res);
      if (res.value() == AliasType::kEdge || res.value() == AliasType::kPath) {
        return Status::SemanticError("The type of `%s' should be tag", name.c_str());
      }
      return Status::OK();
    }
    case Expression::Kind::kLabelAttribute: {
      auto name = static_cast<const LabelAttributeExpression *>(refExpr)->left()->name();
      auto res = getAliasType(aliasesAvailable, name);
      NG_RETURN_IF_ERROR(res);
      if (res.value() == AliasType::kNode) {
        return Status::SemanticError(
            "To get the property of the vertex in `%s', should use the format `var.tag.prop'",
            refExpr->toString().c_str());
      }
      return Status::OK();
    }
    case Expression::Kind::kEdgeSrc: {
      auto name = static_cast<const EdgeSrcIdExpression *>(refExpr)->sym();
      auto res = getAliasType(aliasesAvailable, name);
      NG_RETURN_IF_ERROR(res);
      aliasType = res.value();
      switch (aliasType) {
        case AliasType::kNode:
          return Status::SemanticError("Vertex `%s' does not have the src attribute", name.c_str());
        case AliasType::kEdge:
          return Status::SemanticError("To get the src vid of the edge, use src(%s)", name.c_str());
        case AliasType::kPath:
          return Status::SemanticError("To get the start node of the path, use startNode(%s)",
                                       name.c_str());
        default:
          return Status::SemanticError("Alias `%s' does not have the edge property src",
                                       name.c_str());
      }
    }
    case Expression::Kind::kEdgeDst: {
      auto name = static_cast<const EdgeDstIdExpression *>(refExpr)->sym();
      auto res = getAliasType(aliasesAvailable, name);
      NG_RETURN_IF_ERROR(res);
      aliasType = res.value();
      switch (aliasType) {
        case AliasType::kNode:
          return Status::SemanticError("Vertex `%s' does not have the dst attribute", name.c_str());
        case AliasType::kEdge:
          return Status::SemanticError("To get the dst vid of the edge, use dst(%s)", name.c_str());
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
      auto res = getAliasType(aliasesAvailable, name);
      NG_RETURN_IF_ERROR(res);
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
          return Status::SemanticError("Alias `%s' does not have the edge property ranking",
                                       name.c_str());
      }
    }
    case Expression::Kind::kEdgeType: {
      auto name = static_cast<const EdgeTypeExpression *>(refExpr)->sym();
      auto res = getAliasType(aliasesAvailable, name);
      NG_RETURN_IF_ERROR(res);
      aliasType = res.value();
      switch (aliasType) {
        case AliasType::kNode:
          return Status::SemanticError("Vertex `%s' does not have the type attribute",
                                       name.c_str());
        case AliasType::kEdge:
          return Status::SemanticError("To get the type of the edge, use type(%s)", name.c_str());
        case AliasType::kPath:
          return Status::SemanticError("Path `%s' does not have the type attribute", name.c_str());
        default:
          return Status::SemanticError("Alias `%s' does not have the edge property ranking",
                                       name.c_str());
      }
    }
    default:  // refExpr must satisfy one of cases and should never hit this branch
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

}  // namespace graph
}  // namespace nebula
