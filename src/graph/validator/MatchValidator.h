/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_MATCHVALIDATOR_H_
#define GRAPH_VALIDATOR_MATCHVALIDATOR_H_

#include "graph/context/ast/CypherAstContext.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/AnonVarGenerator.h"
#include "graph/validator/Validator.h"

namespace nebula {

class MatchStepRange;
class ObjectPool;
namespace graph {
class MatchValidator final : public Validator {
 public:
  MatchValidator(Sentence *sentence, QueryContext *context);

 private:
  Status validateImpl() override;

  AstContext *getAstContext() override;

  Status validatePath(const MatchPath *path, MatchClauseContext &matchClauseCtx);

  Status validatePath(const MatchPath *path, Path &pathInfo);

  Status validateFilter(const Expression *filter, WhereClauseContext &whereClauseCtx);

  Status validateReturn(MatchReturn *ret,
                        const std::vector<QueryPart> &queryParts,
                        ReturnClauseContext &retClauseCtx);

  Status validateAliases(const std::vector<const Expression *> &exprs,
                         const std::unordered_map<std::string, AliasType> &aliases) const;

  Status validateStepRange(const MatchStepRange *range) const;

  Status validateWith(const WithClause *with,
                      const std::vector<QueryPart> &queryParts,
                      WithClauseContext &withClauseCtx);

  Status validateUnwind(const UnwindClause *unwind, UnwindClauseContext &unwindClauseCtx);

  Status validatePagination(const Expression *skipExpr,
                            const Expression *limitExpr,
                            PaginationContext &paginationCtx) const;

  Status validateOrderBy(const OrderFactors *factors,
                         const YieldColumns *yieldColumns,
                         OrderByClauseContext &orderByCtx) const;

  Status validateGroup(YieldClauseContext &yieldCtx);

  Status validateYield(YieldClauseContext &yieldCtx);

  Status buildColumnsForAllNamedAliases(const std::vector<QueryPart> &queryParts,
                                        YieldColumns *columns) const;

  static Expression *andConnect(ObjectPool *pool, Expression *left, Expression *right);

  Status buildNodeInfo(const MatchPath *path,
                       std::vector<NodeInfo> &edgeInfos,
                       std::unordered_map<std::string, AliasType> &aliases);

  Status buildEdgeInfo(const MatchPath *path,
                       std::vector<EdgeInfo> &nodeInfos,
                       std::unordered_map<std::string, AliasType> &aliases);

  Status buildPathExpr(const MatchPath *path,
                       Path &pathInfo,
                       std::unordered_map<std::string, AliasType> &aliasesGenerated);

  Status combineAliases(std::unordered_map<std::string, AliasType> &curAliases,
                        const std::unordered_map<std::string, AliasType> &lastAliases) const;

  Status combineYieldColumns(YieldColumns *yieldColumns, YieldColumns *prevYieldColumns) const;

  StatusOr<AliasType> getAliasType(
      const std::unordered_map<std::string, AliasType> &aliasesAvailable,
      const std::string &name) const;

  Status checkAlias(const Expression *refExpr,
                    const std::unordered_map<std::string, AliasType> &aliasesAvailable) const;

  Status buildOutputs(const YieldColumns *yields);

  StatusOr<Expression *> makeEdgeSubFilter(MapExpression *map) const;

  StatusOr<Expression *> makeNodeSubFilter(MapExpression *map, const std::string &label) const;

  // Check undefined variable in match path expression
  Status validateMatchPathExpr(Expression *expr,
                               const std::unordered_map<std::string, AliasType> &availableAliases,
                               std::vector<Path> &paths);

  static Status checkMatchPathExpr(
      const MatchPathPatternExpression *expr,
      const std::unordered_map<std::string, AliasType> &availableAliases);

  static Status buildRollUpPathInfo(const MatchPath *path, Path &pathInfo);

 private:
  std::unique_ptr<CypherContext> cypherCtx_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_VALIDATOR_MATCHVALIDATOR_H_
