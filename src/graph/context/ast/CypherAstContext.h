/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_CONTEXT_AST_CYPHERASTCONTEXT_H_
#define GRAPH_CONTEXT_AST_CYPHERASTCONTEXT_H_

#include "common/base/Base.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/PathBuildExpression.h"
#include "graph/context/ast/AstContext.h"
#include "parser/MatchSentence.h"

namespace nebula {
namespace graph {
enum class CypherClauseKind : uint8_t {
  kMatch,
  kUnwind,
  kWith,
  kWhere,
  kReturn,
  kOrderBy,
  kPagination,
  kYield,
};

enum class PatternKind : uint8_t {
  kNode,
  kEdge,
};

using Direction = MatchEdge::Direction;
struct NodeInfo {
  bool anonymous{false};
  std::vector<TagID> tids;
  std::vector<std::string> labels;
  std::vector<MapExpression*> labelProps;
  std::string alias;
  const MapExpression* props{nullptr};
  Expression* filter{nullptr};
};

struct EdgeInfo {
  bool anonymous{false};
  MatchStepRange* range{nullptr};
  std::vector<EdgeType> edgeTypes;
  MatchEdge::Direction direction{MatchEdge::Direction::OUT_EDGE};
  std::vector<std::string> types;
  std::string alias;
  const MapExpression* props{nullptr};
  Expression* filter{nullptr};
};

enum class AliasType : int8_t { kNode, kEdge, kPath, kDefault };

struct ScanInfo {
  Expression* filter{nullptr};
  std::vector<int32_t> schemaIds;
  std::vector<std::string> schemaNames;
  // use for seek by index itself
  std::vector<IndexID> indexIds;
  // use for seek by edge only
  MatchEdge::Direction direction{MatchEdge::Direction::OUT_EDGE};
  // use for scan seek
  bool anyLabel{false};
};

struct CypherClauseContextBase : AstContext {
  explicit CypherClauseContextBase(CypherClauseKind k) : kind(k) {}
  virtual ~CypherClauseContextBase() = default;

  const CypherClauseKind kind;
};

struct WhereClauseContext final : CypherClauseContextBase {
  WhereClauseContext() : CypherClauseContextBase(CypherClauseKind::kWhere) {}

  Expression* filter{nullptr};
  std::unordered_map<std::string, AliasType> aliasesAvailable;
};

struct OrderByClauseContext final : CypherClauseContextBase {
  OrderByClauseContext() : CypherClauseContextBase(CypherClauseKind::kOrderBy) {}

  std::vector<std::pair<size_t, OrderFactor::OrderType>> indexedOrderFactors;
};

struct PaginationContext final : CypherClauseContextBase {
  PaginationContext() : CypherClauseContextBase(CypherClauseKind::kPagination) {}

  int64_t skip{0};
  int64_t limit{std::numeric_limits<int64_t>::max()};
};

struct YieldClauseContext final : CypherClauseContextBase {
  YieldClauseContext() : CypherClauseContextBase(CypherClauseKind::kYield) {}

  bool distinct{false};
  const YieldColumns* yieldColumns{nullptr};
  std::unordered_map<std::string, AliasType> aliasesAvailable;

  bool hasAgg_{false};
  bool needGenProject_{false};
  YieldColumns* projCols_;
  std::vector<Expression*> groupKeys_;
  std::vector<Expression*> groupItems_;
  std::vector<std::string> aggOutputColumnNames_;
  std::vector<std::string> projOutputColumnNames_;
};

struct ReturnClauseContext final : CypherClauseContextBase {
  ReturnClauseContext() : CypherClauseContextBase(CypherClauseKind::kReturn) {}

  std::unique_ptr<OrderByClauseContext> order;
  std::unique_ptr<PaginationContext> pagination;
  std::unique_ptr<YieldClauseContext> yield;
};

struct WithClauseContext final : CypherClauseContextBase {
  WithClauseContext() : CypherClauseContextBase(CypherClauseKind::kWith) {}

  std::unique_ptr<OrderByClauseContext> order;
  std::unique_ptr<PaginationContext> pagination;
  std::unique_ptr<WhereClauseContext> where;
  std::unique_ptr<YieldClauseContext> yield;
  std::unordered_map<std::string, AliasType> aliasesGenerated;
};

struct Path final {
  std::vector<NodeInfo> nodeInfos;
  std::vector<EdgeInfo> edgeInfos;
  PathBuildExpression* pathBuild{nullptr};

  // True for pattern expresssion, to collect path to list
  bool rollUpApply{false};
  // vector ["v"] in (v)-[:like]->()
  std::vector<std::string> compareVariables;
  // "(v)-[:like]->()" in (v)-[:like]->()
  std::string collectVariable;
};

struct MatchClauseContext final : CypherClauseContextBase {
  MatchClauseContext() : CypherClauseContextBase(CypherClauseKind::kMatch) {}

  bool isOptional{false};
  std::vector<Path> paths;
  std::unique_ptr<WhereClauseContext> where;
  std::unordered_map<std::string, AliasType> aliasesAvailable;
  std::unordered_map<std::string, AliasType> aliasesGenerated;
};

struct UnwindClauseContext final : CypherClauseContextBase {
  UnwindClauseContext() : CypherClauseContextBase(CypherClauseKind::kUnwind) {}

  Expression* unwindExpr{nullptr};
  std::string alias;

  std::unordered_map<std::string, AliasType> aliasesAvailable;
  std::unordered_map<std::string, AliasType> aliasesGenerated;
};

// A QueryPart begin with an arbitrary number of MATCH clauses, followed by either
// (1) WITH and an optional UNWIND,
// (2) a single UNWIND,
// (3) a RETURN in case of the last query part.
struct QueryPart final {
  std::vector<std::unique_ptr<MatchClauseContext>> matchs;
  // A with/unwind/return
  std::unique_ptr<CypherClauseContextBase> boundary;
  std::unordered_map<std::string, AliasType> aliasesAvailable;
  std::unordered_map<std::string, AliasType> aliasesGenerated;
};

// A cypher query is made up of many QueryPart
struct CypherContext final : AstContext {
  std::vector<QueryPart> queryParts;
};

struct PatternContext {
  PatternContext(PatternKind k, MatchClauseContext* m) : kind(k), matchClauseCtx(m) {}
  const PatternKind kind;
  MatchClauseContext* matchClauseCtx{nullptr};
};

struct NodeContext final : PatternContext {
  NodeContext(MatchClauseContext* m, NodeInfo* i)
      : PatternContext(PatternKind::kNode, m), info(i) {}

  NodeInfo* info{nullptr};
  std::unordered_set<std::string>* nodeAliasesAvailable;

  // Output fields
  ScanInfo scanInfo;
  List ids;
  // initialize start expression in project node
  Expression* initialExpr{nullptr};
};

struct EdgeContext final : PatternContext {
  EdgeContext(MatchClauseContext* m, EdgeInfo* i)
      : PatternContext(PatternKind::kEdge, m), info(i) {}

  EdgeInfo* info{nullptr};

  // Output fields
  ScanInfo scanInfo;
  // initialize start expression in project node
  Expression* initialExpr{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_CONTEXT_AST_CYPHERASTCONTEXT_H_
