/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COONTEXT_AST_QUERYASTCONTEXT_H_
#define COONTEXT_AST_QUERYASTCONTEXT_H_

#include "common/base/Base.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/Expression.h"
#include "common/expression/PathBuildExpression.h"
#include "context/ast/AstContext.h"
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
};

enum class PatternKind : uint8_t {
    kNode,
    kEdge,
};

using Direction = MatchEdge::Direction;
struct NodeInfo {
    TagID                                   tid{0};
    bool                                    anonymous{false};
    const std::string                      *label{nullptr};
    const std::string                      *alias{nullptr};
    const MapExpression                    *props{nullptr};
    Expression                             *filter{nullptr};
};

struct EdgeInfo {
    bool                                    anonymous{false};
    MatchStepRange                         *range{nullptr};
    std::vector<EdgeType>                   edgeTypes;
    MatchEdge::Direction                    direction{MatchEdge::Direction::OUT_EDGE};
    std::vector<std::string>                types;
    const std::string                      *alias{nullptr};
    const MapExpression                    *props{nullptr};
    Expression                             *filter{nullptr};
};

enum class AliasType : int8_t {
    kNode, kEdge, kPath, kDefault
};

struct ScanInfo {
    Expression                             *filter{nullptr};
    int32_t                                 schemaId{0};
    const std::string                      *schemaName{nullptr};
    // use for seek by index itself
    IndexID                                 indexId{-1};
};

struct CypherClauseContextBase : AstContext {
    explicit CypherClauseContextBase(CypherClauseKind k) : kind(k) {}
    virtual ~CypherClauseContextBase() = default;

    const CypherClauseKind  kind;
};

struct WhereClauseContext final : CypherClauseContextBase {
    WhereClauseContext() : CypherClauseContextBase(CypherClauseKind::kWhere) {}

    std::unique_ptr<Expression>                  filter;
    std::unordered_map<std::string, AliasType>*  aliasesUsed{nullptr};
};

struct OrderByClauseContext final : CypherClauseContextBase {
    OrderByClauseContext() : CypherClauseContextBase(CypherClauseKind::kOrderBy) {}

    std::vector<std::pair<size_t, OrderFactor::OrderType>>      indexedOrderFactors;
};

struct PaginationContext final : CypherClauseContextBase {
    PaginationContext() : CypherClauseContextBase(CypherClauseKind::kPagination) {}

    int64_t     skip{0};
    int64_t     limit{std::numeric_limits<int64_t>::max()};
};

struct ReturnClauseContext final : CypherClauseContextBase {
    ReturnClauseContext() : CypherClauseContextBase(CypherClauseKind::kReturn) {}

    bool                                         distinct{false};
    const YieldColumns*                          yieldColumns{nullptr};
    std::unique_ptr<OrderByClauseContext>        order;
    std::unique_ptr<PaginationContext>           pagination;
    std::unordered_map<std::string, AliasType>*  aliasesUsed{nullptr};
    // TODO: grouping columns
};

struct WithClauseContext final : CypherClauseContextBase {
    WithClauseContext() : CypherClauseContextBase(CypherClauseKind::kWith) {}

    bool                                        distinct{false};
    const YieldColumns*                         yieldColumns{nullptr};
    std::unique_ptr<OrderByClauseContext>       order;
    std::unique_ptr<PaginationContext>          pagination;
    std::unique_ptr<WhereClauseContext>         where;
    std::unordered_map<std::string, AliasType>* aliasesUsed{nullptr};
    std::unordered_map<std::string, AliasType>  aliasesGenerated;
    // TODO: grouping columns
};

struct MatchClauseContext final : CypherClauseContextBase {
    MatchClauseContext() : CypherClauseContextBase(CypherClauseKind::kMatch) {}

    std::vector<NodeInfo>                       nodeInfos;
    std::vector<EdgeInfo>                       edgeInfos;
    std::unique_ptr<PathBuildExpression>        pathBuild;
    std::unique_ptr<WhereClauseContext>         where;
    std::unordered_map<std::string, AliasType>* aliasesUsed{nullptr};
    std::unordered_map<std::string, AliasType>  aliasesGenerated;
};

struct UnwindClauseContext final : CypherClauseContextBase {
    UnwindClauseContext() : CypherClauseContextBase(CypherClauseKind::kUnwind) {}

    const YieldColumns*                         yieldColumns{nullptr};
    std::unordered_map<std::string, AliasType>* aliasesUsed{nullptr};
    std::unordered_map<std::string, AliasType>  aliasesGenerated;
};

struct MatchAstContext final : AstContext {
    // Alternative of Match/Unwind/With and ends with Return.
    std::vector<std::unique_ptr<CypherClauseContextBase>>  clauses;
};

struct PatternContext {
    PatternContext(PatternKind k, MatchClauseContext* m) : kind(k), matchClauseCtx(m) {}
    const PatternKind kind;
    MatchClauseContext*    matchClauseCtx{nullptr};
};

struct NodeContext final : PatternContext {
    NodeContext(MatchClauseContext* m, NodeInfo* i)
        : PatternContext(PatternKind::kNode, m), info(i) {}

    NodeInfo*            info{nullptr};

    // Output fields
    ScanInfo                    scanInfo;
    const Expression*           ids{nullptr};
    // initialize start expression in project node
    std::unique_ptr<Expression> initialExpr;
};

struct EdgeContext final : PatternContext {
    EdgeContext(MatchClauseContext* m, EdgeInfo* i)
        : PatternContext(PatternKind::kEdge, m), info(i) {}

    EdgeInfo* info{nullptr};
};
}  // namespace graph
}  // namespace nebula
#endif  // COONTEXT_AST_QUERYASTCONTEXT_H_
