/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_AST_QUERYASTCONTEXT_H_
#define CONTEXT_AST_QUERYASTCONTEXT_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "context/ast/AstContext.h"
#include "visitor/DeducePropsVisitor.h"

namespace nebula {
namespace graph {

enum FromType {
    kInstantExpr,
    kVariable,
    kPipe,
};

struct Starts {
    FromType                fromType{kInstantExpr};
    Expression*             src{nullptr};
    Expression*             originalSrc{nullptr};
    std::string             userDefinedVarName;
    std::string             runtimeVidName;
    std::vector<Value>      vids;
};

struct Over {
    bool                            isOverAll{false};
    std::vector<EdgeType>           edgeTypes;
    storage::cpp2::EdgeDirection    direction;
    std::vector<std::string>        allEdges;
};

// path context
struct PathContext final : AstContext {
    Starts          from;
    Starts          to;
    StepClause      steps;
    Over            over;
    Expression*     filter{nullptr};

    /*
    * find path from A to B OR find path from $-.src to $-.dst
    * fromVidsVar's DataSet save A OR $-.src
    * toVidsVar's DataSet save B OR $-.dst
    */
    std::string     fromVidsVar;
    std::string     toVidsVar;

    bool            isShortest{false};
    bool            isWeight{false};
    bool            noLoop{false};
    bool            withProp{false};

    /*
    * runtime
    * find path from $-.src to $-.dst
    * project($-.src)<- dedup($-.src)
    * runtimeFromProject is project($-.src)
    * runtimeFromDedup is dedup($-.src)
    */
    PlanNode*       runtimeFromProject{nullptr};
    PlanNode*       runtimeFromDedup{nullptr};
    PlanNode*       runtimeToProject{nullptr};
    PlanNode*       runtimeToDedup{nullptr};
    // just for pipe sentence,
    // store the result of the previous sentence
    std::string     inputVarName;
    ExpressionProps exprProps;
};

struct GoContext final : AstContext {
    Starts                      from;
    StepClause                  steps;
    Over                        over;
    Expression*                 filter{nullptr};
    YieldColumns*               yieldExpr;
    bool                        distinct{false};
    // true: sample, false: limit
    bool                        random{false};
    std::vector<std::string>    colNames;

    std::string                 vidsVar;
    // true when pipe or multi-sentence
    bool                        joinInput{false};
    // true when $$.tag.prop exist
    bool                        joinDst{false};

    ExpressionProps             exprProps;

    // save dst prop
    YieldColumns*               dstPropsExpr;
    // save src and edge prop
    YieldColumns*               srcEdgePropsExpr;
    // for track vid in Nsteps
    std::string                 srcVidColName;
    std::string                 dstVidColName;

    // store the result of the previous sentence
    std::string                 inputVarName;
};

struct LookupContext final : public AstContext {
    bool                        isEdge{false};
    bool                        dedup{false};
    bool                        isEmptyResultSet{false};
    int32_t                     schemaId{-1};
    int32_t                     limit{-1};
    Expression*                 filter{nullptr};
    // order by
};

struct SubgraphContext final : public AstContext {
    Starts                          from;
    StepClause                      steps;
    std::string                     loopSteps;

    std::unordered_set<EdgeType>    edgeTypes;
    bool                            withProp{false};
};

}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_AST_QUERYASTCONTEXT_H_
