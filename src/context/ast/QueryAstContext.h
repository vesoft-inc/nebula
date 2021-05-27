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
    std::string             firstBeginningSrcVidColName;
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
};

}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_AST_QUERYASTCONTEXT_H_
