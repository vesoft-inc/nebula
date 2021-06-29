/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXPRESSION_EXPRVISITOR_H_
#define EXPRESSION_EXPRVISITOR_H_

#include "common/base/ObjectPool.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/AttributeExpression.h"
#include "common/expression/CaseExpression.h"
#include "common/expression/ColumnExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/ContainerExpression.h"
#include "common/expression/EdgeExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/AggregateExpression.h"
#include "common/expression/LabelAttributeExpression.h"
#include "common/expression/LabelExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/PathBuildExpression.h"
#include "common/expression/PropertyExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UUIDExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/VariableExpression.h"
#include "common/expression/VertexExpression.h"
#include "common/expression/CaseExpression.h"
#include "common/expression/ColumnExpression.h"
#include "common/expression/PredicateExpression.h"
#include "common/expression/ListComprehensionExpression.h"
#include "common/expression/ReduceExpression.h"

namespace nebula {

class ExprVisitor {
public:
    virtual ~ExprVisitor() = default;

    virtual void visit(ConstantExpression *expr) = 0;
    virtual void visit(UnaryExpression *expr) = 0;
    virtual void visit(TypeCastingExpression *expr) = 0;
    virtual void visit(LabelExpression *expr) = 0;
    virtual void visit(LabelAttributeExpression *expr) = 0;
    // binary expression
    virtual void visit(ArithmeticExpression *expr) = 0;
    virtual void visit(RelationalExpression *expr) = 0;
    virtual void visit(SubscriptExpression *expr) = 0;
    virtual void visit(AttributeExpression *expr) = 0;
    virtual void visit(LogicalExpression *expr) = 0;
    // function call
    virtual void visit(FunctionCallExpression *expr) = 0;
    virtual void visit(AggregateExpression *expr) = 0;
    virtual void visit(UUIDExpression *expr) = 0;
    // variable expression
    virtual void visit(VariableExpression *expr) = 0;
    virtual void visit(VersionedVariableExpression *expr) = 0;
    // container expression
    virtual void visit(ListExpression *expr) = 0;
    virtual void visit(SetExpression *expr) = 0;
    virtual void visit(MapExpression *expr) = 0;
    // property Expression
    virtual void visit(TagPropertyExpression *expr) = 0;
    virtual void visit(EdgePropertyExpression *expr) = 0;
    virtual void visit(InputPropertyExpression *expr) = 0;
    virtual void visit(VariablePropertyExpression *expr) = 0;
    virtual void visit(DestPropertyExpression *expr) = 0;
    virtual void visit(SourcePropertyExpression *expr) = 0;
    virtual void visit(EdgeSrcIdExpression *expr) = 0;
    virtual void visit(EdgeTypeExpression *expr) = 0;
    virtual void visit(EdgeRankExpression *expr) = 0;
    virtual void visit(EdgeDstIdExpression *expr) = 0;
    // vertex/edge expression
    virtual void visit(VertexExpression *expr) = 0;
    virtual void visit(EdgeExpression *expr) = 0;
    // case expression
    virtual void visit(CaseExpression *expr) = 0;
    // path build expression
    virtual void visit(PathBuildExpression *expr) = 0;
    // column expression
    virtual void visit(ColumnExpression *expr) = 0;
    // predicate expression
    virtual void visit(PredicateExpression *expr) = 0;
    // list comprehension expression
    virtual void visit(ListComprehensionExpression *expr) = 0;
    // reduce expression
    virtual void visit(ReduceExpression *expr) = 0;
    // subscript range expression
    virtual void visit(SubscriptRangeExpression *expr) = 0;
};

}   // namespace nebula

#endif   // EXPRESSION_EXPRVISITOR_H_
