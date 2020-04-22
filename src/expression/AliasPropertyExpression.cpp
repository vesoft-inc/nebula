/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "expression/AliasPropertyExpression.h"

namespace nebula {
Value AliasPropertyExpression::eval() const {
    // TODO
    return Value(NullType::NaN);
}

Value InputPropertyExpression::eval() const {
    // TODO
    return Value(NullType::NaN);
}

Value VariablePropertyExpression::eval() const {
    // TODO
    return Value(NullType::NaN);
}

Value SourcePropertyExpression::eval() const {
    // TODO
    return Value(NullType::NaN);
}

Value DestPropertyExpression::eval() const {
    // TODO
    return Value(NullType::NaN);
}

Value EdgeSrcIdExpression::eval() const {
    // TODO
    return Value(NullType::NaN);
}

Value EdgeTypeExpression::eval() const {
    // TODO
    return Value(NullType::NaN);
}

Value EdgeRankExpression::eval() const {
    // TODO
    return Value(NullType::NaN);
}

Value EdgeDstIdExpression::eval() const {
    // TODO
    return Value(NullType::NaN);
}
}  // namespace nebula
