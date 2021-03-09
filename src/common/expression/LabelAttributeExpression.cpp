/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/LabelAttributeExpression.h"

#include "common/expression/ExprVisitor.h"

namespace nebula {

std::string LabelAttributeExpression::toString() const {
    if (right()->value().isStr()) {
        return left()->toString() + "." + right()->value().getStr();
    }
    return left()->toString() + "." + right()->toString();
}

void LabelAttributeExpression::accept(ExprVisitor *visitor) {
    visitor->visit(this);
}

}   // namespace nebula
