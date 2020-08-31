/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/AttributeExpression.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/Map.h"
#include "common/datatypes/Vertex.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {

const Value& AttributeExpression::eval(ExpressionContext &ctx) {
    auto &lvalue = left()->eval(ctx);
    auto &rvalue = right()->eval(ctx);
    DCHECK(rvalue.isStr());

    // TODO(dutor) Take care of the builtin properties, _src, _vid, _type, etc.
    if (lvalue.isMap()) {
        return lvalue.getMap().at(rvalue.getStr());
    } else if (lvalue.isVertex()) {
        for (auto &tag : lvalue.getVertex().tags) {
            auto iter = tag.props.find(rvalue.getStr());
            if (iter != tag.props.end()) {
                return iter->second;
            }
        }
        return Value::kNullValue;
    } else if (lvalue.isEdge()) {
        auto iter = lvalue.getEdge().props.find(rvalue.getStr());
        if (iter == lvalue.getEdge().props.end()) {
            return Value::kNullValue;
        }
        return iter->second;
    }

    return Value::kNullBadType;
}

void AttributeExpression::accept(ExprVisitor *visitor) {
    visitor->visit(this);
}

std::string AttributeExpression::toString() const {
    CHECK(right()->kind() == Kind::kLabel || right()->kind() == Kind::kConstant);
    std::string buf;
    buf.reserve(256);
    buf += left()->toString();
    buf += '.';
    buf += right()->toString();

    return buf;
}

}   // namespace nebula
