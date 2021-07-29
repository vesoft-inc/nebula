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
#include "common/time/TimeUtils.h"

namespace nebula {

const Value& AttributeExpression::eval(ExpressionContext &ctx) {
    auto &lvalue = left()->eval(ctx);
    auto &rvalue = right()->eval(ctx);
    DCHECK(rvalue.isStr());

    // TODO(dutor) Take care of the builtin properties, _src, _vid, _type, etc.
    switch (lvalue.type()) {
        case Value::Type::MAP:
            return lvalue.getMap().at(rvalue.getStr());
        case Value::Type::VERTEX: {
            if (rvalue.getStr() == kVid) {
                result_ = lvalue.getVertex().vid;
                return result_;
            }
            for (auto &tag : lvalue.getVertex().tags) {
                auto iter = tag.props.find(rvalue.getStr());
                if (iter != tag.props.end()) {
                    return iter->second;
                }
            }
            return Value::kNullUnknownProp;
        }
        case Value::Type::EDGE: {
            DCHECK(!rvalue.getStr().empty());
            if (rvalue.getStr()[0] == '_') {
                if (rvalue.getStr() == kSrc) {
                    result_ = lvalue.getEdge().src;
                } else if (rvalue.getStr() == kDst) {
                    result_ = lvalue.getEdge().dst;
                } else if (rvalue.getStr() == kRank) {
                    result_ = lvalue.getEdge().ranking;
                } else if (rvalue.getStr() == kType) {
                    result_ = lvalue.getEdge().name;
                }
                return result_;
            }
            auto iter = lvalue.getEdge().props.find(rvalue.getStr());
            if (iter == lvalue.getEdge().props.end()) {
                return Value::kNullUnknownProp;
            }
            return iter->second;
        }
        case Value::Type::DATE:
            result_ = time::TimeUtils::getDateAttr(lvalue.getDate(), rvalue.getStr());
            return result_;
        case Value::Type::TIME:
            result_ = time::TimeUtils::getTimeAttr(lvalue.getTime(), rvalue.getStr());
            return result_;
        case Value::Type::DATETIME:
            result_ = time::TimeUtils::getDateTimeAttr(lvalue.getDateTime(), rvalue.getStr());
            return result_;
        default:
            return Value::kNullBadType;
    }
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
    auto *constant = static_cast<const ConstantExpression*>(right());
    if (constant->value().isStr()) {
        buf += constant->value().getStr();
    } else {
        buf += right()->toString();
    }

    return buf;
}

}   // namespace nebula
