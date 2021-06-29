/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/UnaryExpression.h"
#include "common/expression/VariableExpression.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {

bool UnaryExpression::operator==(const Expression& rhs) const {
    if (kind_ != rhs.kind()) {
        return false;
    }

    const auto& r = static_cast<const UnaryExpression&>(rhs);
    return *operand_ == *(r.operand_);
}



void UnaryExpression::writeTo(Encoder& encoder) const {
    // kind_
    encoder << kind_;

    // operand_
    DCHECK(!!operand_);
    encoder << *operand_;
}


void UnaryExpression::resetFrom(Decoder& decoder) {
    // Read operand_
    operand_ = decoder.readExpression(pool_);
    CHECK(!!operand_);
}


const Value& UnaryExpression::eval(ExpressionContext& ctx) {
    DCHECK(!!operand_);
    switch (kind_) {
        case Kind::kUnaryPlus: {
            Value val(operand_->eval(ctx));
            result_ = std::move(val);
            break;
        }
        case Kind::kUnaryNegate: {
            result_ = -(operand_->eval(ctx));
            break;
        }
        case Kind::kUnaryNot: {
            result_ = !(operand_->eval(ctx));
            break;
        }
        case Kind::kUnaryIncr: {
            if (UNLIKELY(operand_->kind() != Kind::kVar
                        && operand_->kind() != Kind::kVersionedVar)) {
                result_ = Value(NullType::BAD_TYPE);
                break;
            }
            result_ = operand_->eval(ctx) + 1;
            auto* varExpr = static_cast<VariableExpression*>(operand_);
            ctx.setVar(varExpr->var(), result_);
            break;
        }
        case Kind::kUnaryDecr: {
            if (UNLIKELY(operand_->kind() != Kind::kVar
                        && operand_->kind() != Kind::kVersionedVar)) {
                result_ = Value(NullType::BAD_TYPE);
                break;
            }
            result_ = operand_->eval(ctx) - 1;
            auto* varExpr = static_cast<VariableExpression*>(operand_);
            ctx.setVar(varExpr->var(), result_);
            break;
        }
        case Kind::kIsNull: {
            result_ = (operand_->eval(ctx)).isNull() ? true : false;
            break;
        }
        case Kind::kIsNotNull: {
            result_ = (operand_->eval(ctx)).isNull() ? false : true;
            break;
        }
        case Kind::kIsEmpty: {
            result_ = (operand_->eval(ctx)).empty() ? true : false;
            break;
        }
        case Kind::kIsNotEmpty: {
            result_ = (operand_->eval(ctx)).empty() ? false : true;
            break;
        }
       default:
           LOG(FATAL) << "Unknown type: " << kind_;
   }
   return result_;
}

std::string UnaryExpression::toString() const {
    std::string op;
    switch (kind_) {
        case Kind::kUnaryPlus:
            op = "+";
            break;
        case Kind::kUnaryNegate:
            op = "-";
            break;
        case Kind::kUnaryNot:
            op = "!";
            break;
        case Kind::kUnaryIncr:
            op = "++";
            break;
        case Kind::kUnaryDecr:
            op = "--";
            break;
        case Kind::kIsNull:
            return operand_->toString() + " IS NULL";
        case Kind::kIsNotNull:
            return operand_->toString() + " IS NOT NULL";
        case Kind::kIsEmpty:
            return operand_->toString() + " IS EMPTY";
        case Kind::kIsNotEmpty:
            return operand_->toString() + " IS NOT EMPTY";
        default:
            op = "illegal symbol ";
    }
    std::stringstream out;
    out << op << "(" << operand_->toString() << ")";
    return out.str();
}

void UnaryExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

}  // namespace nebula
