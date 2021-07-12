/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/PredicateExpression.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {

std::unordered_map<std::string, PredicateExpression::Type> PredicateExpression::typeMap_ = {
    {"all", Type::ALL},
    {"any", Type::ANY},
    {"single", Type::SINGLE},
    {"none", Type::NONE},
};

const Value& PredicateExpression::evalExists(ExpressionContext& ctx) {
    DCHECK(collection_->kind() == Expression::Kind::kAttribute ||
           collection_->kind() == Expression::Kind::kSubscript);

    auto* attributeExpr = static_cast<BinaryExpression*>(collection_);
    auto& container = attributeExpr->left()->eval(ctx);
    auto& key = attributeExpr->right()->eval(ctx);

    if (!key.isStr()) {
        result_ = Value::kNullBadType;
        return result_;
    }

    switch (container.type()) {
        case Value::Type::VERTEX: {
            result_ = !container.getVertex().value(key.getStr()).isNull();
            break;
        }
        case Value::Type::EDGE: {
            result_ = !container.getEdge().value(key.getStr()).isNull();
            break;
        }
        case Value::Type::MAP: {
            result_ = !container.getMap().at(key.getStr()).isNull();
            break;
        }
        case Value::Type::NULLVALUE: {
            result_ = Value::kNullValue;
            break;
        }
        default: {
            result_ = Value::kNullBadType;
        }
    }
    return result_;
}

const Value& PredicateExpression::eval(ExpressionContext& ctx) {
    if (name_ == "exists") {
        return evalExists(ctx);
    }
    Type type;
    auto iter = typeMap_.find(name_);
    if (iter != typeMap_.end()) {
        type = iter->second;
    } else {
        result_ = Value::kNullBadType;
        return result_;
    }

    auto& listVal = collection_->eval(ctx);
    if (listVal.isNull() || listVal.empty()) {
        result_ = listVal;
        return result_;
    }
    if (!listVal.isList()) {
        result_ = Value::kNullBadType;
        return result_;
    }
    auto& list = listVal.getList();

    switch (type) {
        case Type::ALL: {
            result_ = true;
            for (size_t i = 0; i < list.size(); ++i) {
                auto& v = list[i];
                ctx.setVar(innerVar_, v);
                auto& filterVal = filter_->eval(ctx);
                if (!filterVal.empty() && !filterVal.isNull() && !filterVal.isBool()) {
                    return Value::kNullBadType;
                }
                if (filterVal.empty() || filterVal.isNull() || !filterVal.getBool()) {
                    result_ = false;
                    return result_;
                }
            }
            return result_;
        }
        case Type::ANY: {
            result_ = false;
            for (size_t i = 0; i < list.size(); ++i) {
                auto& v = list[i];
                ctx.setVar(innerVar_, v);
                auto& filterVal = filter_->eval(ctx);
                if (!filterVal.empty() && !filterVal.isNull() && !filterVal.isBool()) {
                    return Value::kNullBadType;
                }
                if (filterVal.isBool() && filterVal.getBool()) {
                    result_ = true;
                    return result_;
                }
            }
            return result_;
        }
        case Type::SINGLE: {
            result_ = false;
            for (size_t i = 0; i < list.size(); ++i) {
                auto& v = list[i];
                ctx.setVar(innerVar_, v);
                auto& filterVal = filter_->eval(ctx);
                if (!filterVal.empty() && !filterVal.isNull() && !filterVal.isBool()) {
                    return Value::kNullBadType;
                }
                if (filterVal.isBool() && filterVal.getBool()) {
                    if (result_ == false) {
                        result_ = true;
                    } else {
                        result_ = false;
                        return result_;
                    }
                }
            }
            return result_;
        }
        case Type::NONE: {
            result_ = true;
            for (size_t i = 0; i < list.size(); ++i) {
                auto& v = list[i];
                ctx.setVar(innerVar_, v);
                auto& filterVal = filter_->eval(ctx);
                if (!filterVal.empty() && !filterVal.isNull() && !filterVal.isBool()) {
                    return Value::kNullBadType;
                }
                if (filterVal.isBool() && filterVal.getBool()) {
                    result_ = false;
                    return result_;
                }
            }
            return result_;
        }
        // no default so the compiler will warning when lack
    }

    result_ = Value::kNullBadType;
    return result_;
}

bool PredicateExpression::operator==(const Expression& rhs) const {
    if (kind() != rhs.kind()) {
        return false;
    }

    const auto& expr = static_cast<const PredicateExpression&>(rhs);

    if (name_ != expr.name_) {
        return false;
    }

    if (hasInnerVar() != expr.hasInnerVar()) {
        return false;
    }

    if (hasInnerVar()) {
        if (innerVar_ != expr.innerVar_) {
            return false;
        }
    }

    if (*collection_ != *expr.collection_) {
        return false;
    }

    if (hasFilter() != expr.hasFilter()) {
        return false;
    }

    if (hasFilter()) {
        if (*filter_ != *expr.filter_) {
            return false;
        }
    }

    return true;
}

Expression* PredicateExpression::clone() const {
    auto expr = PredicateExpression::make(pool_,
                                          name_,
                                          innerVar_,
                                          collection_->clone(),
                                          filter_ != nullptr ? filter_->clone() : nullptr);
    if (hasOriginString()) {
        expr->setOriginString(originString_);
    }
    return expr;
}

void PredicateExpression::writeTo(Encoder& encoder) const {
    encoder << kind_;
    encoder << Value(hasInnerVar());
    encoder << Value(hasFilter());
    encoder << Value(hasOriginString());

    encoder << name_;
    if (hasInnerVar()) {
        encoder << innerVar_;
    }
    encoder << *collection_;
    if (hasFilter()) {
        encoder << *filter_;
    }
    if (hasOriginString()) {
        encoder << originString_;
    }
}

void PredicateExpression::resetFrom(Decoder& decoder) {
    bool hasInnerVar = decoder.readValue().getBool();
    bool hasFilter = decoder.readValue().getBool();
    bool hasString = decoder.readValue().getBool();

    name_ = decoder.readStr();
    if (hasInnerVar) {
        innerVar_ = decoder.readStr();
    }
    collection_ = decoder.readExpression(pool_);
    if (hasFilter) {
        filter_ = decoder.readExpression(pool_);
    }
    if (hasString) {
        originString_ = decoder.readStr();
    }
}

std::string PredicateExpression::toString() const {
    std::string buf;
    buf.reserve(256);

    buf += name_;
    buf += "(";
    if (name_ != "exists") {
        buf += innerVar_;
        buf += " IN ";
        buf += collection_->toString();
        buf += " WHERE ";
        buf += filter_->toString();
    } else {
        buf += collection_->toString();
    }
    buf += ")";

    return buf;
}

void PredicateExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

}   // namespace nebula
