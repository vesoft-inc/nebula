/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/TypeCastingExpression.h"
#include "common/expression/ExprVisitor.h"

namespace nebula {

// first:operand's type  second:vType
static std::unordered_multimap<Value::Type, Value::Type> typeCastMap = {
    // cast to INT
    {Value::Type::INT, Value::Type::INT},
    {Value::Type::FLOAT, Value::Type::INT},
    {Value::Type::STRING, Value::Type::INT},
    {Value::Type::__EMPTY__, Value::Type::INT},
    // cast to STRING
    {Value::Type::STRING, Value::Type::STRING},
    {Value::Type::__EMPTY__, Value::Type::STRING},
    {Value::Type::NULLVALUE, Value::Type::STRING},
    {Value::Type::BOOL, Value::Type::STRING},
    {Value::Type::INT, Value::Type::STRING},
    {Value::Type::FLOAT, Value::Type::STRING},
    {Value::Type::DATE, Value::Type::STRING},
    {Value::Type::DATETIME, Value::Type::STRING},
    {Value::Type::MAP, Value::Type::STRING},
    {Value::Type::PATH, Value::Type::STRING},
    {Value::Type::LIST, Value::Type::STRING},
    {Value::Type::SET, Value::Type::STRING},
    {Value::Type::DATASET, Value::Type::STRING},
    {Value::Type::VERTEX, Value::Type::STRING},
    {Value::Type::EDGE, Value::Type::STRING},
    // cast to BOOL
    {Value::Type::BOOL, Value::Type::BOOL},
    {Value::Type::__EMPTY__, Value::Type::BOOL},
    {Value::Type::NULLVALUE, Value::Type::BOOL},
    {Value::Type::INT, Value::Type::BOOL},
    {Value::Type::FLOAT, Value::Type::BOOL},
    {Value::Type::STRING, Value::Type::BOOL},
    {Value::Type::DATE, Value::Type::BOOL},
    // cast to FLOAT
    {Value::Type::FLOAT, Value::Type::FLOAT},
    {Value::Type::INT, Value::Type::FLOAT},
    {Value::Type::STRING, Value::Type::FLOAT},
    {Value::Type::__EMPTY__, Value::Type::FLOAT},
};

// static
bool TypeCastingExpression::validateTypeCast(Value::Type operandType, Value::Type type) {
    auto range = typeCastMap.equal_range(operandType);
    if (range.first == typeCastMap.end() && range.second == typeCastMap.end()) {
        return false;
    }
    for (auto& it = range.first; it != range.second; ++it) {
        if (it->second == type) {
            return true;
        }
    }
    return false;
}

const Value& TypeCastingExpression::eval(ExpressionContext& ctx) {
    DCHECK(!!operand_);
    auto val = operand_->eval(ctx);

    switch (vType_) {
        case Value::Type::BOOL: {
            result_ = val.toBool();
            break;
        }
        case Value::Type::INT: {
            result_ = val.toInt();
            break;
        }
        case Value::Type::FLOAT: {
            result_ = val.toFloat();
            break;
        }
        case Value::Type::STRING: {
            if (val.isStr()) {
                result_.setStr(val.moveStr());
            } else {
                result_.setStr(val.toString());
            }
            break;
        }
        default: {
            LOG(ERROR) << "Can not convert the type of `" << val << "` to `" << vType_ << "`";
            return Value::kNullValue;
        }
    }
    return result_;
}


bool TypeCastingExpression::operator==(const Expression& rhs) const {
    if (kind_ != rhs.kind()) {
        return false;
    }

    const auto& r = static_cast<const TypeCastingExpression&>(rhs);
    return vType_ == r.vType_ && *operand_ == *(r.operand_);
}


void TypeCastingExpression::writeTo(Encoder& encoder) const {
    // kind_
    encoder << kind_;

    // vType_
    encoder << vType_;

    // operand_
    DCHECK(!!operand_);
    encoder << *operand_;
}


void TypeCastingExpression::resetFrom(Decoder& decoder) {
    // Read vType_
    vType_ = decoder.readValueType();

    // Read operand_
    operand_ = decoder.readExpression(pool_);
    CHECK(!!operand_);
}

std::string TypeCastingExpression::toString() const {
    std::stringstream out;
    out << "(" << vType_ << ")" << operand_->toString();
    return out.str();
}

void TypeCastingExpression::accept(ExprVisitor* visitor) {
    visitor->visit(this);
}

}  // namespace nebula
