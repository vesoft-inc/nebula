/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "parser/Expressions.h"

namespace vesoft {

void Expression::print(const ReturnType &value) {
    switch (value.which()) {
        case 0:
            fprintf(stderr, "%ld\n", asInt(value));
            break;
        case 1:
            fprintf(stderr, "%luUL\n", asUInt(value));
            break;
        case 2:
            fprintf(stderr, "%lf\n", asDouble(value));
            break;
        case 3:
            fprintf(stderr, "%d\n", asBool(value));
            break;
        case 4:
            fprintf(stderr, "%s\n", asString(value).c_str());
            break;
    }
}

std::string PropertyExpression::toString() const {
    if (majorPropName_ != nullptr) {
        if (tag_ != nullptr) {
            return *majorPropName_ + "[" + *tag_ + "]" + "." + *minorPropName_;
        }
        return *majorPropName_ + "." + *minorPropName_;
    } else {
        return *minorPropName_;
    }
}

Expression::ReturnType PropertyExpression::eval() const {
    // TODO evaluate property's value
    return toString();
}

std::string PrimaryExpression::toString() const {
    char buf[1024];
    switch (operand_.which()) {
        case 0:
            snprintf(buf, sizeof(buf), "%s", boost::get<bool>(operand_) ? "true" : "false");
            break;
        case 1:
            snprintf(buf, sizeof(buf), "%ld", boost::get<int64_t>(operand_));
            break;
        case 2:
            snprintf(buf, sizeof(buf), "%luUL", boost::get<uint64_t>(operand_));
            break;
        case 3:
            return std::to_string(boost::get<double>(operand_));
        case 4:
            return "\"" + boost::get<std::string>(operand_) + "\"";
    }
    return buf;
}

Expression::ReturnType PrimaryExpression::eval() const {
    switch (operand_.which()) {
        case 0:
            return boost::get<int64_t>(operand_);
            break;
        case 1:
            return boost::get<uint64_t>(operand_);
            break;
        case 2:
            return boost::get<double>(operand_);
            break;
        case 3:
            return boost::get<std::string>(operand_);
    }
    return "Unknown";
}

std::string UnaryExpression::toString() const {
    std::string buf;
    buf.reserve(256);
    switch (op_) {
        case PLUS:
            buf += '+';
            break;
        case MINUS:
            buf += '-';
            break;
        case NOT:
            buf += '!';
            break;
    }
    buf += '(';
    buf.append(operand_->toString());
    buf += ')';
    return buf;
}

Expression::ReturnType UnaryExpression::eval() const {
    // TODO
    auto value = operand_->eval();
    if (op_ == PLUS) {
        return value;
    } else if (op_ == MINUS) {
        if (isInt(value)) {
            return -asInt(value);
        } else if (isUInt(value)) {
            return -asUInt(value);
        } else if (isDouble(value)) {
            return -asDouble(value);
        }
    } else {
        return !asBool(value);
    }
    return value;
}

std::string columnTypeToString(ColumnType type) {
    switch (type) {
        case INT8:
            return "int8";
        case INT16:
            return "int16";
        case INT32:
            return "int32";
        case INT64:
            return "int64";
        case UINT8:
            return "uint8";
        case UINT16:
            return "uint16";
        case UINT32:
            return "uint32";
        case UINT64:
            return "uint64";
        case STRING:
            return "string";
        case DOUBLE:
            return "double";
        case BIGINT:
            return "bigint";
        case BOOL:
            return "bool";
        default:
            return "unknown";
    }
}

std::string TypeCastingExpression::toString() const {
    return "";
}

Expression::ReturnType TypeCastingExpression::eval() const {
    return ReturnType(0UL);
}

std::string ArithmeticExpression::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += '(';
    buf.append(left_->toString());
    switch (op_) {
        case ADD:
            buf += '+';
            break;
        case SUB:
            buf += '-';
            break;
        case MUL:
            buf += '*';
            break;
        case DIV:
            buf += '/';
            break;
        case MOD:
            buf += '%';
            break;
    }
    buf.append(right_->toString());
    buf += ')';
    return buf;
}

Expression::ReturnType ArithmeticExpression::eval() const {
    auto left = left_->eval();
    auto right = right_->eval();
    switch (op_) {
    case ADD:
        assert((isArithmetic(left) && isArithmetic(right))
                || (isString(left) && isString(right)));
        if (isArithmetic(left) && isArithmetic(right)) {
            if (isDouble(left) || isDouble(right)) {
                return asDouble(left) + asDouble(right);
            }
            if (isInt(left) && isInt(right)) {
                return asInt(left) + asInt(right);
            }
            return asUInt(left) + asUInt(right);
        }
        return asString(left) + asString(right);
    case SUB:
        assert(isArithmetic(left) && isArithmetic(right));
        if (isDouble(left) || isDouble(right)) {
            return asDouble(left) - asDouble(right);
        }
        if (isInt(left) && isInt(right)) {
            return asInt(left) - asInt(right);
        }
        return asUInt(left) - asUInt(right);
    case MUL:
        assert(isArithmetic(left) && isArithmetic(right));
        if (isDouble(left) || isDouble(right)) {
            return asDouble(left) * asDouble(right);
        }
        if (isInt(left) && isInt(right)) {
            return asInt(left) * asInt(right);
        }
        return asUInt(left) * asUInt(right);
    case DIV:
        assert(isArithmetic(left) && isArithmetic(right));
        if (isDouble(left) || isDouble(right)) {
            return asDouble(left) / asDouble(right);
        }
        if (isInt(left) && isInt(right)) {
            return asInt(left) / asInt(right);
        }
        return asUInt(left) / asUInt(right);
    case MOD:
        assert(isInt(left) && isInt(right));
        if (isUInt(left) || isUInt(right)) {
            return asUInt(left) / asUInt(right);
        }
        return asInt(left) % asInt(right);
    default:
        assert(false);
    }
}

std::string RelationalExpression::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += '(';
    buf.append(left_->toString());
    switch (op_) {
        case LT:
            buf += '<';
            break;
        case LE:
            buf += '<';
            buf += '=';
            break;
        case GT:
            buf += '>';
            break;
        case GE:
            buf += '>';
            buf += '=';
            break;
        case EQ:
            buf += '=';
            buf += '=';
            break;
        case NE:
            buf += '!';
            buf += '=';
            break;
    }
    buf.append(right_->toString());
    buf += ')';
    return buf;
}

Expression::ReturnType RelationalExpression::eval() const {
    auto left = left_->eval();
    auto right = right_->eval();
    switch (op_) {
        case LT:
            return left < right;
        case LE:
            return left <= right;
        case GT:
            return left > right;
        case GE:
            return left >= right;
        case EQ:
            return left == right;
        case NE:
            return left != right;
    }
    return false;
}

std::string LogicalExpression::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += '(';
    buf.append(left_->toString());
    switch (op_) {
        case AND:
            buf += '&';
            buf += '&';
            break;
        case OR:
            buf += '|';
            buf += '|';
            break;
    }
    buf.append(right_->toString());
    buf += ')';
    return buf;
}

Expression::ReturnType LogicalExpression::eval() const {
    // TODO
    auto left = left_->eval();
    auto right = right_->eval();
    if (op_ == AND) {
        return asBool(left) && asBool(right);
    } else {
        return asBool(left) || asBool(right);
    }
}

}   // namespace vesoft
