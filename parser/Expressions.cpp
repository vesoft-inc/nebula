/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "parser/Expressions.h"

namespace vesoft {

void ExpressionContext::print() const {
    for (auto &entry : aliasInfo_) {
        FLOG_INFO("Alias `%s': kind of `%s'",
                entry.first.c_str(), aliasKindToString(entry.second.kind_).c_str());
    }
    if (!srcNodePropNames_.empty()) {
    auto srclist = std::accumulate(std::next(srcNodePropNames_.begin()), srcNodePropNames_.end(),
                                   *srcNodePropNames_.begin(), [] (auto &a, auto &b) { return a + ", " + b; });
    FLOG_INFO("Referred source node's properties: %s", srclist.c_str());
    }
    if (!dstNodePropNames_.empty()) {
    auto dstlist = std::accumulate(std::next(dstNodePropNames_.begin()), dstNodePropNames_.end(),
                                   *dstNodePropNames_.begin(), [] (auto &a, auto &b) { return a + ", " + b; });
    FLOG_INFO("Referred destination node's properties: %s", dstlist.c_str());
    }
    if (!edgePropNames_.empty()) {
    auto edgelist = std::accumulate(std::next(edgePropNames_.begin()), edgePropNames_.end(),
                                   *edgePropNames_.begin(), [] (auto &a, auto &b) { return a + ", " + b; });
    FLOG_INFO("Referred edge's properties: %s", edgelist.c_str());
    }
}


Status ExpressionContext::addAliasProp(const std::string &alias, const std::string &prop) {
    auto kind = aliasKind(alias);
    if (kind == AliasKind::Unknown) {
        return Status::Error("Alias `%s' not defined", alias.c_str());
    }
    if (kind == AliasKind::SourceNode) {
        addSrcNodeProp(prop);
        return Status::OK();
    }
    if (kind == AliasKind::Edge) {
        addEdgeProp(prop);
        return Status::OK();
    }
    return Status::Error("Illegal alias `%s'", alias.c_str());
}


std::string aliasKindToString(AliasKind kind) {
    switch (kind) {
        case AliasKind::Unknown:
            return "Unknown";
        case AliasKind::SourceNode:
            return "SourceNode";
        case AliasKind::Edge:
            return "Edge";
        default:
            FLOG_FATAL("Illegal AliasKind");
    }
    return "Unknown";
}


void Expression::print(const VariantType &value) {
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


VariantType PropertyExpression::eval() const {
    // TODO evaluate property's value
    return toString();
}


std::string InputPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$_.";
    buf += *prop_;
    return buf;
}


VariantType InputPropertyExpression::eval() const {
    return toString();
}


std::string InputIdPropertyExpression::toString() const {
    return "$_._id";
}


VariantType InputIdPropertyExpression::eval() const {
    return toString();
}


std::string InputTypePropertyExpression::toString() const {
    return "$_._type";
}


VariantType InputTypePropertyExpression::eval() const {
    return toString();
}


std::string InputTaggedPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$_[";
    buf += *tag_;
    buf += "].";
    buf += *prop_;
    return buf;
}


VariantType InputTaggedPropertyExpression::eval() const {
    return toString();
}


std::string DestPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$$.";
    buf += *prop_;
    return buf;
}


VariantType DestPropertyExpression::eval() const {
    return toString();
}


Status DestPropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    context->addDstNodeProp(*prop_);
    return Status::OK();
}


std::string DestIdPropertyExpression::toString() const {
    return "$$._id";
}


VariantType DestIdPropertyExpression::eval() const {
    return toString();
}


Status DestIdPropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    context_->addDstNodeProp("_id");
    return Status::OK();
}


std::string DestTypePropertyExpression::toString() const {
    return "$$._type";
}


VariantType DestTypePropertyExpression::eval() const {
    return toString();
}


Status DestTypePropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    context_->addDstNodeProp("_type");
    return Status::OK();
}


std::string DestTaggedPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$$[";
    buf += *tag_;
    buf += "].";
    buf += *prop_;
    return buf;
}


VariantType DestTaggedPropertyExpression::eval() const {
    return toString();
}


Status DestTaggedPropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return Status::NotSupported("Tagged property not supported yet");
}


std::string VariablePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$";
    buf += *var_;
    buf += ".";
    buf += *prop_;
    return buf;
}


VariantType VariablePropertyExpression::eval() const {
    return toString();
}


Status VariablePropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return Status::OK();
}


std::string VariableIdPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$";
    buf += *var_;
    buf += "._id";
    return buf;
}


VariantType VariableIdPropertyExpression::eval() const {
    return toString();
}


Status VariableIdPropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return Status::OK();
}


std::string VariableTypePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$";
    buf += *var_;
    buf += "._type";
    return buf;
}


VariantType VariableTypePropertyExpression::eval() const {
    return toString();
}


Status VariableTypePropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return Status::OK();
}


std::string VariableTaggedPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$";
    buf += *var_;
    buf + "[";
    buf += *tag_;
    buf += "].";
    buf += *prop_;
    return buf;
}


VariantType VariableTaggedPropertyExpression::eval() const {
    return toString();
}


Status VariableTaggedPropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return Status::NotSupported("Tagged property not supported yet");
}


std::string VariableTaggedIdPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$";
    buf += *var_;
    buf + "[";
    buf += *tag_;
    buf += "]._id";
    return buf;
}


VariantType VariableTaggedIdPropertyExpression::eval() const {
    return toString();
}


Status VariableTaggedIdPropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return Status::NotSupported("Tagged property not supported yet");
}


std::string VariableTaggedTypePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$";
    buf += *var_;
    buf + "[";
    buf += *tag_;
    buf += "]._type";
    return buf;
}


VariantType VariableTaggedTypePropertyExpression::eval() const {
    return toString();
}


Status VariableTaggedTypePropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return Status::NotSupported("Tagged property not supported yet");
}


std::string AliasPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += ".";
    buf += *prop_;
    return buf;
}


VariantType AliasPropertyExpression::eval() const {
    return toString();
}


Status AliasPropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return context_->addAliasProp(*alias_, *prop_);
}


std::string AliasIdPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "._id";
    return buf;
}


VariantType AliasIdPropertyExpression::eval() const {
    return toString();
}


Status AliasIdPropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return context_->addAliasProp(*alias_, "_id");
}


std::string AliasTypePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "._type";
    return buf;
}


VariantType AliasTypePropertyExpression::eval() const {
    return toString();
}


Status AliasTypePropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return context_->addAliasProp(*alias_, "_type");
}


std::string EdgeSrcIdExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "._src";
    return buf;
}


VariantType EdgeSrcIdExpression::eval() const {
    return toString();
}


Status EdgeSrcIdExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return context_->addAliasProp(*alias_, "_src");
}


std::string EdgeDstIdExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "._dst";
    return buf;
}


VariantType EdgeDstIdExpression::eval() const {
    return toString();
}


Status EdgeDstIdExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return context_->addAliasProp(*alias_, "_dst");
}


std::string EdgeRankExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "._rank";
    return buf;
}


VariantType EdgeRankExpression::eval() const {
    return toString();
}


Status EdgeRankExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return context_->addAliasProp(*alias_, "_rank");
}


std::string AliasTaggedPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "[";
    buf += *tag_;
    buf += "].";
    buf += *prop_;
    return buf;
}


VariantType AliasTaggedPropertyExpression::eval() const {
    return toString();
}


Status AliasTaggedPropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return Status::NotSupported("Tagged property not supported yet");
}


std::string AliasTaggedIdPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "[";
    buf += *tag_;
    buf += "]._id";
    return buf;
}


VariantType AliasTaggedIdPropertyExpression::eval() const {
    return toString();
}


Status AliasTaggedIdPropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return Status::NotSupported("Tagged property not supported yet");
}


std::string AliasTaggedTypePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "[";
    buf += *tag_;
    buf += "]._type";
    return buf;
}


VariantType AliasTaggedTypePropertyExpression::eval() const {
    return toString();
}


Status AliasTaggedTypePropertyExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return Status::NotSupported("Tagged property not supported yet");
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


VariantType PrimaryExpression::eval() const {
    switch (operand_.which()) {
        case 0:
            return boost::get<bool>(operand_);
            break;
        case 1:
            return boost::get<int64_t>(operand_);
            break;
        case 2:
            return boost::get<uint64_t>(operand_);
            break;
        case 3:
            return boost::get<double>(operand_);
            break;
        case 4:
            return boost::get<std::string>(operand_);
    }
    return std::string("Unknown");
}


Status PrimaryExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return Status::OK();
}


std::string UnaryExpression::toString() const {
    std::string buf;
    buf.reserve(256);
    switch (op_) {
        case PLUS:
            buf += '+';
            break;
        case NEGATE:
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


VariantType UnaryExpression::eval() const {
    auto value = operand_->eval();
    if (op_ == PLUS) {
        return value;
    } else if (op_ == NEGATE) {
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


Status UnaryExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return operand_->prepare(context);
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


VariantType TypeCastingExpression::eval() const {
    return VariantType(0UL);
}


Status TypeCastingExpression::prepare(ExpressionContext *context) {
    context_ = context;
    return operand_->prepare(context);
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


VariantType ArithmeticExpression::eval() const {
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
        DCHECK(false);
    }
    return false;
}


Status ArithmeticExpression::prepare(ExpressionContext *context) {
    context_ = context;
    auto status = left_->prepare(context);
    if (!status.ok()) {
        return status;
    }
    status = right_->prepare(context);
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
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
            buf += "<=";
            break;
        case GT:
            buf += '>';
            break;
        case GE:
            buf += ">=";
            break;
        case EQ:
            buf += "==";
            break;
        case NE:
            buf += "!=";
            break;
    }
    buf.append(right_->toString());
    buf += ')';
    return buf;
}


VariantType RelationalExpression::eval() const {
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


Status RelationalExpression::prepare(ExpressionContext *context) {
    context_ = context;
    auto status = left_->prepare(context);
    if (!status.ok()) {
        return status;
    }
    status = right_->prepare(context);
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}


std::string LogicalExpression::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += '(';
    buf.append(left_->toString());
    switch (op_) {
        case AND:
            buf += "&&";
            break;
        case OR:
            buf += "||";
            break;
    }
    buf.append(right_->toString());
    buf += ')';
    return buf;
}


VariantType LogicalExpression::eval() const {
    auto left = left_->eval();
    auto right = right_->eval();
    if (op_ == AND) {
        return asBool(left) && asBool(right);
    } else {
        return asBool(left) || asBool(right);
    }
}


Status LogicalExpression::prepare(ExpressionContext *context) {
    context_ = context;
    auto status = left_->prepare(context);
    if (!status.ok()) {
        return status;
    }
    status = right_->prepare(context);
    return Status::OK();
}

}   // namespace vesoft
