/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/Cord.h"
#include "filter/Expressions.h"
#include "filter/FunctionManager.h"


#define THROW_IF_NO_SPACE(POS, END, REQUIRE)                                        \
    do {                                                                            \
        if ((POS) + (REQUIRE) > (END)) {                                            \
            throw Status::Error("Not enough space left, left: %lu bytes, "          \
                                "require: %lu bytes, at: %s:%d", (END) - (POS),     \
                                (REQUIRE), __FILE__, __LINE__);                     \
        }                                                                           \
    } while (false)

namespace nebula {


Status ExpressionContext::addAliasProp(const std::string &alias, const std::string &prop) {
    auto kind = aliasKind(alias);
    if (kind == AliasKind::Unknown) {
        return Status::Error("Alias `%s' not defined", alias.c_str());
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
            fprintf(stderr, "%lf\n", asDouble(value));
            break;
        case 2:
            fprintf(stderr, "%d\n", asBool(value));
            break;
        case 3:
            fprintf(stderr, "%s\n", asString(value).c_str());
            break;
    }
}


std::unique_ptr<Expression> Expression::makeExpr(uint8_t kind) {
    switch (intToKind(kind)) {
        case kPrimary:
            return std::make_unique<PrimaryExpression>();
        case kFunctionCall:
            return std::make_unique<FunctionCallExpression>();
        case kUnary:
            return std::make_unique<UnaryExpression>();
        case kTypeCasting:
            return std::make_unique<TypeCastingExpression>();
        case kArithmetic:
            return std::make_unique<ArithmeticExpression>();
        case kRelational:
            return std::make_unique<RelationalExpression>();
        case kLogical:
            return std::make_unique<LogicalExpression>();
        case kSourceProp:
            return std::make_unique<SourcePropertyExpression>();
        case kEdgeRank:
            return std::make_unique<EdgeRankExpression>();
        case kEdgeDstId:
            return std::make_unique<EdgeDstIdExpression>();
        case kEdgeSrcId:
            return std::make_unique<EdgeSrcIdExpression>();
        case kEdgeType:
            return std::make_unique<EdgeTypeExpression>();
        case kEdgeProp:
            return std::make_unique<EdgePropertyExpression>();
        case kVariableProp:
            return std::make_unique<VariablePropertyExpression>();
        case kDestProp:
            return std::make_unique<DestPropertyExpression>();
        case kInputProp:
            return std::make_unique<InputPropertyExpression>();
        default:
            throw Status::Error("Illegal expression kind: %u", kind);
    }
}


// static
std::string Expression::encode(Expression *expr) noexcept {
    Cord cord(1024);
    expr->encode(cord);
    return cord.str();
}


// static
StatusOr<std::unique_ptr<Expression>>
Expression::decode(folly::StringPiece buffer) noexcept {
    auto *pos = buffer.data();
    auto *end = pos + buffer.size();
    try {
        THROW_IF_NO_SPACE(pos, end, 1UL);
        auto expr = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
        pos = expr->decode(pos, end);
        if (pos != end) {
            return Status::Error("Buffer not consumed up, end: %p, used upto: %p", end, pos);
        }
        return std::move(expr);
    } catch (const Status &status) {
        return status;
    }
}


std::string InputPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$-.";
    buf += *prop_;
    return buf;
}


Status InputPropertyExpression::prepare() {
    context_->addInputProp(*prop_);
    return Status::OK();
}


OptVariantType InputPropertyExpression::eval() const {
    return context_->getters().getInputProp(*prop_);
}


void InputPropertyExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}


const char* InputPropertyExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    auto size = *reinterpret_cast<const uint16_t*>(pos);
    pos += 2;

    THROW_IF_NO_SPACE(pos, end, size);
    prop_ = std::make_unique<std::string>(pos, size);
    pos += size;

    return pos;
}


std::string DestPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$$.";
    buf += *tag_;
    buf += ".";
    buf += *prop_;
    return buf;
}


OptVariantType DestPropertyExpression::eval() const {
    return context_->getters().getDstTagProp(*tag_, *prop_);
}


Status DestPropertyExpression::prepare() {
    context_->addDstTagProp(*tag_, *prop_);
    return Status::OK();
}


void DestPropertyExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(tag_->size());
    cord << *tag_;
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}


const char* DestPropertyExpression::decode(const char *pos, const char *end) {
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        tag_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        prop_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }

    return pos;
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


OptVariantType VariablePropertyExpression::eval() const {
    return context_->getters().getVariableProp(*prop_);
}

Status VariablePropertyExpression::prepare() {
    context_->addVariableProp(*var_, *prop_);
    return Status::OK();
}


void VariablePropertyExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(var_->size());
    cord << *var_;
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}


const char* VariablePropertyExpression::decode(const char *pos, const char *end) {
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        var_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        prop_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }

    return pos;
}


std::string EdgePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += ".";
    buf += *prop_;
    return buf;
}


OptVariantType EdgePropertyExpression::eval() const {
    return context_->getters().getEdgeProp(*prop_);
}


Status EdgePropertyExpression::prepare() {
    return context_->addAliasProp(*alias_, *prop_);
}


void EdgePropertyExpression::encode(Cord &cord) const {
    // TODO(dutor) We better replace `alias_' with integral edge type
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}


const char* EdgePropertyExpression::decode(const char *pos, const char *end) {
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        alias_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        prop_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }

    return pos;
}


std::string EdgeTypeExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "._type";
    return buf;
}

OptVariantType EdgeTypeExpression::eval() const { return OptVariantType(*alias_); }

Status EdgeTypeExpression::prepare() {
    return context_->addAliasProp(*alias_, "_type");
}


void EdgeTypeExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
}


const char* EdgeTypeExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    auto size = *reinterpret_cast<const uint16_t*>(pos);
    pos += 2;

    THROW_IF_NO_SPACE(pos, end, size);
    alias_ = std::make_unique<std::string>(pos, size);
    pos += size;

    return pos;
}


std::string EdgeSrcIdExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "._src";
    return buf;
}


OptVariantType EdgeSrcIdExpression::eval() const {
    return context_->getters().getEdgeProp("_src");
}


Status EdgeSrcIdExpression::prepare() {
    return context_->addAliasProp(*alias_, "_src");
}


void EdgeSrcIdExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
}


const char* EdgeSrcIdExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    auto size = *reinterpret_cast<const uint16_t*>(pos);
    pos += 2;

    THROW_IF_NO_SPACE(pos, end, size);
    alias_ = std::make_unique<std::string>(pos, size);
    pos += size;

    return pos;
}


std::string EdgeDstIdExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "._dst";
    return buf;
}


OptVariantType EdgeDstIdExpression::eval() const {
    return context_->getters().getEdgeProp("_dst");
}


Status EdgeDstIdExpression::prepare() {
    return context_->addAliasProp(*alias_, "_dst");
}


void EdgeDstIdExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
}


const char* EdgeDstIdExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    auto size = *reinterpret_cast<const uint16_t*>(pos);
    pos += 2;

    THROW_IF_NO_SPACE(pos, end, size);
    alias_ = std::make_unique<std::string>(pos, size);
    pos += size;

    return pos;
}


std::string EdgeRankExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *alias_;
    buf += "._rank";
    return buf;
}


OptVariantType EdgeRankExpression::eval() const {
    return context_->getters().getEdgeProp("_rank");
}


Status EdgeRankExpression::prepare() {
    return context_->addAliasProp(*alias_, "_rank");
}


void EdgeRankExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
}


const char* EdgeRankExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    auto size = *reinterpret_cast<const uint16_t*>(pos);
    pos += 2;

    THROW_IF_NO_SPACE(pos, end, size);
    alias_ = std::make_unique<std::string>(pos, size);
    pos += size;

    return pos;
}


std::string SourcePropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += "$^.";
    buf += *tag_;
    buf += ".";
    buf += *prop_;
    return buf;
}


OptVariantType SourcePropertyExpression::eval() const {
    return context_->getters().getSrcTagProp(*tag_, *prop_);
}


Status SourcePropertyExpression::prepare() {
    context_->addSrcTagProp(*tag_, *prop_);
    return Status::OK();
}


void SourcePropertyExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(tag_->size());
    cord << *tag_;
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}


const char* SourcePropertyExpression::decode(const char *pos, const char *end) {
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        tag_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, size);
        prop_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }

    return pos;
}


std::string PrimaryExpression::toString() const {
    char buf[1024];
    switch (operand_.which()) {
        case VAR_INT64:
            snprintf(buf, sizeof(buf), "%ld", boost::get<int64_t>(operand_));
            break;
        case VAR_DOUBLE:
            return std::to_string(boost::get<double>(operand_));
        case VAR_BOOL:
            snprintf(buf, sizeof(buf), "%s", boost::get<bool>(operand_) ? "true" : "false");
            break;
        case VAR_STR:
            return "\"" + boost::get<std::string>(operand_) + "\"";
    }
    return buf;
}

OptVariantType PrimaryExpression::eval() const {
    switch (operand_.which()) {
        case VAR_INT64:
            return boost::get<int64_t>(operand_);
            break;
        case VAR_DOUBLE:
            return boost::get<double>(operand_);
            break;
        case VAR_BOOL:
            return boost::get<bool>(operand_);
            break;
        case VAR_STR:
            return boost::get<std::string>(operand_);
    }

    return OptVariantType(Status::Error("Unknown type"));
}

Status PrimaryExpression::prepare() {
    return Status::OK();
}


void PrimaryExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    uint8_t which = operand_.which();
    cord << which;
    switch (which) {
        case VAR_INT64:
            cord << boost::get<int64_t>(operand_);
            break;
        case VAR_DOUBLE:
            cord << boost::get<double>(operand_);
            break;
        case VAR_BOOL:
            cord << static_cast<uint8_t>(boost::get<bool>(operand_));
            break;
        case VAR_STR: {
            auto &str = boost::get<std::string>(operand_);
            cord << static_cast<uint16_t>(str.size());
            cord << str;
            break;
        }
        default:
            DCHECK(false);
    }
}


const char* PrimaryExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 1);
    auto which = *reinterpret_cast<const uint8_t*>(pos++);
    switch (which) {
        case VAR_INT64:
            THROW_IF_NO_SPACE(pos, end, 8UL);
            operand_ = *reinterpret_cast<const int64_t*>(pos);
            pos += 8;
            break;
        case VAR_DOUBLE:
            THROW_IF_NO_SPACE(pos, end, 8UL);
            operand_ = *reinterpret_cast<const double*>(pos);
            pos += 8;
            break;
        case VAR_BOOL:
            THROW_IF_NO_SPACE(pos, end, 1UL);
            operand_ = *reinterpret_cast<const bool*>(pos++);
            break;
        case VAR_STR: {
            THROW_IF_NO_SPACE(pos, end, 2UL);
            auto size = *reinterpret_cast<const uint16_t*>(pos);
            pos += 2;
            THROW_IF_NO_SPACE(pos, end, size);
            operand_ = std::string(pos, size);
            pos += size;
            break;
        }
        default:
            throw Status::Error("Unknown variant type");
    }
    return pos;
}


std::string FunctionCallExpression::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += *name_;
    buf += "(";
    for (auto &arg : args_) {
        buf += arg->toString();
        buf += ",";
    }
    if (!args_.empty()) {
        buf.resize(buf.size() - 1);
    }
    buf += ")";
    return buf;
}

OptVariantType FunctionCallExpression::eval() const {
    std::vector<VariantType> args;

    for (auto it = args_.cbegin(); it != args_.cend(); ++it) {
        auto result = (*it)->eval();
        if (!result.ok()) {
            return result;
        }
        args.push_back(std::move(result.value()));
    }

    // TODO(simon.liu)
    auto r = function_(args);
    return OptVariantType(r);
}

Status FunctionCallExpression::prepare() {
    auto result = FunctionManager::get(*name_, args_.size());
    if (!result.ok()) {
        return std::move(result).status();
    }

    function_ = std::move(result).value();

    auto status = Status::OK();
    for (auto &arg : args_) {
        status = arg->prepare();
        if (!status.ok()) {
            break;
        }
    }
    return status;
}


void FunctionCallExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());

    cord << static_cast<uint16_t>(name_->size());
    cord << *name_;

    cord << static_cast<uint16_t>(args_.size());
    for (auto &arg : args_) {
        arg->encode(cord);
    }
}


const char* FunctionCallExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    auto size = *reinterpret_cast<const uint16_t*>(pos);
    pos += 2;

    THROW_IF_NO_SPACE(pos, end, size);
    name_ = std::make_unique<std::string>(pos, size);
    pos += size;

    auto count = *reinterpret_cast<const uint16_t*>(pos);
    pos += 2;

    args_.reserve(count);
    for (auto i = 0u; i < count; i++) {
        THROW_IF_NO_SPACE(pos, end, 1UL);
        auto arg = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
        pos = arg->decode(pos, end);
        args_.emplace_back(std::move(arg));
    }
    return pos;
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

OptVariantType UnaryExpression::eval() const {
    auto value = operand_->eval();
    if (value.ok()) {
        if (op_ == PLUS) {
            return value;
        } else if (op_ == NEGATE) {
            if (isInt(value.value())) {
                return OptVariantType(-asInt(value.value()));
            } else if (isDouble(value.value())) {
                return OptVariantType(-asDouble(value.value()));
            }
        } else {
            return OptVariantType(!asBool(value.value()));
        }
    }

    return OptVariantType(Status::Error(folly::sformat(
        "attempt to perform unary arithmetic on a {}", value.value().type().name())));
}

Status UnaryExpression::prepare() {
    return operand_->prepare();
}


void UnaryExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint8_t>(op_);
    operand_->encode(cord);
}


const char* UnaryExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    op_ = *reinterpret_cast<const Operator*>(pos++);
    DCHECK(op_ == PLUS || op_ == NEGATE || op_ == NOT);

    operand_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    return operand_->decode(pos, end);
}


std::string columnTypeToString(ColumnType type) {
    switch (type) {
        case INT:
            return "int";
        case STRING:
            return "string";
        case DOUBLE:
            return "double";
        case BIGINT:
            return "bigint";
        case BOOL:
            return "bool";
        case TIMESTAMP:
            return  "timestamp";
        default:
            return "unknown";
    }
}


std::string TypeCastingExpression::toString() const {
    return "";
}

OptVariantType TypeCastingExpression::eval() const {
    return OptVariantType(toString());
}

Status TypeCastingExpression::prepare() {
    return operand_->prepare();
}


void TypeCastingExpression::encode(Cord &cord) const {
    UNUSED(cord);
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

OptVariantType ArithmeticExpression::eval() const {
    auto left = left_->eval();
    auto right = right_->eval();
    if (!left.ok()) {
        return left;
    }

    if (!right.ok()) {
        return right;
    }

    auto l = left.value();
    auto r = right.value();

    switch (op_) {
        case ADD:
            if (isArithmetic(l) && isArithmetic(r)) {
                if (isDouble(l) || isDouble(r)) {
                    return OptVariantType(asDouble(l) + asDouble(r));
                }
                return OptVariantType(asInt(l) + asInt(r));
            }

            if (isString(l) && isString(r)) {
                return OptVariantType(asString(l) + asString(r));
            }
            break;
        case SUB:
            if (isArithmetic(l) && isArithmetic(r)) {
                if (isDouble(l) || isDouble(r)) {
                    return OptVariantType(asDouble(l) - asDouble(r));
                }
                return OptVariantType(asInt(l) - asInt(r));
            }
            break;
        case MUL:
            if (isArithmetic(l) && isArithmetic(r)) {
                if (isDouble(l) || isDouble(r)) {
                    return OptVariantType(asDouble(l) * asDouble(r));
                }
                return OptVariantType(asInt(l) * asInt(r));
            }
            break;
        case DIV:
            if (isArithmetic(l) && isArithmetic(r)) {
                if (isDouble(l) || isDouble(r)) {
                    return OptVariantType(asDouble(l) / asDouble(r));
                }
                return OptVariantType(asInt(l) / asInt(r));
            }
            break;
        case MOD:
            if (isInt(l) && isInt(r)) {
                return OptVariantType(asInt(l) % asInt(r));
            }
            break;
        default:
            DCHECK(false);
    }

    return OptVariantType(Status::Error(folly::sformat(
        "attempt to perform arithmetic on {} with {}", l.type().name(), r.type().name())));
}

Status ArithmeticExpression::prepare() {
    auto status = left_->prepare();
    if (!status.ok()) {
        return status;
    }
    status = right_->prepare();
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}


void ArithmeticExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint8_t>(op_);
    left_->encode(cord);
    right_->encode(cord);
}


const char* ArithmeticExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    op_ = *reinterpret_cast<const Operator*>(pos++);
    DCHECK(op_ == ADD || op_ == SUB || op_ == MUL || op_ == DIV || op_ == MOD);

    left_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    pos = left_->decode(pos, end);

    THROW_IF_NO_SPACE(pos, end, 1UL);
    right_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    return right_->decode(pos, end);
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

OptVariantType RelationalExpression::eval() const {
    auto left = left_->eval();
    auto right = right_->eval();

    if (!left.ok()) {
        return left;
    }

    if (!right.ok()) {
        return right;
    }

    auto l = left.value();
    auto r = right.value();
    switch (op_) {
        case LT:
            return OptVariantType(l < r);
        case LE:
            return OptVariantType(l <= r);
        case GT:
            return OptVariantType(l > r);
        case GE:
            return OptVariantType(l >= r);
        case EQ:
            if (isArithmetic(l) && isArithmetic(r)) {
                if (isDouble(l) || isDouble(r)) {
                    return OptVariantType(
                        almostEqual(asDouble(l), asDouble(r)));
                }
            }
            return OptVariantType(l == r);
        case NE:
            if (isArithmetic(l) && isArithmetic(r)) {
                if (isDouble(l) || isDouble(r)) {
                    return OptVariantType(
                        !almostEqual(asDouble(l), asDouble(r)));
                }
            }
            return OptVariantType(l != r);
    }

    return OptVariantType(Status::Error("Wrong operator"));
}

Status RelationalExpression::prepare() {
    auto status = left_->prepare();
    if (!status.ok()) {
        return status;
    }
    status = right_->prepare();
    if (!status.ok()) {
        return status;
    }
    return Status::OK();
}


void RelationalExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint8_t>(op_);
    left_->encode(cord);
    right_->encode(cord);
}


const char* RelationalExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    op_ = *reinterpret_cast<const Operator*>(pos++);
    DCHECK(op_ == LT || op_ == LE || op_ == GT || op_ == GE || op_ == EQ || op_ == NE);

    left_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    pos = left_->decode(pos, end);

    THROW_IF_NO_SPACE(pos, end, 1UL);
    right_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    return right_->decode(pos, end);
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

OptVariantType LogicalExpression::eval() const {
    auto left = left_->eval();
    auto right = right_->eval();

    if (!left.ok()) {
        return left;
    }

    if (!right.ok()) {
        return right;
    }

    if (op_ == AND) {
        if (!asBool(left.value())) {
            return OptVariantType(false);
        }
        return OptVariantType(asBool(right.value()));
    } else {
        if (asBool(left.value())) {
            return OptVariantType(true);
        }
        return OptVariantType(asBool(right.value()));
    }
}

Status LogicalExpression::prepare() {
    auto status = left_->prepare();
    if (!status.ok()) {
        return status;
    }
    status = right_->prepare();
    return Status::OK();
}


void LogicalExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint8_t>(op_);
    left_->encode(cord);
    right_->encode(cord);
}


const char* LogicalExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    op_ = *reinterpret_cast<const Operator*>(pos++);
    DCHECK(op_ == AND || op_ == OR);

    left_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    pos = left_->decode(pos, end);

    THROW_IF_NO_SPACE(pos, end, 1UL);
    right_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    return right_->decode(pos, end);
}


#undef THROW_IF_NO_SPACE

}   // namespace nebula
