/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
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

namespace {
constexpr char INPUT_REF[] = "$-";
constexpr char VAR_REF[] = "$";
constexpr char SRC_REF[] = "$^";
constexpr char DST_REF[] = "$$";
}   // namespace

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
        case kUUID:
            return std::make_unique<UUIDExpression>();
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
        case kAliasProp:
            return std::make_unique<AliasPropertyExpression>();
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

std::string AliasPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += *ref_;
    if (*ref_ != "" && *ref_ != VAR_REF) {
        buf += ".";
    }
    buf += *alias_;
    if (*alias_ != "") {
        buf += ".";
    }
    buf += *prop_;
    return buf;
}

OptVariantType AliasPropertyExpression::eval() const {
    return context_->getters().getAliasProp(*alias_, *prop_);
}

Status AliasPropertyExpression::prepare() {
    context_->addAliasProp(*alias_, *prop_);
    return Status::OK();
}

void AliasPropertyExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}

const char* AliasPropertyExpression::decode(const char *pos, const char *end) {
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

InputPropertyExpression::InputPropertyExpression(std::string *prop) {
    kind_ = kInputProp;
    ref_.reset(new std::string(INPUT_REF));
    alias_.reset(new std::string(""));
    prop_.reset(prop);
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

DestPropertyExpression::DestPropertyExpression(std::string *tag, std::string *prop) {
    kind_ = kDestProp;
    ref_.reset(new std::string(DST_REF));
    alias_.reset(tag);
    prop_.reset(prop);
}

OptVariantType DestPropertyExpression::eval() const {
    return context_->getters().getDstTagProp(*alias_, *prop_);
}


Status DestPropertyExpression::prepare() {
    context_->addDstTagProp(*alias_, *prop_);
    return Status::OK();
}


void DestPropertyExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}


const char* DestPropertyExpression::decode(const char *pos, const char *end) {
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

VariablePropertyExpression::VariablePropertyExpression(std::string *var, std::string *prop) {
    kind_ = kVariableProp;
    ref_.reset(new std::string(VAR_REF));
    alias_.reset(var);
    prop_.reset(prop);
}

OptVariantType VariablePropertyExpression::eval() const {
    return context_->getters().getVariableProp(*prop_);
}

Status VariablePropertyExpression::prepare() {
    context_->addVariableProp(*alias_, *prop_);
    return Status::OK();
}


void VariablePropertyExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}


const char* VariablePropertyExpression::decode(const char *pos, const char *end) {
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


OptVariantType EdgeTypeExpression::eval() const {
    return *alias_;
}

Status EdgeTypeExpression::prepare() {
    context_->addAliasProp(*alias_, *prop_);
    return Status::OK();
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

OptVariantType EdgeSrcIdExpression::eval() const {
    return context_->getters().getAliasProp(*alias_, *prop_);
}

Status EdgeSrcIdExpression::prepare() {
    context_->addAliasProp(*alias_, *prop_);
    return Status::OK();
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

OptVariantType EdgeDstIdExpression::eval() const {
    return context_->getters().getAliasProp(*alias_, *prop_);
}

Status EdgeDstIdExpression::prepare() {
    context_->addAliasProp(*alias_, *prop_);
    return Status::OK();
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

OptVariantType EdgeRankExpression::eval() const {
    return context_->getters().getAliasProp(*alias_, *prop_);
}

Status EdgeRankExpression::prepare() {
    context_->addAliasProp(*alias_, *prop_);
    return Status::OK();
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

SourcePropertyExpression::SourcePropertyExpression(std::string *tag, std::string *prop) {
    kind_ = kSourceProp;
    ref_.reset(new std::string(SRC_REF));
    alias_.reset(tag);
    prop_.reset(prop);
}

OptVariantType SourcePropertyExpression::eval() const {
    return context_->getters().getSrcTagProp(*alias_, *prop_);
}


Status SourcePropertyExpression::prepare() {
    context_->addSrcTagProp(*alias_, *prop_);
    return Status::OK();
}


void SourcePropertyExpression::encode(Cord &cord) const {
    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(alias_->size());
    cord << *alias_;
    cord << static_cast<uint16_t>(prop_->size());
    cord << *prop_;
}


const char* SourcePropertyExpression::decode(const char *pos, const char *end) {
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
            return boost::get<std::string>(operand_);
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
        args.emplace_back(std::move(result.value()));
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

std::string UUIDExpression::toString() const {
    return folly::stringPrintf("uuid(%s)", field_->c_str());
}

OptVariantType UUIDExpression::eval() const {
     auto client = context_->storageClient();
     auto space = context_->space();
     auto uuidResult = client->getUUID(space, *field_).get();
     if (!uuidResult.ok()) {
        LOG(ERROR) << "Get UUID failed for " << toString() << ", status " << uuidResult.status();
        return OptVariantType(Status::Error("Get UUID Failed"));
     }
     auto v = std::move(uuidResult).value();
     for (auto& rc : v.get_result().get_failed_codes()) {
        LOG(ERROR) << "Get UUID failed, error " << static_cast<int32_t>(rc.get_code())
                   << ", part " << rc.get_part_id() << ", str id " << toString();
        return OptVariantType(Status::Error("Get UUID Failed"));
     }
     VLOG(3) << "Get UUID from " << *field_ << " to " << v.get_id();
     return v.get_id();
}

Status UUIDExpression::prepare() {
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
        case ColumnType::INT:
            return "int";
        case ColumnType::STRING:
            return "string";
        case ColumnType::DOUBLE:
            return "double";
        case ColumnType::BIGINT:
            return "bigint";
        case ColumnType::BOOL:
            return "bool";
        case ColumnType::TIMESTAMP:
            return  "timestamp";
        default:
            return "unknown";
    }
}


std::string TypeCastingExpression::toString() const {
    std::string buf;
    buf.reserve(256);

    buf += "(";
    buf += columnTypeToString(type_);
    buf += ")";
    buf += operand_->toString();

    return buf;
}


OptVariantType TypeCastingExpression::eval() const {
    auto result = operand_->eval();
    if (!result.ok()) {
        return result;
    }

    switch (type_) {
        case ColumnType::INT:
        case ColumnType::TIMESTAMP:
            return Expression::toInt(result.value());
        case ColumnType::STRING:
            return Expression::toString(result.value());
        case ColumnType::DOUBLE:
            return Expression::toDouble(result.value());
        case ColumnType::BOOL:
            return Expression::toBool(result.value());
        case ColumnType::BIGINT:
            return Status::Error("Type bigint not supported yet");
    }
    LOG(FATAL) << "casting to unknown type: " << static_cast<int>(type_);
}


Status TypeCastingExpression::prepare() {
    return operand_->prepare();
}


void TypeCastingExpression::encode(Cord &) const {
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
        case XOR:
            buf += '^';
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
            if (isArithmetic(l) && isArithmetic(r)) {
                if (isDouble(l) || isDouble(r)) {
                    return fmod(asDouble(l), asDouble(r));
                }
                return OptVariantType(asInt(l) % asInt(r));
            }
            break;
        case XOR:
            if (isArithmetic(l) && isArithmetic(r)) {
                if (isDouble(l) || isDouble(r)) {
                    return (static_cast<int64_t>(std::round(asDouble(l)))
                                ^ static_cast<int64_t>(std::round(asDouble(r))));
                }
                return OptVariantType(asInt(l) ^ asInt(r));
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
    if (l.which() != r.which()) {
        auto s = implicitCasting(l, r);
        if (!s.ok()) {
            return s;
        }
    }

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

Status RelationalExpression::implicitCasting(VariantType &lhs, VariantType &rhs) const {
    // Rule: bool -> int64_t -> double
    if (lhs.which() == VAR_STR || rhs.which() == VAR_STR) {
        return Status::Error("A string type can not be compared with a non-string type.");
    } else if (lhs.which() == VAR_DOUBLE || rhs.which() == VAR_DOUBLE) {
        lhs = toDouble(lhs);
        rhs = toDouble(rhs);
    } else if (lhs.which() == VAR_INT64 || rhs.which() == VAR_INT64) {
        lhs = toInt(lhs);
        rhs = toInt(rhs);
    } else if (lhs.which() == VAR_BOOL || rhs.which() == VAR_BOOL) {
        // No need do cast here.
    } else {
        // If the variant type is expanded, we should update the rule.
        LOG(FATAL) << "Unknown type: " << lhs.which() << ", " << rhs.which();
    }

    return Status::OK();
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
        case XOR:
            buf += "XOR";
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
    } else if (op_ == OR) {
        if (asBool(left.value())) {
            return OptVariantType(true);
        }
        return OptVariantType(asBool(right.value()));
    } else {
        if (asBool(left.value()) == asBool(right.value())) {
            return OptVariantType(false);
        }
        return OptVariantType(true);
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
