/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/ICord.h"
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
        case kVariableVariant:
            return std::make_unique<VariableVariantExpression>();
        case kDestProp:
            return std::make_unique<DestPropertyExpression>();
        case kInputProp:
            return std::make_unique<InputPropertyExpression>();
        default:
            throw Status::Error("Illegal expression kind: %u", kind);
    }
}

bool Expression::encodeCache(ICord<> &cord) const {
    if (cacheable()) {
        PrimaryExpression e(*cache());
        dynamic_cast<Expression*>(&e)->encode(cord);
        return true;
    }
    return false;
}

// static
std::string Expression::encode(Expression *expr) noexcept {
    ICord<> cord;
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
        return expr;
    } catch (const Status &status) {
        return status;
    }
}

std::string AliasPropertyExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    if (ref_ != nullptr) {
        buf += *ref_;
    }
    if (*ref_ != "" && *ref_ != VAR_REF) {
        buf += ".";
    }
    if (alias_ != nullptr) {
        buf += *alias_;
    }
    if (*alias_ != "") {
        buf += ".";
    }
    if (prop_ != nullptr) {
        buf += *prop_;
    }
    return buf;
}

OptVariantType AliasPropertyExpression::eval(Getters &getters) const {
    if (getters.getAliasProp == nullptr) {
        return Status::Error("`getAliasProp' function is not implemented");
    }
    return getters.getAliasProp(*alias_, *prop_);
}

Status AliasPropertyExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    visitor(this);
    return Status::OK();
}

Status AliasPropertyExpression::prepare() {
    context_->addAliasProp(*alias_, *prop_);
    return Status::OK();
}

void AliasPropertyExpression::encode(ICord<> &cord) const {
    if (encodeCache(cord)) return;

    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(ref_->size());
    cord << *ref_;
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

        THROW_IF_NO_SPACE(pos, end, static_cast<uint64_t>(size));
        ref_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, static_cast<uint64_t>(size));
        alias_ = std::make_unique<std::string>(pos, size);
        pos += size;
    }
    {
        THROW_IF_NO_SPACE(pos, end, 2UL);
        auto size = *reinterpret_cast<const uint16_t*>(pos);
        pos += 2;

        THROW_IF_NO_SPACE(pos, end, static_cast<uint64_t>(size));
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


OptVariantType InputPropertyExpression::eval(Getters &getters) const {
    if (getters.getInputProp == nullptr) {
        return Status::Error("`getInputProp' function is not implemented");
    }
    return getters.getInputProp(*prop_);
}

Status
InputPropertyExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    visitor(this);
    return Status::OK();
}

DestPropertyExpression::DestPropertyExpression(std::string *tag, std::string *prop) {
    kind_ = kDestProp;
    ref_.reset(new std::string(DST_REF));
    alias_.reset(tag);
    prop_.reset(prop);
}

OptVariantType DestPropertyExpression::eval(Getters &getters) const {
    if (getters.getDstTagProp == nullptr) {
        return Status::Error("`getDstTagProp' function is not implemented");
    }
    return getters.getDstTagProp(*alias_, *prop_);
}

Status DestPropertyExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    visitor(this);
    return Status::OK();
}

Status DestPropertyExpression::prepare() {
    context_->addDstTagProp(*alias_, *prop_);
    return Status::OK();
}


VariablePropertyExpression::VariablePropertyExpression(std::string *var, std::string *prop) {
    kind_ = kVariableProp;
    ref_.reset(new std::string(VAR_REF));
    alias_.reset(var);
    prop_.reset(prop);
}

OptVariantType VariablePropertyExpression::eval(Getters &getters) const {
    if (getters.getVariableProp == nullptr) {
        return Status::Error("`getVariableProp' function is not implemented");
    }
    return getters.getVariableProp(*prop_);
}

Status VariablePropertyExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    visitor(this);
    return Status::OK();
}


VariableVariantExpression::VariableVariantExpression(std::string *var) {
    kind_ = kVariableVariant;
    var_.reset(var);
}

std::string VariableVariantExpression::toString() const {
    std::string buf;
    buf.reserve(64);
    buf += VAR_REF;
    buf += *var_;
    return buf;
}

Status VariableVariantExpression::prepare() {
    auto status = context_->getVariableVariant(*var_);
    if (status.ok()) {
        cache_ = std::move(status).value();
    }
    return Status::OK();
}

OptVariantType VariableVariantExpression::eval(Getters &getters) const {
    UNUSED(getters);
    if (cache_.hasValue()) {
        return *cache_;
    }
    auto status = context_->getVariableVariant(*var_);
    if (!status.ok()) {
        return status.status();
    }
    return std::move(status).value();
}

Status VariableVariantExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    visitor(this);
    return Status::OK();
}

void VariableVariantExpression::encode(ICord<> &cord) const {
    if (encodeCache(cord)) return;

    cord << kindToInt(kind());
    cord << static_cast<uint16_t>(var_->size());
    cord << *var_;
}

const char* VariableVariantExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    auto size = *reinterpret_cast<const uint16_t*>(pos);
    pos += 2;

    THROW_IF_NO_SPACE(pos, end, static_cast<uint64_t>(size));
    var_ = std::make_unique<std::string>(pos, size);
    pos += size;

    return pos;
}


Status VariablePropertyExpression::prepare() {
    context_->addVariableProp(*alias_, *prop_);
    return Status::OK();
}

OptVariantType EdgeTypeExpression::eval(Getters &getters) const {
    if (getters.getAliasProp == nullptr) {
        return Status::Error("`getAliasProp' function is not implemented");
    }
    return getters.getAliasProp(*alias_, *prop_);
}

Status EdgeTypeExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    visitor(this);
    return Status::OK();
}

Status EdgeTypeExpression::prepare() {
    context_->addAliasProp(*alias_, *prop_);
    return Status::OK();
}


OptVariantType EdgeSrcIdExpression::eval(Getters &getters) const {
    if (getters.getAliasProp == nullptr) {
        return Status::Error("`getAliasProp' function is not implemented");
    }
    return getters.getAliasProp(*alias_, *prop_);
}

Status EdgeSrcIdExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    visitor(this);
    return Status::OK();
}

Status EdgeSrcIdExpression::prepare() {
    context_->addAliasProp(*alias_, *prop_);
    return Status::OK();
}


OptVariantType EdgeDstIdExpression::eval(Getters &getters) const {
    if (getters.getEdgeDstId == nullptr) {
        return Status::Error("`getEdgeDstId' function is not implemented");
    }
    return getters.getEdgeDstId(*alias_);
}

Status EdgeDstIdExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    visitor(this);
    return Status::OK();
}

Status EdgeDstIdExpression::prepare() {
    context_->addAliasProp(*alias_, *prop_);
    return Status::OK();
}


OptVariantType EdgeRankExpression::eval(Getters &getters) const {
    if (getters.getAliasProp == nullptr) {
        return Status::Error("`getAliasProp' function is not implemented");
    }
    return getters.getAliasProp(*alias_, *prop_);
}

Status EdgeRankExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    visitor(this);
    return Status::OK();
}

Status EdgeRankExpression::prepare() {
    context_->addAliasProp(*alias_, *prop_);
    return Status::OK();
}


SourcePropertyExpression::SourcePropertyExpression(std::string *tag, std::string *prop) {
    kind_ = kSourceProp;
    ref_.reset(new std::string(SRC_REF));
    alias_.reset(tag);
    prop_.reset(prop);
}

OptVariantType SourcePropertyExpression::eval(Getters &getters) const {
    if (getters.getSrcTagProp== nullptr) {
        return Status::Error("`getSrcTagProp' function is not implemented");
    }
    return getters.getSrcTagProp(*alias_, *prop_);
}

Status SourcePropertyExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    visitor(this);
    return Status::OK();
}

Status SourcePropertyExpression::prepare() {
    context_->addSrcTagProp(*alias_, *prop_);
    return Status::OK();
}


std::string PrimaryExpression::toString() const {
    char buf[1024];
    switch (operand_.which()) {
        case VAR_INT64:
            snprintf(buf, sizeof(buf), "%ld", boost::get<int64_t>(operand_));
            break;
        case VAR_DOUBLE: {
            int digits10 = std::numeric_limits<double>::digits10;
            std::string fmt = folly::sformat("%.{}lf", digits10);
            snprintf(buf, sizeof(buf), fmt.c_str(), boost::get<double>(operand_));
            break;
        }
        case VAR_BOOL:
            snprintf(buf, sizeof(buf), "%s", boost::get<bool>(operand_) ? "true" : "false");
            break;
        case VAR_STR:
            return boost::get<std::string>(operand_);
    }
    return buf;
}

OptVariantType PrimaryExpression::eval(Getters &getters) const {
    UNUSED(getters);
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

Status PrimaryExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    visitor(this);
    return Status::OK();
}

Status PrimaryExpression::prepare() {
    return Status::OK();
}


void PrimaryExpression::encode(ICord<> &cord) const {
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
    THROW_IF_NO_SPACE(pos, end, 1UL);
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
            THROW_IF_NO_SPACE(pos, end, static_cast<uint64_t>(size));
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

OptVariantType FunctionCallExpression::eval(Getters &getters) const {
    std::vector<VariantType> args;

    for (auto it = args_.cbegin(); it != args_.cend(); ++it) {
        auto result = (*it)->eval(getters);
        if (!result.ok()) {
            return result;
        }
        args.emplace_back(std::move(result.value()));
    }

    // TODO(simon.liu)
    auto r = function_(args);
    return r;
}

Status FunctionCallExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    for (const auto &it : args_) {
        it->traversal(visitor);
    }
    visitor(this);
    return Status::OK();
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


void FunctionCallExpression::encode(ICord<> &cord) const {
    if (encodeCache(cord)) return;

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

    THROW_IF_NO_SPACE(pos, end, static_cast<uint64_t>(size));
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

OptVariantType UUIDExpression::eval(Getters &getters) const {
    UNUSED(getters);
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

Status UUIDExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    visitor(this);
    return Status::OK();
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

OptVariantType UnaryExpression::eval(Getters &getters) const {
    auto value = operand_->eval(getters);
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
        "attempt to perform unary arithmetic on a `{}'",
        VARIANT_TYPE_NAME[value.value().which()])));
}

Status UnaryExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    operand_->traversal(visitor);
    visitor(this);
    return Status::OK();
}

Status UnaryExpression::prepare() {
    return operand_->prepare();
}


void UnaryExpression::encode(ICord<> &cord) const {
    if (encodeCache(cord)) return;

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


OptVariantType TypeCastingExpression::eval(Getters &getters) const {
    auto result = operand_->eval(getters);
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
    }
    LOG(FATAL) << "casting to unknown type: " << static_cast<int>(type_);
}

Status TypeCastingExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    operand_->traversal(visitor);
    visitor(this);
    return Status::OK();
}

Status TypeCastingExpression::prepare() {
    return operand_->prepare();
}


void TypeCastingExpression::encode(ICord<> &cord) const {
    if (encodeCache(cord)) return;

    cord << kindToInt(kind());
    cord << static_cast<uint8_t>(type_);
    operand_->encode(cord);
}


const char* TypeCastingExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    type_ = *reinterpret_cast<const ColumnType*>(pos++);

    operand_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    return operand_->decode(pos, end);
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

OptVariantType ArithmeticExpression::eval(Getters &getters) const {
    auto left = left_->eval(getters);
    auto right = right_->eval(getters);
    if (!left.ok()) {
        return left;
    }

    if (!right.ok()) {
        return right;
    }

    auto l = left.value();
    auto r = right.value();

    static constexpr int64_t maxInt = std::numeric_limits<int64_t>::max();
    static constexpr int64_t minInt = std::numeric_limits<int64_t>::min();

    auto isAddOverflow = [] (int64_t lv, int64_t rv) -> bool {
        if (lv >= 0 && rv >= 0) {
            return maxInt - lv < rv;
        } else if (lv < 0 && rv < 0) {
            return minInt - lv > rv;
        } else {
            return false;
        }
    };

    auto isSubOverflow = [] (int64_t lv, int64_t rv) -> bool {
        if (lv > 0 && rv < 0) {
            return maxInt - lv < -rv;
        } else if (lv < 0 && rv > 0) {
            return minInt - lv > -rv;
        } else {
            return false;
        }
    };

    auto isMulOverflow = [] (int64_t lv, int64_t rv) -> bool {
        if (lv > 0 && rv > 0) {
            return maxInt / lv < rv;
        } else if (lv < 0 && rv < 0) {
            return maxInt / lv > rv;
        } else if (lv > 0 && rv < 0) {
            return minInt / lv > rv;
        } else if (lv < 0 && rv > 0) {
            return minInt / lv < rv;
        } else {
            return false;
        }
    };

    switch (op_) {
        case ADD:
            if (isArithmetic(l) && isArithmetic(r)) {
                if (isDouble(l) || isDouble(r)) {
                    return OptVariantType(asDouble(l) + asDouble(r));
                }
                int64_t lValue = asInt(l);
                int64_t rValue = asInt(r);
                if (isAddOverflow(lValue, rValue)) {
                    return Status::Error(folly::stringPrintf("Out of range %ld + %ld",
                                lValue, rValue));
                }
                return OptVariantType(lValue + rValue);
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
                int64_t lValue = asInt(l);
                int64_t rValue = asInt(r);
                if (isSubOverflow(lValue, rValue)) {
                    return Status::Error(folly::stringPrintf("Out of range %ld - %ld",
                                lValue, rValue));
                }
                return OptVariantType(lValue - rValue);
            }
            break;
        case MUL:
            if (isArithmetic(l) && isArithmetic(r)) {
                if (isDouble(l) || isDouble(r)) {
                    return OptVariantType(asDouble(l) * asDouble(r));
                }
                int64_t lValue = asInt(l);
                int64_t rValue = asInt(r);
                if (isMulOverflow(lValue, rValue)) {
                    return Status::Error("Out of range %ld * %ld", lValue, rValue);
                }
                return OptVariantType(lValue * rValue);
            }
            break;
        case DIV:
            if (isArithmetic(l) && isArithmetic(r)) {
                if (isDouble(l) || isDouble(r)) {
                    if (abs(asDouble(r)) < 1e-8) {
                        // When Null is supported, should be return NULL
                        return Status::Error("Division by zero");
                    }
                    return OptVariantType(asDouble(l) / asDouble(r));
                }

                if (abs(asInt(r)) == 0) {
                    // When Null is supported, should be return NULL
                    return Status::Error("Division by zero");
                }
                return OptVariantType(asInt(l) / asInt(r));
            }
            break;
        case MOD:
            if (isArithmetic(l) && isArithmetic(r)) {
                if (isDouble(l) || isDouble(r)) {
                    if (abs(asDouble(r)) < 1e-8) {
                        // When Null is supported, should be return NULL
                        return Status::Error("Division by zero");
                    }
                    return fmod(asDouble(l), asDouble(r));
                }
                if (abs(asInt(r)) == 0) {
                    // When Null is supported, should be return NULL
                    return Status::Error("Division by zero");
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
        "attempt to perform arithmetic on `{}' with `{}'",
        VARIANT_TYPE_NAME[l.which()], VARIANT_TYPE_NAME[r.which()])));
}

Status ArithmeticExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    left_->traversal(visitor);
    right_->traversal(visitor);
    visitor(this);
    return Status::OK();
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


void ArithmeticExpression::encode(ICord<> &cord) const {
    if (encodeCache(cord)) return;

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
        case CONTAINS:
            buf += " CONTAINS ";
            break;
    }
    buf.append(right_->toString());
    buf += ')';
    return buf;
}

OptVariantType RelationalExpression::eval(Getters &getters) const {
    auto left = left_->eval(getters);
    auto right = right_->eval(getters);

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
        case CONTAINS:
            if (isString(l) && isString(r)) {
                return OptVariantType(contains(asString(l), asString(r)));
            }
    }

    return OptVariantType(Status::Error("Wrong operator"));
}

Status RelationalExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    left_->traversal(visitor);
    right_->traversal(visitor);
    visitor(this);
    return Status::OK();
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


void RelationalExpression::encode(ICord<> &cord) const {
    if (encodeCache(cord)) return;

    cord << kindToInt(kind());
    cord << static_cast<uint8_t>(op_);
    left_->encode(cord);
    right_->encode(cord);
}


const char* RelationalExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    op_ = *reinterpret_cast<const Operator*>(pos++);
    DCHECK(op_ == LT || op_ == LE || op_ == GT ||
            op_ == GE || op_ == EQ || op_ == NE ||
            op_ == CONTAINS);

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

OptVariantType LogicalExpression::eval(Getters &getters) const {
    auto left = left_->eval(getters);
    auto right = right_->eval(getters);

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

Status LogicalExpression::traversal(std::function<void(const Expression*)> visitor) const {
    if (!visitor) {
        return Status::Error("Null visitor.");
    }
    left_->traversal(visitor);
    right_->traversal(visitor);
    visitor(this);
    return Status::OK();
}

Status LogicalExpression::prepare() {
    auto status = left_->prepare();
    if (!status.ok()) {
        return status;
    }
    status = right_->prepare();
    return Status::OK();
}

void LogicalExpression::encode(ICord<> &cord) const {
    if (encodeCache(cord)) return;

    cord << kindToInt(kind());
    cord << static_cast<uint8_t>(op_);
    left_->encode(cord);
    right_->encode(cord);
}


const char* LogicalExpression::decode(const char *pos, const char *end) {
    THROW_IF_NO_SPACE(pos, end, 2UL);
    op_ = *reinterpret_cast<const Operator*>(pos++);
    DCHECK(op_ == AND || op_ == OR || op_ == XOR);

    left_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    pos = left_->decode(pos, end);

    THROW_IF_NO_SPACE(pos, end, 1UL);
    right_ = makeExpr(*reinterpret_cast<const uint8_t*>(pos++));
    return right_->decode(pos, end);
}


#undef THROW_IF_NO_SPACE

}   // namespace nebula
