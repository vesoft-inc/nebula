/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_FILTER_EXPRESSIONS_H_
#define COMMON_FILTER_EXPRESSIONS_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "base/Status.h"

namespace nebula {

class Cord;
using OptVariantType = StatusOr<VariantType>;

enum ColumnType {
    INT, STRING, DOUBLE, BIGINT, BOOL, TIMESTAMP,
};

std::string columnTypeToString(ColumnType type);


enum AliasKind {
    Unknown,
    SourceNode,
    Edge,
};

std::string aliasKindToString(AliasKind kind);

struct AliasInfo {
    AliasKind                               kind_;
    std::string                             data_;
};

class ExpressionContext final {
public:
    void addSrcTagProp(const std::string &tag, const std::string &prop) {
        srcTagProps_.emplace(tag, prop);
    }

    void addDstTagProp(const std::string &tag, const std::string &prop) {
        dstTagProps_.emplace(tag, prop);
    }

    void addEdgeProp(const std::string &prop) {
        edgeProps_.emplace(prop);
    }

    void addVariableProp(const std::string &var, const std::string &prop) {
        variableProps_.emplace(var, prop);
        variables_.emplace(var);
    }

    void addInputProp(const std::string &prop) {
        inputProps_.emplace(prop);
    }

    Status addAliasProp(const std::string &alias, const std::string &prop);

    // TODO(dutor) check duplications on aliases
    void addAlias(const std::string &alias, AliasKind kind) {
        aliasInfo_[alias].kind_ = kind;
    }

    void addAlias(const std::string &alias, AliasKind kind, const std::string &data) {
        auto &entry = aliasInfo_[alias];
        entry.kind_ = kind;
        entry.data_ = data;
    }

    AliasKind aliasKind(const std::string &alias) {
        auto iter = aliasInfo_.find(alias);
        if (iter == aliasInfo_.end()) {
            return AliasKind::Unknown;
        }
        return iter->second.kind_;
    }

    using TagProp = std::pair<std::string, std::string>;

    std::vector<TagProp> srcTagProps() const {
        return std::vector<TagProp>(srcTagProps_.begin(), srcTagProps_.end());
    }

    std::vector<TagProp> dstTagProps() const {
        return std::vector<TagProp>(dstTagProps_.begin(), dstTagProps_.end());
    }

    std::vector<std::string> edgeProps() const {
        return std::vector<std::string>(edgeProps_.begin(), edgeProps_.end());
    }

    using VariableProp = std::pair<std::string, std::string>;

    std::vector<VariableProp> variableProps() const {
        return std::vector<VariableProp>(variableProps_.begin(), variableProps_.end());
    }

    const std::unordered_set<std::string>& variables() const {
        return variables_;
    }

    bool hasSrcTagProp() const {
        return !srcTagProps_.empty();
    }

    bool hasDstTagProp() const {
        return !dstTagProps_.empty();
    }

    bool hasEdgeProp() const {
        return !edgeProps_.empty();
    }

    bool hasVariableProp() const {
        return !variableProps_.empty();
    }

    bool hasInputProp() const {
        return !inputProps_.empty();
    }

    struct Getters {
        std::function<OptVariantType()> getEdgeRank;
        std::function<OptVariantType(const std::string&)> getEdgeProp;
        std::function<OptVariantType(const std::string&)> getInputProp;
        std::function<OptVariantType(const std::string&)> getVariableProp;
        std::function<OptVariantType(const std::string&, const std::string&)> getSrcTagProp;
        std::function<OptVariantType(const std::string&, const std::string&)> getDstTagProp;
    };

    Getters& getters() {
        return getters_;
    }

    void print() const;

private:
    Getters                                     getters_;
    std::unordered_map<std::string, AliasInfo>  aliasInfo_;
    std::unordered_set<TagProp>                 srcTagProps_;
    std::unordered_set<TagProp>                 dstTagProps_;
    std::unordered_set<std::string>             edgeProps_;
    std::unordered_set<VariableProp>            variableProps_;
    std::unordered_set<std::string>             variables_;
    std::unordered_set<std::string>             inputProps_;
};


class Expression {
public:
    virtual ~Expression() {}

    virtual std::string toString() const = 0;

    virtual void setContext(ExpressionContext *context) {
        context_ = context;
    }

    virtual Status MUST_USE_RESULT prepare() = 0;

    virtual OptVariantType eval() const = 0;

    virtual bool isInputExpression() const {
        return kind_ == kInputProp;
    }

    virtual bool isVariableExpression() const {
        return kind_ == kVariableProp;
    }

    /**
     * To encode an expression into a byte buffer.
     *
     * We assume the same byte order on both sides of the buffer
     */
    static std::string encode(Expression *expr) noexcept;

    /**
     * To decode an expression from a byte buffer.
     */
    static StatusOr<std::unique_ptr<Expression>> decode(folly::StringPiece buffer) noexcept;

    static int64_t asInt(const VariantType &value) {
        return boost::get<int64_t>(value);
    }

    static double asDouble(const VariantType &value) {
        if (value.which() == 0) {
            return static_cast<double>(boost::get<int64_t>(value));
        }
        return boost::get<double>(value);
    }

    static bool asBool(const VariantType &value) {
        switch (value.which()) {
            case 0:
                return asInt(value) != 0;
            case 1:
                return asDouble(value) != 0.0;
            case 2:
                return boost::get<bool>(value);
            case 3:
                return asString(value).empty();
            default:
                DCHECK(false);
        }
        return false;
    }

    static const std::string& asString(const VariantType &value) {
        return boost::get<std::string>(value);
    }

    static bool isInt(const VariantType &value) {
        return value.which() == 0;
    }

    static bool isDouble(const VariantType &value) {
        return value.which() == 1;
    }

    static bool isBool(const VariantType &value) {
        return value.which() == 2;
    }

    static bool isString(const VariantType &value) {
        return value.which() == 3;
    }

    static bool isArithmetic(const VariantType &value) {
        return isInt(value) || isDouble(value);
    }

    static bool almostEqual(double left, double right) {
        constexpr auto EPSILON = 1e-8;
        return std::abs(left - right) < EPSILON;
    }

    static void print(const VariantType &value);

    enum Kind : uint8_t {
        kUnknown = 0,

        kPrimary,
        kFunctionCall,
        kUnary,
        kTypeCasting,
        kArithmetic,
        kRelational,
        kLogical,
        kSourceProp,
        kEdgeRank,
        kEdgeDstId,
        kEdgeSrcId,
        kEdgeType,
        kEdgeProp,
        kVariableProp,
        kDestProp,
        kInputProp,

        kMax,
    };

    static_assert(sizeof(Kind) == sizeof(uint8_t), "");

    Kind kind() const {
        return kind_;
    }

protected:
    static uint8_t kindToInt(Kind kind) {
        return static_cast<uint8_t>(kind);
    }

    static Kind intToKind(uint8_t kind) {
        return static_cast<Kind>(kind);
    }

    static std::unique_ptr<Expression> makeExpr(uint8_t kind);

private:
    // Make friend to derived classes,
    // to allow them to call private encode/decode on each other.
    friend class PrimaryExpression;
    friend class UnaryExpression;
    friend class FunctionCallExpression;
    friend class TypeCastingExpression;
    friend class ArithmeticExpression;
    friend class RelationalExpression;
    friend class LogicalExpression;
    friend class SourcePropertyExpression;
    friend class EdgeRankExpression;
    friend class EdgeDstIdExpression;
    friend class EdgeSrcIdExpression;
    friend class EdgeTypeExpression;
    friend class EdgePropertyExpression;
    friend class VariablePropertyExpression;
    friend class InputPropertyExpression;

    virtual void encode(Cord &cord) const = 0;
    /*
     * Decode an expression from within a buffer [pos, end).
     * Return a pointer to where the decoding consumed up.
     * Throw a Status if any error happened.
     */
    virtual const char* decode(const char *pos, const char *end) = 0;


protected:
    ExpressionContext                          *context_{nullptr};
    Kind                                        kind_{kUnknown};
};


// $-.any_prop_name or $-
class InputPropertyExpression final : public Expression {
public:
    InputPropertyExpression() {
        kind_ = kInputProp;
    }

    explicit InputPropertyExpression(std::string *prop) {
        kind_ = kInputProp;
        prop_.reset(prop);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

    std::string* prop() const {
        return prop_.get();
    }

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    std::unique_ptr<std::string>                prop_;
};


// $$[TagName].any_prop_name
class DestPropertyExpression final : public Expression {
public:
    DestPropertyExpression() {
        kind_ = kDestProp;
    }

    DestPropertyExpression(std::string *tag, std::string *prop) {
        kind_ = kDestProp;
        tag_.reset(tag);
        prop_.reset(prop);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    std::unique_ptr<std::string>                tag_;
    std::unique_ptr<std::string>                prop_;
};


// $VarName.any_prop_name
class VariablePropertyExpression final : public Expression {
public:
    VariablePropertyExpression() {
        kind_ = kVariableProp;
    }

    VariablePropertyExpression(std::string *var, std::string *prop) {
        kind_ = kVariableProp;
        var_.reset(var);
        prop_.reset(prop);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

    std::string* var() const {
        return var_.get();
    }

    std::string* prop() const {
        return prop_.get();
    }

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    std::unique_ptr<std::string>                var_;
    std::unique_ptr<std::string>                prop_;
};


// Alias.any_prop_name, i.e. EdgeName.any_prop_name
class EdgePropertyExpression final : public Expression {
public:
    EdgePropertyExpression() {
        kind_ = kEdgeProp;
    }

    EdgePropertyExpression(std::string *alias, std::string *prop) {
        kind_ = kEdgeProp;
        alias_.reset(alias);
        prop_.reset(prop);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

    const std::string& alias() const {
        return *alias_;
    }

    const std::string& prop() const {
        return *prop_;
    }

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    std::unique_ptr<std::string>                alias_;
    std::unique_ptr<std::string>                prop_;
};


// Alias._type, i.e. EdgeName._type
class EdgeTypeExpression final : public Expression {
public:
    EdgeTypeExpression() {
        kind_ = kEdgeType;
    }

    explicit EdgeTypeExpression(std::string *alias) {
        kind_ = kEdgeType;
        alias_.reset(alias);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    std::unique_ptr<std::string>                alias_;
};


// Alias._src, i.e. EdgeName._src
class EdgeSrcIdExpression final : public Expression {
public:
    EdgeSrcIdExpression() {
        kind_ = kEdgeSrcId;
    }

    explicit EdgeSrcIdExpression(std::string *alias) {
        kind_ = kEdgeSrcId;
        alias_.reset(alias);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    std::unique_ptr<std::string>                alias_;
};


// Alias._dst, i.e. EdgeName._dst
class EdgeDstIdExpression final : public Expression {
public:
    EdgeDstIdExpression() {
        kind_ = kEdgeDstId;
    }

    explicit EdgeDstIdExpression(std::string *alias) {
        kind_ = kEdgeDstId;
        alias_.reset(alias);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    std::unique_ptr<std::string>                alias_;
};


// Alias._rank, i.e. EdgeName._rank
class EdgeRankExpression final : public Expression {
public:
    EdgeRankExpression() {
        kind_ = kEdgeRank;
    }

    explicit EdgeRankExpression(std::string *alias) {
        kind_ = kEdgeRank;
        alias_.reset(alias);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    std::unique_ptr<std::string>                alias_;
};


// $^[TagName].any_prop_name
class SourcePropertyExpression final : public Expression {
public:
    SourcePropertyExpression() {
        kind_ = kSourceProp;
    }

    SourcePropertyExpression(std::string *tag, std::string *prop) {
        kind_ = kSourceProp;
        tag_.reset(tag);
        prop_.reset(prop);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

    const std::string& tag() const {
        return *tag_;
    }

    const std::string& prop() const {
        return *prop_;
    }

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    std::unique_ptr<std::string>                tag_;
    std::unique_ptr<std::string>                prop_;
};


// literal constants: bool, integer, double, string
class PrimaryExpression final : public Expression {
public:
    PrimaryExpression() {
        kind_ = kPrimary;
    }

    explicit PrimaryExpression(bool val) {
        kind_ = kPrimary;
        operand_ = val;
    }

    explicit PrimaryExpression(int64_t val) {
        kind_ = kPrimary;
        operand_ = val;
    }

    explicit PrimaryExpression(double val) {
        kind_ = kPrimary;
        operand_ = val;
    }

    explicit PrimaryExpression(std::string val) {
        kind_ = kPrimary;
        operand_ = std::move(val);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    VariantType                                 operand_;
};


class ArgumentList final {
public:
    void addArgument(Expression *arg) {
        args_.emplace_back(arg);
    }

    auto args() {
        return std::move(args_);
    }

private:
    std::vector<std::unique_ptr<Expression>>    args_;
};


class FunctionCallExpression final : public Expression {
public:
    FunctionCallExpression() {
        kind_ = kFunctionCall;
    }

    FunctionCallExpression(std::string *name, ArgumentList *args) {
        kind_ = kFunctionCall;
        name_.reset(name);
        if (args != nullptr) {
            args_ = args->args();
            delete args;
        }
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

    void setContext(ExpressionContext *ctx) override {
        context_ = ctx;
        for (auto &arg : args_) {
            arg->setContext(ctx);
        }
    }

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    std::unique_ptr<std::string>                name_;
    std::vector<std::unique_ptr<Expression>>    args_;
    std::function<VariantType(const std::vector<VariantType>&)> function_;
};


// +expr, -expr, !expr
class UnaryExpression final : public Expression {
public:
    enum Operator : uint8_t {
        PLUS, NEGATE, NOT
    };
    static_assert(sizeof(Operator) == sizeof(uint8_t), "");

    UnaryExpression() {
        kind_ = kUnary;
    }

    UnaryExpression(Operator op, Expression *operand) {
        kind_ = kUnary;
        op_ = op;
        operand_.reset(operand);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

    void setContext(ExpressionContext *context) override {
        Expression::setContext(context);
        operand_->setContext(context);
    }

    const Expression* operand() const {
        return operand_.get();
    }

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 operand_;
};


// (type)expr
class TypeCastingExpression final : public Expression {
public:
    TypeCastingExpression() {
        kind_ = kTypeCasting;
    }

    TypeCastingExpression(ColumnType type, Expression *operand) {
        kind_ = kTypeCasting;
        type_ = type;
        operand_.reset(operand);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

    void setContext(ExpressionContext *context) override {
        Expression::setContext(context);
        operand_->setContext(context);
    }

    const Expression* operand() const {
        return operand_.get();
    }

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *buf, const char *end) override {
        UNUSED(buf);
        UNUSED(end);
        throw Status::Error("Not supported yet");
    }

private:
    ColumnType                                  type_;
    std::unique_ptr<Expression>                 operand_;
};


// +, -, *, /, %
class ArithmeticExpression final : public Expression {
public:
    enum Operator : uint8_t {
        ADD, SUB, MUL, DIV, MOD
    };
    static_assert(sizeof(Operator) == sizeof(uint8_t), "");

    ArithmeticExpression() {
        kind_ = kArithmetic;
    }

    ArithmeticExpression(Expression *left, Operator op, Expression *right) {
        kind_ = kArithmetic;
        op_ = op;
        left_.reset(left);
        right_.reset(right);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

    void setContext(ExpressionContext *context) override {
        Expression::setContext(context);
        left_->setContext(context);
        right_->setContext(context);
    }

    const Expression* left() const {
        return left_.get();
    }

    const Expression* right() const {
        return right_.get();
    }

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 left_;
    std::unique_ptr<Expression>                 right_;
};


// <, <=, >, >=, ==, !=
class RelationalExpression final : public Expression {
public:
    enum Operator : uint8_t {
        LT, LE, GT, GE, EQ, NE
    };
    static_assert(sizeof(Operator) == sizeof(uint8_t), "");

    RelationalExpression() {
        kind_ = kRelational;
    }

    RelationalExpression(Expression *left, Operator op, Expression *right) {
        kind_ = kRelational;
        op_ = op;
        left_.reset(left);
        right_.reset(right);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

    void setContext(ExpressionContext *context) override {
        Expression::setContext(context);
        left_->setContext(context);
        right_->setContext(context);
    }

    const Expression* left() const {
        return left_.get();
    }

    const Expression* right() const {
        return right_.get();
    }

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;


private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 left_;
    std::unique_ptr<Expression>                 right_;
};


// &&, ||
class LogicalExpression final : public Expression {
public:
    enum Operator : uint8_t {
        AND, OR
    };
    static_assert(sizeof(Operator) == sizeof(uint8_t), "");

    LogicalExpression() {
        kind_ = kLogical;
    }

    LogicalExpression(Expression *left, Operator op, Expression *right) {
        kind_ = kLogical;
        op_ = op;
        left_.reset(left);
        right_.reset(right);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

    void setContext(ExpressionContext *context) override {
        Expression::setContext(context);
        left_->setContext(context);
        right_->setContext(context);
    }

    const Expression* left() const {
        return left_.get();
    }

    const Expression* right() const {
        return right_.get();
    }

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;

private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 left_;
    std::unique_ptr<Expression>                 right_;
};

}   // namespace nebula

#endif  // COMMON_FILTER_EXPRESSIONS_H_
