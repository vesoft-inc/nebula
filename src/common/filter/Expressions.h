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
#include "storage/client/StorageClient.h"
#include <boost/variant.hpp>
#include <folly/futures/Future.h>

namespace nebula {

class Cord;
using OptVariantType = StatusOr<VariantType>;

enum class ColumnType {
    INT, STRING, DOUBLE, BIGINT, BOOL, TIMESTAMP,
};

std::string columnTypeToString(ColumnType type);

class ExpressionContext final {
public:
    using EdgeInfo = boost::variant<std::string, EdgeType>;
    void addSrcTagProp(const std::string &tag, const std::string &prop) {
        tagMap_.emplace(tag, -1);
        srcTagProps_.emplace(tag, prop);
    }

    void addDstTagProp(const std::string &tag, const std::string &prop) {
        tagMap_.emplace(tag, -1);
        dstTagProps_.emplace(tag, prop);
    }

    std::unordered_map<std::string, TagID>& getTagMap() {
        return tagMap_;
    }

    bool getTagId(const std::string &tag, TagID &tagId) const {
        auto tagFound = tagMap_.find(tag);
        if (tagFound == tagMap_.end() || tagFound->second < 0) {
            return false;
        }

        tagId = tagFound->second;
        return true;
    }

    void addVariableProp(const std::string &var, const std::string &prop) {
        variableProps_.emplace(var, prop);
        variables_.emplace(var);
    }

    void addInputProp(const std::string &prop) {
        inputProps_.emplace(prop);
    }

    void addAliasProp(const std::string &alias, const std::string &prop) {
        aliasProps_.emplace(alias, prop);
    }

    bool addEdge(const std::string &alias, EdgeType edgeType) {
        auto it = edgeMap_.find(alias);
        if (it != edgeMap_.end()) {
            return false;
        }
        edgeMap_.emplace(alias, edgeType);
        return true;
    }

    bool getEdgeType(const std::string &alias, EdgeType &edgeType) {
        auto it = edgeMap_.find(alias);
        if (it == edgeMap_.end()) {
            return false;
        }

        edgeType = it->second;
        return true;
    }

    using PropPair = std::pair<std::string, std::string>;

    std::vector<PropPair> srcTagProps() const {
        return std::vector<PropPair>(srcTagProps_.begin(), srcTagProps_.end());
    }

    std::vector<PropPair> dstTagProps() const {
        return std::vector<PropPair>(dstTagProps_.begin(), dstTagProps_.end());
    }

    std::vector<PropPair> aliasProps() const {
        return std::vector<PropPair>(aliasProps_.begin(), aliasProps_.end());
    }

    using VariableProp = std::pair<std::string, std::string>;

    std::vector<VariableProp> variableProps() const {
        return std::vector<VariableProp>(variableProps_.begin(), variableProps_.end());
    }

    const std::unordered_set<std::string>& variables() const {
        return variables_;
    }

    std::vector<std::string> inputProps() const {
        return std::vector<std::string>(inputProps_.begin(), inputProps_.end());
    }

    bool hasSrcTagProp() const {
        return !srcTagProps_.empty();
    }

    bool hasDstTagProp() const {
        return !dstTagProps_.empty();
    }

    bool hasEdgeProp() const {
        return !aliasProps_.empty();
    }

    bool hasVariableProp() const {
        return !variableProps_.empty();
    }

    bool hasInputProp() const {
        return !inputProps_.empty();
    }

    void setStorageClient(nebula::storage::StorageClient *storageClient) {
        storageClient_ = storageClient;
    }

    nebula::storage::StorageClient* storageClient() {
        return storageClient_;
    }


    void setSpace(GraphSpaceID space) {
        space_ = space;
    }

    GraphSpaceID space() {
        return space_;
    }

    struct Getters {
        std::function<OptVariantType()>                                       getEdgeRank;
        std::function<OptVariantType(const std::string&)>                     getInputProp;
        std::function<OptVariantType(const std::string&)>                     getVariableProp;
        std::function<OptVariantType(const std::string&, const std::string&)> getSrcTagProp;
        std::function<OptVariantType(const std::string&, const std::string&)> getDstTagProp;
        std::function<OptVariantType(const std::string&, const std::string&)> getAliasProp;
    };

    Getters& getters() {
        return getters_;
    }

    void print() const;

    bool isOverAllEdge() const { return overAll_; }

    void setOverAllEdge() { overAll_ = true; }

private:
    Getters                                   getters_;
    std::unordered_set<PropPair>              srcTagProps_;
    std::unordered_set<PropPair>              dstTagProps_;
    std::unordered_set<PropPair>              aliasProps_;
    std::unordered_set<VariableProp>          variableProps_;
    std::unordered_set<std::string>           variables_;
    std::unordered_set<std::string>           inputProps_;
    // alias => edgeType
    std::unordered_map<std::string, EdgeType> edgeMap_;
    std::unordered_map<std::string, TagID>    tagMap_;
    bool                                      overAll_{false};
    GraphSpaceID                              space_;
    nebula::storage::StorageClient            *storageClient_{nullptr};
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

    virtual bool isAliasExpression() const {
        return kind_ == kAliasProp;
    }

    virtual bool isTypeCastingExpression() const {
        return kind_ == kTypeCasting;
    }

    virtual bool isFunCallExpression() const {
        return kind_ == kFunctionCall;
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

    // Procedures used to do type conversions only between compatible ones.
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

    // Procedures used to do type casting
    static std::string toString(const VariantType &value) {
        char buf[1024];
        switch (value.which()) {
            case 0:
                return folly::to<std::string>(boost::get<int64_t>(value));
            case 1:
                return folly::to<std::string>(boost::get<double>(value));
            case 2:
                snprintf(buf, sizeof(buf), "%s", boost::get<bool>(value) ? "true" : "false");
                return buf;
            case 3:
                return boost::get<std::string>(value);
        }
        LOG(FATAL) << "unknown type: " << value.which();
    }

    static bool toBool(const VariantType &value) {
        return asBool(value);
    }

    static double toDouble(const VariantType &value) {
        switch (value.which()) {
            case 0:
                return static_cast<double>(boost::get<int64_t>(value));
            case 1:
                return boost::get<double>(value);
            case 2:
                return boost::get<bool>(value) ? 1.0 : 0.0;
            case 3:
                // TODO(dutor) error handling
                return folly::to<double>(boost::get<std::string>(value));
        }
        LOG(FATAL) << "unknown type: " << value.which();
    }

    static int64_t toInt(const VariantType &value) {
        switch (value.which()) {
            case 0:
                return boost::get<int64_t>(value);
            case 1:
                return static_cast<int64_t>(boost::get<double>(value));
            case 2:
                return boost::get<bool>(value) ? 1.0 : 0.0;
            case 3:
                // TODO(dutor) error handling
                return folly::to<int64_t>(boost::get<std::string>(value));
        }
        LOG(FATAL) << "unknown type: " << value.which();
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
        kAliasProp,
        kVariableProp,
        kDestProp,
        kInputProp,
        kUUID,
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
    friend class UUIDExpression;
    friend class TypeCastingExpression;
    friend class ArithmeticExpression;
    friend class RelationalExpression;
    friend class LogicalExpression;
    friend class SourcePropertyExpression;
    friend class EdgeRankExpression;
    friend class EdgeDstIdExpression;
    friend class EdgeSrcIdExpression;
    friend class EdgeTypeExpression;
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

// Alias.any_prop_name, i.e. EdgeName.any_prop_name
class AliasPropertyExpression: public Expression {
public:
    AliasPropertyExpression() {
        kind_ = kAliasProp;
    }

    AliasPropertyExpression(std::string *ref,
                            std::string *alias,
                            std::string *prop) {
        kind_ = kAliasProp;
        ref_.reset(ref);
        alias_.reset(alias);
        prop_.reset(prop);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

    std::string* alias() const {
        return alias_.get();
    }

    std::string* prop() const {
        return prop_.get();
    }

private:
    void encode(Cord &cord) const override;
    const char* decode(const char *pos, const char *end) override;

protected:
    std::unique_ptr<std::string>    ref_;
    std::unique_ptr<std::string>    alias_;
    std::unique_ptr<std::string>    prop_;
};

// $-.any_prop_name or $-
class InputPropertyExpression final : public AliasPropertyExpression {
public:
    InputPropertyExpression() {
        kind_ = kInputProp;
    }

    explicit InputPropertyExpression(std::string *prop);

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;
};


// $$.TagName.any_prop_name
class DestPropertyExpression final : public AliasPropertyExpression {
public:
    DestPropertyExpression() {
        kind_ = kDestProp;
    }

    DestPropertyExpression(std::string *tag, std::string *prop);

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;
};


// $VarName.any_prop_name
class VariablePropertyExpression final : public AliasPropertyExpression {
public:
    VariablePropertyExpression() {
        kind_ = kVariableProp;
    }

    VariablePropertyExpression(std::string *var, std::string *prop);

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;
};


// Alias._type, i.e. EdgeName._type
class EdgeTypeExpression final : public AliasPropertyExpression {
public:
    EdgeTypeExpression() {
        kind_ = kEdgeType;
    }

    explicit EdgeTypeExpression(std::string *alias) {
        kind_ = kEdgeType;
        ref_.reset(new std::string(""));
        alias_.reset(alias);
        prop_.reset(new std::string(_TYPE));
    }

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;
};


// Alias._src, i.e. EdgeName._src
class EdgeSrcIdExpression final : public AliasPropertyExpression {
public:
    EdgeSrcIdExpression() {
        kind_ = kEdgeSrcId;
    }

    explicit EdgeSrcIdExpression(std::string *alias) {
        kind_ = kEdgeSrcId;
        ref_.reset(new std::string(""));
        alias_.reset(alias);
        prop_.reset(new std::string(_SRC));
    }

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;
};


// Alias._dst, i.e. EdgeName._dst
class EdgeDstIdExpression final : public AliasPropertyExpression {
public:
    EdgeDstIdExpression() {
        kind_ = kEdgeDstId;
    }

    explicit EdgeDstIdExpression(std::string *alias) {
        kind_ = kEdgeDstId;
        ref_.reset(new std::string(""));
        alias_.reset(alias);
        prop_.reset(new std::string(_DST));
    }

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;
};


// Alias._rank, i.e. EdgeName._rank
class EdgeRankExpression final : public AliasPropertyExpression {
public:
    EdgeRankExpression() {
        kind_ = kEdgeRank;
    }

    explicit EdgeRankExpression(std::string *alias) {
        kind_ = kEdgeRank;
        ref_.reset(new std::string(""));
        alias_.reset(alias);
        prop_.reset(new std::string(_RANK));
    }

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;
};


// $^.TagName.any_prop_name
class SourcePropertyExpression final : public AliasPropertyExpression {
public:
    SourcePropertyExpression() {
        kind_ = kSourceProp;
    }

    SourcePropertyExpression(std::string *tag, std::string *prop);

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *pos, const char *end) override;
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

    const std::string* name() const {
        return name_.get();
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

// (uuid)expr
class UUIDExpression final : public Expression {
public:
    UUIDExpression() {
        kind_ = kUUID;
    }

    explicit UUIDExpression(std::string *field) {
        kind_ = kUUID;
        field_.reset(field);
    }

    std::string toString() const override;

    OptVariantType eval() const override;

    Status MUST_USE_RESULT prepare() override;

    void setContext(ExpressionContext *ctx) override {
        context_ = ctx;
    }

private:
    void encode(Cord &) const override {
        throw Status::Error("Not supported yet");
    }

    const char* decode(const char *, const char *) override {
        throw Status::Error("Not supported yet");
    }

private:
    std::unique_ptr<std::string>                field_;
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

    const ColumnType getType() const {
        return type_;
    }

private:
    void encode(Cord &cord) const override;

    const char* decode(const char *, const char *) override {
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
        ADD, SUB, MUL, DIV, MOD, XOR
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

    Status implicitCasting(VariantType &lhs, VariantType &rhs) const;

private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 left_;
    std::unique_ptr<Expression>                 right_;
};


// &&, ||
class LogicalExpression final : public Expression {
public:
    enum Operator : uint8_t {
        AND, OR, XOR
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
