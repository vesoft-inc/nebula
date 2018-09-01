/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_ASTTYPES_H_
#define PARSER_ASTTYPES_H_
#include <boost/variant.hpp>

namespace vesoft {

class Expression {
public:
    virtual ~Expression() {}
    virtual std::string toString() const {
        return "";
    }
    using ReturnType = boost::variant<int64_t, uint64_t, double, bool, std::string>;
    virtual ReturnType eval() const = 0;

    static int64_t asInt(const ReturnType &value) {
        return boost::get<int64_t>(value);
    }
    static uint64_t asUInt(const ReturnType &value) {
        return boost::get<uint64_t>(value);
    }
    static double asDouble(const ReturnType &value) {
        if (value.which() == 0) {
            return (double)boost::get<int64_t>(value);
        }
        if (value.which() == 1) {
            return (double)boost::get<uint64_t>(value);
        }
        return boost::get<double>(value);
    }
    static bool asBool(const ReturnType &value) {
        switch (value.which()) {
            case 0:
                return asInt(value) != 0;
            case 1:
                return asUInt(value) != 0;
            case 2:
                return asDouble(value) != 0.0;
            case 3:
                return boost::get<bool>(value);
            case 4:
                return asString(value).empty();
            default:
                assert(false);
        }
    }
    static const std::string& asString(const ReturnType &value) {
        return boost::get<std::string>(value);
    }
    static bool isInt(const ReturnType &value) {
        return value.which() == 0;
    }
    static bool isUInt(const ReturnType &value) {
        return value.which() == 1;
    }
    static bool isDouble(const ReturnType &value) {
        return value.which() == 2;
    }
    static bool isBool(const ReturnType &value) {
        return value.which() == 3;
    }
    static bool isString(const ReturnType &value) {
        return value.which() == 4;
    }
    static bool isArithmetic(const ReturnType &value) {
        return isInt(value) || isUInt(value) || isDouble(value);
    }

    static void print(const ReturnType &value) {
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
};

class PropertyExpression final : public Expression {
public:
    explicit PropertyExpression(std::string *minorProp) {
        minorPropName_.reset(minorProp);
    }

    PropertyExpression(std::string *majorProp, std::string *minorProp) {
        minorPropName_.reset(minorProp);
        majorPropName_.reset(majorProp);
    }

    PropertyExpression(std::string *majorProp, std::string *minorProp, std::string *tag) {
        minorPropName_.reset(minorProp);
        majorPropName_.reset(majorProp);
        tag_.reset(tag);
    }

    ReturnType eval() const override {
        // TODO evaluate property's value
        return toString();
    }

    std::string toString() const override {
        if (majorPropName_ != nullptr) {
            if (tag_ != nullptr) {
                return *majorPropName_ + "[" + *tag_ + "]" + "." + *minorPropName_;
            }
            return *majorPropName_ + "." + *minorPropName_;
        } else {
            return *minorPropName_;
        }
    }

private:
    std::unique_ptr<std::string>                majorPropName_;
    std::unique_ptr<std::string>                minorPropName_;
    std::unique_ptr<std::string>                tag_;
};

class PrimaryExpression final : public Expression {
public:
    using Operand = boost::variant<bool, int64_t, uint64_t, double, std::string>;

    std::string toString() const override {
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

    PrimaryExpression(bool val) {
        operand_ = val;
    }

    PrimaryExpression(int64_t val) {
        operand_ = val;
    }

    PrimaryExpression(uint64_t val) {
        operand_ = val;
    }

    PrimaryExpression(double val) {
        operand_ = val;
    }

    PrimaryExpression(std::string val) {
        operand_ = val;
    }

    ReturnType eval() const override {
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

private:
    Operand                                     operand_;
};

class UnaryExpression final : public Expression {
public:
    enum Operator {
        PLUS, MINUS, NOT
    };

    UnaryExpression(Operator op, Expression *operand) {
        op_ = op;
        operand_.reset(operand);
    }

    std::string toString() const override {
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

    ReturnType eval() const override {
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

private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 operand_;
};

enum ColumnType {
    INT8, INT16, INT32, INT64,
    UINT8, UINT16, UINT32, UINT64,
    STRING, DOUBLE, BIGINT, BOOL,
};

inline std::string columnTypeToString(ColumnType type) {
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

class TypeCastingExpression final : public Expression {
public:
    TypeCastingExpression(ColumnType type, Expression *operand) {
        type_ = type;
        operand_.reset(operand);
    }

    std::string toString() const override {
        return "";
    }

    ReturnType eval() const override {
        return ReturnType(0UL);
    }

private:
    ColumnType                                  type_;
    std::unique_ptr<Expression>                 operand_;
};

class ArithmeticExpression final : public Expression {
public:
    enum Operator {
        ADD, SUB, MUL, DIV, MOD
    };

    ArithmeticExpression(Expression *left, Operator op, Expression *right) {
        op_ = op;
        left_.reset(left);
        right_.reset(right);
    }

    std::string toString() const override {
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

    ReturnType eval() const override {
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

private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 left_;
    std::unique_ptr<Expression>                 right_;
};

class RelationalExpression final : public Expression {
public:
    enum Operator {
        LT, LE, GT, GE, EQ, NE
    };

    RelationalExpression(Expression *left, Operator op, Expression *right) {
        op_ = op;
        left_.reset(left);
        right_.reset(right);
    }

    std::string toString() const override {
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

    ReturnType eval() const override {
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

private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 left_;
    std::unique_ptr<Expression>                 right_;
};

class LogicalExpression final : public Expression {
public:
    enum Operator {
        AND, OR
    };

    LogicalExpression(Expression *left, Operator op, Expression *right) {
        op_ = op;
        left_.reset(left);
        right_.reset(right);
    }

    std::string toString() const override {
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

    ReturnType eval() const override {
        // TODO
        auto left = left_->eval();
        auto right = right_->eval();
        if (op_ == AND) {
            return asBool(left) && asBool(right);
        } else {
            return asBool(left) || asBool(right);
        }
    }

private:
    Operator                                    op_;
    std::unique_ptr<Expression>                 left_;
    std::unique_ptr<Expression>                 right_;
};

class ColumnSpecification final {
public:
    ColumnSpecification(ColumnType type, std::string *name) {
        type_ = type;
        name_.reset(name);
    }

    ColumnSpecification(ColumnType type, std::string *name, int64_t ttl) {
        hasTTL_ = true;
        ttl_ = ttl;
        type_ = type;
        name_.reset(name);
    }

    bool                                        hasTTL_{false};
    int64_t                                     ttl_;
    ColumnType                                  type_;
    std::unique_ptr<std::string>                name_;
};

class ColumnSpecificationList final {
public:
    ColumnSpecificationList() = default;
    void addColumn(ColumnSpecification *column) {
        columns_.emplace_back(column);
    }

    std::vector<std::unique_ptr<ColumnSpecification>> columns_;
};

class Sentence {
public:
    virtual ~Sentence() {}
    virtual std::string toString() const = 0;
};

class StepClause final {
public:
    explicit StepClause(uint64_t steps = 1, bool isUpto = false) {
        steps_ = steps;
        isUpto_ = isUpto;
    }

    bool isUpto() const {
        return isUpto_;
    }

    std::string toString() const {
        std::string buf;
        if (isUpto()) {
            buf += "UPTO ";
        }
        buf += std::to_string(steps_);
        buf += " STEPS";
        return buf;
    }

private:
    uint64_t                                    steps_{1};
    bool                                        isUpto_{false};
};

class SourceNodeList final {
public:
    void addNodeId(uint64_t id) {
        nodes_.push_back(id);
    }

    const std::vector<uint64_t>& nodeIds() const {
        return nodes_;
    }

    std::string toString(bool isRef) const {
        std::string buf;
        if (isRef) {
            buf += "[";
        }
        for (auto id : nodes_) {
            if (isRef) {
                buf += "$";
            }
            buf += std::to_string(id);
            buf += ",";
        }
        buf.resize(buf.size() - 1);
        if (isRef) {
            buf += "]";
        }
        return buf;
    }

private:
    std::vector<uint64_t>                       nodes_;
};

class FromClause final {
public:
    FromClause(SourceNodeList *srcNodeList, std::string *alias, bool isRef = false) {
        srcNodeList_.reset(srcNodeList);
        alias_.reset(alias);
        isRef_ = isRef;
    }

    void setSourceNodeList(SourceNodeList *clause) {
        srcNodeList_.reset(clause);
    }

    SourceNodeList* srcNodeList() const {
        return srcNodeList_.get();
    }

    const std::string& alias() const {
        return *alias_;
    }

    bool isRef() const {
        return isRef_;
    }

    std::string toString() const {
        std::string buf;
        buf += "FROM ";
        buf += srcNodeList_->toString(isRef_);
        buf += " AS ";
        buf += *alias_;
        return buf;
    }

private:
    std::unique_ptr<SourceNodeList>             srcNodeList_;
    std::unique_ptr<std::string>                alias_;
    bool                                        isRef_{false};
};

class OverClause final {
public:
    explicit OverClause(std::string *edge, bool isReversely = false) {
        edge_.reset(edge);
        isReversely_ = isReversely;
    }

    std::string toString() const {
        std::string buf;
        buf += "OVER ";
        buf += *edge_;
        if (isReversely_) {
            buf += " REVERSELY";
        }
        return buf;
    }

private:
    std::unique_ptr<std::string>                edge_;
    bool                                        isReversely_{false};
};

class WhereClause final {
public:
    explicit WhereClause(Expression *filter) {
        filter_.reset(filter);
    }

    std::string toString() const {
        std::string buf;
        buf += "WHERE ";
        buf += filter_->toString();
        return buf;
    }

private:
    std::unique_ptr<Expression>                 filter_;
};

class ColumnList final {
public:
    void addColumn(Expression *column) {
        columns_.emplace_back(column);
    }

    std::string toString() const {
        std::string buf;
        for (auto &expr : columns_) {
            buf += expr->toString();
            buf += ",";
        }
        buf.resize(buf.size() -1 );
        return buf;
    }

private:
    std::vector<std::unique_ptr<Expression>>    columns_;
};

class ReturnClause final {
public:
    explicit ReturnClause(ColumnList *list) {
        columnList_.reset(list);
    }

    std::string toString() const {
        std::string buf;
        buf += "RETURN ";
        buf += columnList_->toString();
        return buf;
    }

private:
    std::unique_ptr<ColumnList>                 columnList_;
};

class GoSentence final : public Sentence {
public:

    void setStepClause(StepClause *clause) {
        stepClause_.reset(clause);
    }

    void setFromClause(FromClause *clause) {
        fromClause_.reset(clause);
    }

    void setOverClause(OverClause *clause) {
        overClause_.reset(clause);
    }

    void setWhereClause(WhereClause *clause) {
        whereClause_.reset(clause);
    }

    void setReturnClause(ReturnClause *clause) {
        returnClause_.reset(clause);
    }

    StepClause* stepClause() const {
        return stepClause_.get();
    }

    FromClause* fromClause() const {
        return fromClause_.get();
    }

    OverClause* overClause() const {
        return overClause_.get();
    }

    WhereClause* whereClause() const {
        return whereClause_.get();
    }

    ReturnClause* returnClause() const {
        return returnClause_.get();
    }

    std::string toString() const override {
        std::string buf;
        buf += "GO ";
        if (stepClause_ != nullptr) {
            buf += stepClause_->toString();
        }
        if (fromClause_ != nullptr) {
            buf += " ";
            buf += fromClause_->toString();
        }
        if (overClause_ != nullptr) {
            buf += " ";
            buf += overClause_->toString();
        }
        if (whereClause_ != nullptr) {
            buf += " ";
            buf += whereClause_->toString();
        }
        if (returnClause_ != nullptr) {
            buf += " ";
            buf += returnClause_->toString();
        }

        return buf;
    }

private:
    std::unique_ptr<StepClause>         stepClause_;
    std::unique_ptr<FromClause>         fromClause_;
    std::unique_ptr<OverClause>         overClause_;
    std::unique_ptr<WhereClause>        whereClause_;
    std::unique_ptr<ReturnClause>       returnClause_;
};

class MatchSentence final : public Sentence {
public:
    std::string toString() const override {
        return "MATCH sentence";
    }
};

class UseSentence final : public Sentence {
public:
    explicit UseSentence(std::string *ns) {
        ns_.reset(ns);
    }
    std::string toString() const override {
        return "USE NAMESPACE " + *ns_;
    }

private:
    std::unique_ptr<std::string>        ns_;
};

class DefineTagSentence final : public Sentence {
public:
    DefineTagSentence(std::string *name, ColumnSpecificationList *columns) {
        name_.reset(name);
        columns_.reset(columns);
    }

    std::string toString() const override {
        std::string buf;
        buf += "DEFINE TAG ";
        buf += *name_;
        buf += " (";
        for (auto &col : columns_->columns_) {
            buf += *col->name_;
            buf += " ";
            buf += columnTypeToString(col->type_);
            if (col->hasTTL_) {
                buf += " TTL = ";
                buf += std::to_string(col->ttl_);
            }
            buf += ",";
        }
        buf.resize(buf.size() - 1);
        buf += ")";
        return buf;
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
};

class DefineEdgeSentence final : public Sentence {
public:
    DefineEdgeSentence(std::string *name, ColumnSpecificationList *columns) {
        name_.reset(name);
        columns_.reset(columns);
    }

    std::string toString() const override {
        std::string buf;
        buf += "DEFINE EDGE ";
        buf += *name_;
        buf += " (";
        for (auto &col : columns_->columns_) {
            buf += *col->name_;
            buf += " ";
            buf += columnTypeToString(col->type_);
            if (col->hasTTL_) {
                buf += " TTL = ";
                buf += std::to_string(col->ttl_);
            }
            buf += ",";
        }
        buf.resize(buf.size() - 1);
        buf += ")";
        return buf;
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
};

class AlterTagSentence final : public Sentence {
public:
    AlterTagSentence(std::string *name, ColumnSpecificationList *columns) {
        name_.reset(name);
        columns_.reset(columns);
    }

    std::string toString() const override {
        std::string buf;
        buf += "ALTER TAG ";
        buf += *name_;
        buf += "(";
        for (auto &col : columns_->columns_) {
            buf += *col->name_;
            buf += " ";
            buf += columnTypeToString(col->type_);
            if (col->hasTTL_) {
                buf += " TTL = ";
                buf += std::to_string(col->ttl_);
            }
            buf += ",";
        }
        buf.resize(buf.size() - 1);
        buf += ")";
        return buf;
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
};

class AlterEdgeSentence final : public Sentence {
public:
    AlterEdgeSentence(std::string *name, ColumnSpecificationList *columns) {
        name_.reset(name);
        columns_.reset(columns);
    }

    std::string toString() const override {
        std::string buf;
        buf += "ALTER EDGE ";
        buf += *name_;
        buf += "(";
        for (auto &col : columns_->columns_) {
            buf += *col->name_;
            buf += " ";
            buf += columnTypeToString(col->type_);
            if (col->hasTTL_) {
                buf += " TTL = ";
                buf += std::to_string(col->ttl_);
            }
            buf += ",";
        }
        buf.resize(buf.size() - 1);
        buf += ")";
        return buf;
    }

private:
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<ColumnSpecificationList>    columns_;
};

class SetSentence final : public Sentence {
public:
    enum Operator {
        UNION, INTERSECT, MINUS
    };

    SetSentence(Sentence *left, Operator op, Sentence *right) {
        left_.reset(left);
        right_.reset(right);
        op_ = op;
    }

    std::string toString() const override {
        std::string buf;
        buf = "(";
        buf += left_->toString();
        switch (op_) {
            case UNION:
                buf += " UNION ";
                break;
            case INTERSECT:
                buf += " INTERSECT ";
                break;
            case MINUS:
                buf += " MINUS ";
                break;
        }
        buf += right_->toString();
        buf += ")";
        return buf;
    }

private:
    Operator                                    op_;
    std::unique_ptr<Sentence>                   left_;
    std::unique_ptr<Sentence>                   right_;
};

class PipedSentence final : public Sentence {
public:
    PipedSentence(Sentence *left, Sentence *right) {
        left_.reset(left);
        right_.reset(right);
    }

    std::string toString() const override {
        std::string buf;
        buf += left_->toString();
        buf += " | ";
        buf += right_->toString();
        return buf;
    }

private:
    //std::vector<std::unique_ptr<Sentence>>      sentences_;
    std::unique_ptr<Sentence>                   left_;
    std::unique_ptr<Sentence>                   right_;
};

class AssignmentSentence final : public Sentence {
public:
    AssignmentSentence(std::string *variable, Sentence *sentence) {
        variable_.reset(variable);
        sentence_.reset(sentence);
    }

    std::string toString() const override {
        std::string buf;
        buf += "$";
        buf += *variable_;
        buf += " = ";
        buf += sentence_->toString();
        return buf;
    }

private:
    std::unique_ptr<std::string>                variable_;
    std::unique_ptr<Sentence>                   sentence_;
};

class PropertyList final {
public:
    void addProp(std::string *propname) {
        properties_.emplace_back(propname);
    }

    std::string toString() const {
        std::string buf;
        for (auto &prop : properties_) {
            buf += *prop;
            buf += ",";
        }
        buf.resize(buf.size() - 1);
        return buf;
    }

private:
    std::vector<std::unique_ptr<std::string>>   properties_;
};

class ValueList final {
public:
    void addValue(Expression *value) {
        values_.emplace_back(value);
    }

    std::string toString() const {
        std::string buf;
        for (auto &expr : values_) {
            buf += expr->toString();
            buf += ",";
        }
        buf.resize(buf.size() - 1);
        return buf;
    }

private:
    std::vector<std::unique_ptr<Expression>>    values_;
};

class InsertVertexSentence final : public Sentence {
public:
    InsertVertexSentence(int64_t id, std::string *vertex, PropertyList *props, ValueList *values, bool overwrite = true) {
        id_ = id;
        vertex_.reset(vertex);
        properties_.reset(props);
        values_.reset(values);
        overwrite_ = overwrite;
    }

    std::string toString() const {
        std::string buf;
        buf += "INSERT VERTEX ";
        buf += *vertex_;
        buf += "(";
        buf += properties_->toString();
        buf += ") VALUES(";
        buf += std::to_string(id_);
        buf += ": ";
        buf += values_->toString();
        buf += ")";
        return buf;
    }

private:
    bool                                        overwrite_{true};
    int64_t                                     id_;
    std::unique_ptr<std::string>                vertex_;
    std::unique_ptr<PropertyList>               properties_;
    std::unique_ptr<ValueList>                  values_;
};

class Statement final {
public:
    explicit Statement(Sentence *sentence) {
        sentences_.emplace_back(sentence);
    }

    void addSentence(Sentence *sentence) {
        sentences_.emplace_back(sentence);
    }

    std::string toString() const {
        std::string buf;
        buf.reserve(1024);
        auto i = 0UL;
        buf += sentences_[i++]->toString();
        for ( ; i < sentences_.size(); i++) {
            buf += "; ";
            buf += sentences_[i]->toString();
        }
        return buf;
    }

private:
    std::vector<std::unique_ptr<Sentence>>        sentences_;
};


}

#endif  // PARSER_ASTTYPES_H_
