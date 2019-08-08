/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_TRAVERSESENTENCES_H_
#define PARSER_TRAVERSESENTENCES_H_

#include "base/Base.h"
#include "parser/Sentence.h"
#include "parser/Clauses.h"
#include "parser/MutateSentences.h"

namespace nebula {

class GoSentence final : public Sentence {
public:
    GoSentence() {
        kind_ = Kind::kGo;
    }

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

    void setYieldClause(YieldClause *clause) {
        yieldClause_.reset(clause);
    }

    const StepClause* stepClause() const {
        return stepClause_.get();
    }

    const FromClause* fromClause() const {
        return fromClause_.get();
    }

    const OverClause* overClause() const {
        return overClause_.get();
    }

    const WhereClause* whereClause() const {
        return whereClause_.get();
    }

    const YieldClause* yieldClause() const {
        return yieldClause_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<StepClause>                 stepClause_;
    std::unique_ptr<FromClause>                 fromClause_;
    std::unique_ptr<OverClause>                 overClause_;
    std::unique_ptr<WhereClause>                whereClause_;
    std::unique_ptr<YieldClause>                yieldClause_;
};


class MatchSentence final : public Sentence {
public:
    MatchSentence() {
        kind_ = Kind::kMatch;
    }

    std::string toString() const override;
};


class FindSentence final : public Sentence {
public:
    FindSentence(std::string *type, PropertyList *props) {
        type_.reset(type);
        properties_.reset(props);
        kind_ = Kind::kFind;
    }

    const std::string* type() const {
        return type_.get();
    }

    std::vector<std::string*> properties() const {
        return properties_->properties();
    }

    void setWhereClause(WhereClause *whereClause) {
        whereClause_.reset(whereClause);
    }

    const WhereClause* whereClause() const {
        return whereClause_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                type_;
    std::unique_ptr<PropertyList>               properties_;
    std::unique_ptr<WhereClause>                whereClause_;
};


class UseSentence final : public Sentence {
public:
    explicit UseSentence(std::string *space) {
        kind_ = Kind::kUse;
        space_.reset(space);
    }

    const std::string* space() const {
        return space_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>        space_;
};


class SetSentence final : public Sentence {
public:
    enum Operator {
        UNION, INTERSECT, MINUS
    };

    SetSentence(Sentence *left, Operator op, Sentence *right) {
        kind_ = Kind::kSet;
        left_.reset(left);
        right_.reset(right);
        op_ = op;
    }

    std::string toString() const override;

private:
    Operator                                    op_;
    std::unique_ptr<Sentence>                   left_;
    std::unique_ptr<Sentence>                   right_;
};


class PipedSentence final : public Sentence {
public:
    PipedSentence(Sentence *left, Sentence *right) {
        kind_ = Kind::kPipe;
        left_.reset(left);
        right_.reset(right);
    }

    Sentence* left() const {
        return left_.get();
    }

    Sentence* right() const {
        return right_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<Sentence>                   left_;
    std::unique_ptr<Sentence>                   right_;
};


class AssignmentSentence final : public Sentence {
public:
    AssignmentSentence(std::string *variable, Sentence *sentence) {
        kind_ = Kind::kAssignment;
        variable_.reset(variable);
        sentence_.reset(sentence);
    }

    const std::string* var() const {
        return variable_.get();
    }

    Sentence* sentence() const {
        return sentence_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                variable_;
    std::unique_ptr<Sentence>                   sentence_;
};

class OrderFactor final {
public:
    enum OrderType : uint8_t {
        ASCEND,
        DESCEND
    };

    OrderFactor(Expression *expr, OrderType op) {
        expr_.reset(expr);
        orderType_ = op;
    }

    Expression* expr() {
        return expr_.get();
    }

    OrderType orderType() {
        return orderType_;
    }

    std::string toString() const;

private:
    std::unique_ptr<Expression>                 expr_;
    OrderType                                   orderType_;
};

class OrderFactors final {
public:
    void addFactor(OrderFactor *factor) {
        factors_.emplace_back(factor);
    }

    std::vector<OrderFactor*> factors() {
        std::vector<OrderFactor*> result;
        result.resize(factors_.size());
        auto get = [] (auto &factor) { return factor.get(); };
        std::transform(factors_.begin(), factors_.end(), result.begin(), get);
        return result;
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<OrderFactor>>   factors_;
};

class OrderBySentence final : public Sentence {
public:
    explicit OrderBySentence(OrderFactors *factors) {
        orderFactors_.reset(factors);
        kind_ = Kind::kOrderBy;
    }

    std::vector<OrderFactor*> factors() {
        return orderFactors_->factors();
    }

    std::string toString() const override;

private:
    std::unique_ptr<OrderFactors>               orderFactors_;
};

class FetchVerticesSentence final : public Sentence {
public:
    FetchVerticesSentence(std::string  *tag,
                          VertexIDList *vidList,
                          YieldClause  *clause) {
        kind_ = Kind::kFetchVertices;
        tag_.reset(tag);
        vidList_.reset(vidList);
        yieldClause_.reset(clause);
    }

    FetchVerticesSentence(std::string  *tag,
                          Expression   *ref,
                          YieldClause  *clause) {
        kind_ = Kind::kFetchVertices;
        tag_.reset(tag);
        vidRef_.reset(ref);
        yieldClause_.reset(clause);
    }

    auto tag() const {
        return tag_.get();
    }

    auto vidList() const {
        return vidList_->vidList();
    }

    bool isRef() const {
        return vidRef_ != nullptr;
    }

    Expression* ref() const {
        return vidRef_.get();
    }

    YieldClause* yieldClause() const {
        return yieldClause_.get();
    }

    void setYieldClause(YieldClause *clause) {
        yieldClause_.reset(clause);
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>    tag_;
    std::unique_ptr<VertexIDList>   vidList_;
    std::unique_ptr<Expression>     vidRef_;
    std::unique_ptr<YieldClause>    yieldClause_;
};

class EdgeKey final {
public:
    EdgeKey(Expression *srcid, Expression *dstid, int64_t rank) {
        srcid_.reset(srcid);
        dstid_.reset(dstid);
        rank_ = rank;
    }

    Expression* srcid() const {
        return srcid_.get();
    }

    Expression* dstid() const {
        return dstid_.get();
    }

    int64_t rank() {
        return rank_;
    }

    std::string toString() const;

private:
    std::unique_ptr<Expression>     srcid_;
    std::unique_ptr<Expression>     dstid_;
    EdgeRanking                     rank_;
};

class EdgeKeys final {
public:
    EdgeKeys() = default;

    void addEdgeKey(EdgeKey *key) {
        keys_.emplace_back(key);
    }

    std::vector<EdgeKey*> keys() {
        std::vector<EdgeKey*> result;
        result.resize(keys_.size());
        auto get = [](const auto&key) { return key.get(); };
        std::transform(keys_.begin(), keys_.end(), result.begin(), get);
        return result;
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<EdgeKey>>   keys_;
};

class EdgeKeyRef final {
public:
    EdgeKeyRef(
            Expression *srcid,
            Expression *dstid,
            Expression *rank,
            bool isInputExpr = true) {
        srcid_.reset(srcid);
        dstid_.reset(dstid);
        rank_.reset(rank);
        isInputExpr_ = isInputExpr;
    }

    StatusOr<std::string> varname() const;

    std::string* srcid();

    std::string* dstid();

    std::string* rank();

    bool isInputExpr() const {
        return isInputExpr_;
    }

    std::string toString() const;

private:
    std::unique_ptr<Expression>             srcid_;
    std::unique_ptr<Expression>             dstid_;
    std::unique_ptr<Expression>             rank_;
    std::unordered_set<std::string>         uniqVar_;
    bool                                    isInputExpr_;
};

class FetchEdgesSentence final : public Sentence {
public:
    FetchEdgesSentence(std::string *edge,
                       EdgeKeys    *keys,
                       YieldClause *clause) {
        kind_ = Kind::kFetchEdges;
        edge_.reset(edge);
        edgeKeys_.reset(keys);
        yieldClause_.reset(clause);
    }

    FetchEdgesSentence(std::string *edge,
                       EdgeKeyRef  *ref,
                       YieldClause *clause) {
        kind_ = Kind::kFetchEdges;
        edge_.reset(edge);
        keyRef_.reset(ref);
        yieldClause_.reset(clause);
    }

    bool isRef() const  {
        return keyRef_ != nullptr;
    }

    void setKeyRef(EdgeKeyRef *ref) {
        keyRef_.reset(ref);
    }

    EdgeKeyRef* ref() const {
        return keyRef_.get();
    }

    void setKeys(EdgeKeys *keys) {
        edgeKeys_.reset(keys);
    }

    EdgeKeys* keys() const {
        return edgeKeys_.get();
    }

    void setYieldClause(YieldClause *clause) {
        yieldClause_.reset(clause);
    }

    YieldClause* yieldClause() const {
        return yieldClause_.get();
    }

    std::string* edge() const {
        return edge_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>    edge_;
    std::unique_ptr<EdgeKeys>       edgeKeys_;
    std::unique_ptr<EdgeKeyRef>     keyRef_;
    std::unique_ptr<YieldClause>    yieldClause_;
};
}   // namespace nebula

#endif  // PARSER_TRAVERSESENTENCES_H_
