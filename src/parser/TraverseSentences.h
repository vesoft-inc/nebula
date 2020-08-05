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


class LookupSentence final : public Sentence {
public:
    explicit LookupSentence(std::string *from) {
        from_.reset(from);
        kind_ = Kind::kLookup;
    }

    const std::string* from() const {
        return from_.get();
    }

    void setWhereClause(WhereClause *whereClause) {
        whereClause_.reset(whereClause);
    }

    const WhereClause* whereClause() const {
        return whereClause_.get();
    }

    void setYieldClause(YieldClause *clause) {
        yieldClause_.reset(clause);
    }

    const YieldClause* yieldClause() const {
        return yieldClause_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                from_;
    std::unique_ptr<WhereClause>                whereClause_;
    std::unique_ptr<YieldClause>                yieldClause_;
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

    auto left() {
        return left_.get();
    }

    auto right() {
        return right_.get();
    }

    auto op() {
        return op_;
    }

    void setDistinct() {
        distinct_ = true;
    }

    auto distinct() {
        return distinct_;
    }

private:
    Operator                                    op_;
    std::unique_ptr<Sentence>                   left_;
    std::unique_ptr<Sentence>                   right_;
    bool                                        distinct_{false};
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
    FetchVerticesSentence(FetchLabels  *tags,
                          VertexIDList *vidList,
                          YieldClause  *clause) {
        kind_ = Kind::kFetchVertices;
        tags_.reset(tags);
        vidList_.reset(vidList);
        yieldClause_.reset(clause);
    }

    FetchVerticesSentence(FetchLabels  *tags,
                          Expression   *ref,
                          YieldClause  *clause) {
        kind_ = Kind::kFetchVertices;
        tags_.reset(tags);
        vidRef_.reset(ref);
        yieldClause_.reset(clause);
    }

    FetchLabels* tags() const {
        return tags_.get();
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

    std::string toString() const override;

private:
    std::unique_ptr<FetchLabels>    tags_;
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
    FetchEdgesSentence(FetchLabels *edges,
                       EdgeKeys    *keys,
                       YieldClause *clause) {
        kind_ = Kind::kFetchEdges;
        edges_.reset(edges);
        edgeKeys_.reset(keys);
        yieldClause_.reset(clause);
    }

    FetchEdgesSentence(FetchLabels *edges,
                       EdgeKeyRef  *ref,
                       YieldClause *clause) {
        kind_ = Kind::kFetchEdges;
        edges_.reset(edges);
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

    FetchLabels* edges() const {
        return edges_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<FetchLabels>    edges_;
    std::unique_ptr<EdgeKeys>       edgeKeys_;
    std::unique_ptr<EdgeKeyRef>     keyRef_;
    std::unique_ptr<YieldClause>    yieldClause_;
};

class FindPathSentence final : public Sentence {
public:
    explicit FindPathSentence(bool isShortest) {
        kind_ = Kind::kFindPath;
        isShortest_ = isShortest;
    }

    void setFrom(FromClause *clause) {
        from_.reset(clause);
    }

    void setTo(ToClause *clause) {
        to_.reset(clause);
    }

    void setOver(OverClause *clause) {
        over_.reset(clause);
    }

    void setStep(StepClause *clause) {
        step_.reset(clause);
    }

    void setWhere(WhereClause *clause) {
        where_.reset(clause);
    }

    FromClause* from() const {
        return from_.get();
    }

    ToClause* to() const {
        return to_.get();
    }

    OverClause* over() const {
        return over_.get();
    }

    StepClause* step() const {
        return step_.get();
    }

    WhereClause* where() const {
        return where_.get();
    }

    bool isShortest() const {
        return isShortest_;
    }

    std::string toString() const override;

private:
    bool                            isShortest_;
    std::unique_ptr<FromClause>     from_;
    std::unique_ptr<ToClause>       to_;
    std::unique_ptr<OverClause>     over_;
    std::unique_ptr<StepClause>     step_;
    std::unique_ptr<WhereClause>    where_;
};

class LimitSentence final : public Sentence {
public:
    explicit LimitSentence(int64_t offset, int64_t count) : offset_(offset), count_(count) {
        kind_ = Kind::kLimit;
    }

    std::string toString() const override;

    int64_t offset() {
        return offset_;
    }

    int64_t count() {
        return count_;
    }

 private:
    int64_t    offset_{-1};
    int64_t    count_{-1};
};

class YieldSentence final : public Sentence {
public:
    explicit YieldSentence(YieldColumns *fields) {
        DCHECK(fields != nullptr);
        yieldClause_ = std::make_unique<YieldClause>(fields);
        kind_ = Kind::kYield;
    }

    std::vector<YieldColumn*> columns() const {
        return yieldClause_->columns();
    }

    void setWhereClause(WhereClause *clause) {
        whereClause_.reset(clause);
    }

    WhereClause* where() {
        return whereClause_.get();
    }

    YieldClause* yield() {
        return yieldClause_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<YieldClause>               yieldClause_;
    std::unique_ptr<WhereClause>               whereClause_;
};

class GroupBySentence final : public Sentence {
public:
    GroupBySentence() {
        kind_ = Kind::KGroupBy;
    }

    void setGroupClause(GroupClause *clause) {
        groupClause_.reset(clause);
    }

    void setYieldClause(YieldClause *clause) {
        yieldClause_.reset(clause);
    }

    const GroupClause* groupClause() const {
        return groupClause_.get();
    }

    const YieldClause* yieldClause() const {
        return yieldClause_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<GroupClause>   groupClause_;
    std::unique_ptr<YieldClause>   yieldClause_;
};

}   // namespace nebula
#endif  // PARSER_TRAVERSESENTENCES_H_


