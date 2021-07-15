/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_TRAVERSESENTENCES_H_
#define PARSER_TRAVERSESENTENCES_H_

#include "parser/Clauses.h"
#include "parser/EdgeKey.h"
#include "parser/MutateSentences.h"
#include "parser/Sentence.h"

namespace nebula {

class GoSentence final : public Sentence {
public:
    GoSentence(StepClause* step,
               FromClause* from,
               OverClause* over,
               WhereClause* where,
               TruncateClause* truncate) {
        kind_ = Kind::kGo;
        stepClause_.reset(step);
        fromClause_.reset(from);
        overClause_.reset(over);
        whereClause_.reset(where);
        truncateClause_.reset(truncate);
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

    WhereClause* whereClause() {
        return whereClause_.get();
    }

    const YieldClause* yieldClause() const {
        return yieldClause_.get();
    }

    YieldClause* yieldClause() {
        return yieldClause_.get();
    }

    const TruncateClause* truncateClause() const {
        return truncateClause_.get();
    }

    TruncateClause* truncateClause() {
        return truncateClause_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<StepClause>                 stepClause_;
    std::unique_ptr<FromClause>                 fromClause_;
    std::unique_ptr<OverClause>                 overClause_;
    std::unique_ptr<WhereClause>                whereClause_;
    std::unique_ptr<YieldClause>                yieldClause_;
    std::unique_ptr<TruncateClause>             truncateClause_;
};


class LookupSentence final : public Sentence {
public:
    LookupSentence(std::string* from, WhereClause* where, YieldClause* yield);

    const std::string& from() const {
        return *from_;
    }

    const WhereClause* whereClause() const {
        return whereClause_.get();
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

    Operator op() const {
        return op_;
    }

    void setDistinct() {
        distinct_ = true;
    }

    bool distinct() const {
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
    enum OrderType : uint8_t { ASCEND, DESCEND };

    OrderFactor(Expression* expr, OrderType op) {
        expr_ = expr;
        orderType_ = op;
    }

    Expression* expr() {
        return expr_;
    }

    void setExpr(Expression* expr) {
        expr_ = expr;
    }

    OrderType orderType() {
        return orderType_;
    }

    std::string toString() const;

private:
    Expression* expr_{nullptr};
    OrderType orderType_;
};

class OrderFactors final {
public:
    void addFactor(OrderFactor *factor) {
        factors_.emplace_back(factor);
    }

    auto& factors() {
        return factors_;
    }

    const auto& factors() const {
        return factors_;
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

    auto& factors() {
        return orderFactors_->factors();
    }

    const auto& factors() const {
        return orderFactors_->factors();
    }

    std::string toString() const override;

private:
    std::unique_ptr<OrderFactors>               orderFactors_;
};

class FetchVerticesSentence final : public Sentence {
public:
    FetchVerticesSentence(NameLabelList *tags,
                          VertexIDList *vidList,
                          YieldClause  *clause) {
        kind_ = Kind::kFetchVertices;
        tags_.reset(tags);
        vertices_.reset(new VerticesClause(vidList));
        yieldClause_.reset(clause);
    }

    FetchVerticesSentence(NameLabelList *tags,
                          Expression   *ref,
                          YieldClause  *clause) {
        kind_ = Kind::kFetchVertices;
        tags_.reset(tags);
        vertices_.reset(new VerticesClause(ref));
        yieldClause_.reset(clause);
    }

    explicit FetchVerticesSentence(Expression *ref, YieldClause  *clause) {
        kind_ = Kind::kFetchVertices;
        tags_ = std::make_unique<NameLabelList>();
        vertices_.reset(new VerticesClause(ref));
        yieldClause_.reset(clause);
    }

    explicit FetchVerticesSentence(VertexIDList *vidList, YieldClause  *clause) {
        kind_ = Kind::kFetchVertices;
        tags_ = std::make_unique<NameLabelList>();
        vertices_.reset(new VerticesClause(vidList));
        yieldClause_.reset(clause);
    }

    bool isAllTagProps() {
        return tags_->empty();
    }

    const NameLabelList* tags() const {
        return tags_.get();
    }

    const VerticesClause* vertices() const {
        return vertices_.get();
    }

    YieldClause* yieldClause() const {
        return yieldClause_.get();
    }

    void setYieldClause(YieldClause *clause) {
        yieldClause_.reset(clause);
    }

    std::string toString() const override;

private:
    std::unique_ptr<NameLabelList>  tags_;
    std::unique_ptr<VerticesClause> vertices_;
    std::unique_ptr<YieldClause>    yieldClause_;
};

class FetchEdgesSentence final : public Sentence {
public:
    FetchEdgesSentence(NameLabelList *edge,
                       EdgeKeys      *keys,
                       YieldClause   *clause) {
        kind_ = Kind::kFetchEdges;
        edge_.reset(edge);
        edgeKeys_.reset(keys);
        yieldClause_.reset(clause);
    }

    FetchEdgesSentence(NameLabelList *edge,
                       EdgeKeyRef    *ref,
                       YieldClause   *clause) {
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

    const std::string* edge() const {
        return edge_->front();
    }

    std::size_t edgeSize() const {
        return edge_->size();
    }

    std::string toString() const override;

private:
    std::unique_ptr<NameLabelList>  edge_;
    std::unique_ptr<EdgeKeys>       edgeKeys_;
    std::unique_ptr<EdgeKeyRef>     keyRef_;
    std::unique_ptr<YieldClause>    yieldClause_;
};

class FindPathSentence final : public Sentence {
public:
    FindPathSentence(bool isShortest, bool withProp, bool noLoop) {
        kind_ = Kind::kFindPath;
        isShortest_ = isShortest;
        withProp_ = withProp;
        noLoop_ = noLoop;
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

    bool withProp() const {
        return withProp_;
    }

    bool noLoop() const {
        return noLoop_;
    }

    std::string toString() const override;

private:
    bool                            isShortest_;
    bool                            withProp_;
    bool                            noLoop_;
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
    explicit YieldSentence(YieldColumns *fields, bool distinct = false) {
        DCHECK(fields != nullptr);
        yieldClause_ = std::make_unique<YieldClause>(fields, distinct);
        kind_ = Kind::kYield;
    }

    std::vector<YieldColumn*> columns() const {
        return yieldClause_->columns();
    }

    YieldColumns* yieldColumns() const {
        return yieldClause_->yields();
    }

    void setWhereClause(WhereClause *clause) {
        whereClause_.reset(clause);
    }

    WhereClause* where() const {
        return whereClause_.get();
    }

    YieldClause* yield() const {
        return yieldClause_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<YieldClause>               yieldClause_;
    std::unique_ptr<WhereClause>               whereClause_;
};

class GroupBySentence final : public Sentence {
public:
    GroupBySentence(YieldClause* yield = nullptr,
                    WhereClause* having = nullptr,
                    GroupClause* groupby = nullptr) {
        kind_ = Kind::kGroupBy;
        yieldClause_.reset(yield);
        havingClause_.reset(having);
        groupClause_.reset(groupby);
    }

    void setGroupClause(GroupClause *clause) {
        groupClause_.reset(clause);
    }

    void setHavingClause(WhereClause *clause) {
        havingClause_.reset(clause);
    }

    void setYieldClause(YieldClause *clause) {
        yieldClause_.reset(clause);
    }

    const GroupClause* groupClause() const {
        return groupClause_.get();
    }

    const WhereClause* havingClause() const {
        return havingClause_.get();
    }

    const YieldClause* yieldClause() const {
        return yieldClause_.get();
    }

    std::string toString() const override;

private:
    std::unique_ptr<YieldClause>   yieldClause_;
    std::unique_ptr<WhereClause>   havingClause_;
    std::unique_ptr<GroupClause>   groupClause_;
};

class GetSubgraphSentence final : public Sentence {
public:
    GetSubgraphSentence(bool withProp,
                        StepClause* step,
                        FromClause* from,
                        InBoundClause* in,
                        OutBoundClause* out,
                        BothInOutClause* both) {
        kind_ = Kind::kGetSubgraph;
        withProp_ = withProp;
        step_.reset(step);
        from_.reset(from);
        in_.reset(in);
        out_.reset(out);
        both_.reset(both);
    }

    StepClause* step() const {
        return step_.get();
    }

    bool withProp() const {
        return withProp_;
    }

    FromClause* from() const {
        return from_.get();
    }

    InBoundClause* in() const {
        return in_.get();
    }

    OutBoundClause* out() const {
        return out_.get();
    }

    BothInOutClause* both() const {
        return both_.get();
    }

    std::string toString() const override;

private:
    bool                                withProp_;
    std::unique_ptr<StepClause>         step_;
    std::unique_ptr<FromClause>         from_;
    std::unique_ptr<InBoundClause>      in_;
    std::unique_ptr<OutBoundClause>     out_;
    std::unique_ptr<BothInOutClause>    both_;
};
}   // namespace nebula
#endif  // PARSER_TRAVERSESENTENCES_H_
