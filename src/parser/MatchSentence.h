/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PARSER_MATCHSENTENCE_H_
#define PARSER_MATCHSENTENCE_H_

#include "common/expression/ContainerExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "parser/Sentence.h"
#include "parser/TraverseSentences.h"
#include "parser/Clauses.h"

namespace nebula {

class MatchEdgeTypeList final {
public:
    MatchEdgeTypeList() = default;

    void add(std::string *item) {
        items_.emplace_back(item);
    }

    auto items() && {
        return std::move(items_);
    }

private:
    std::vector<std::unique_ptr<std::string>>           items_;
};


class MatchStepRange final {
public:
    explicit MatchStepRange(int64_t min, int64_t max = std::numeric_limits<int64_t>::max()) {
        min_ = min;
        max_ = max;
    }

    auto min() const {
        return min_;
    }

    auto max() const {
        return max_;
    }

private:
    int64_t         min_{1};
    int64_t         max_{1};
};


class MatchEdgeProp final {
public:
    MatchEdgeProp(const std::string &alias,
                  MatchEdgeTypeList *types,
                  MatchStepRange *range,
                  Expression *props = nullptr) {
        alias_ = alias;
        range_.reset(range);
        props_ = static_cast<MapExpression*>(props);
        if (types != nullptr) {
            types_ = std::move(*types).items();
            delete types;
        }
    }

    auto get() && {
        return std::make_tuple(std::move(alias_),
                               std::move(types_),
                               std::move(range_),
                               std::move(props_));
    }

private:
    std::string                                         alias_;
    std::vector<std::unique_ptr<std::string>>           types_;
    MapExpression*                                      props_{nullptr};
    std::unique_ptr<MatchStepRange>                     range_;
};


class MatchEdge final {
public:
    using Direction = nebula::storage::cpp2::EdgeDirection;
    MatchEdge(MatchEdgeProp *prop, Direction direction) {
        if (prop != nullptr) {
            auto tuple = std::move(*prop).get();
            alias_ = std::move(std::get<0>(tuple));
            types_ = std::move(std::get<1>(tuple));
            range_ = std::move(std::get<2>(tuple));
            props_ = std::move(std::get<3>(tuple));
            delete prop;
        }
        direction_ = direction;
    }

    auto direction() const {
        return direction_;
    }

    const std::string& alias() const {
        return alias_;
    }

    auto& types() const {
        return types_;
    }

    const MapExpression* props() const {
        return props_;
    }

    auto* range() const {
        return range_.get();
    }

    std::string toString() const;

private:
    Direction                                       direction_;
    std::string                                     alias_;
    std::vector<std::unique_ptr<std::string>>       types_;
    std::unique_ptr<MatchStepRange>                 range_;
    MapExpression*                                  props_{nullptr};
};

class MatchNodeLabel final {
public:
    explicit MatchNodeLabel(std::string *label, Expression *props = nullptr) :
        label_(label), props_(static_cast<MapExpression*>(props)) {
            DCHECK(props == nullptr || props->kind() == Expression::Kind::kMap);
    }

    const std::string* label() const {
        return label_.get();
    }

    const MapExpression* props() const {
        return props_;
    }

    MapExpression* props() {
        return props_;
    }

    std::string toString() const {
        std::stringstream ss;
        ss << ":" << *label_;
        if (props_ != nullptr) {
            ss << props_->toString();
        }
        return ss.str();
    }

private:
    std::unique_ptr<std::string>    label_;
    MapExpression*                  props_{nullptr};
};

class MatchNodeLabelList final {
public:
    void add(MatchNodeLabel *label) {
        labels_.emplace_back(label);
    }

    const auto& labels() const {
        return labels_;
    }

    std::string toString() const {
        std::stringstream ss;
        for (const auto &label : labels_) {
            ss << label->toString();
        }
        return ss.str();
    }

private:
    std::vector<std::unique_ptr<MatchNodeLabel>> labels_;
};

class MatchNode final {
public:
    MatchNode(const std::string &alias,
              MatchNodeLabelList *labels,
              Expression *props = nullptr) {
        alias_ = alias;
        labels_.reset(labels);
        props_ = static_cast<MapExpression*>(props);
    }

    const std::string& alias() const {
        return alias_;
    }

    const auto* labels() const {
        return labels_.get();
    }

    const MapExpression* props() const {
        return props_;
    }

    std::string toString() const;

private:
    std::string                                     alias_;
    std::unique_ptr<MatchNodeLabelList>             labels_;
    MapExpression*                                  props_{nullptr};
};


class MatchPath final {
public:
    explicit MatchPath(MatchNode *node) {
        nodes_.emplace_back(node);
    }

    void add(MatchEdge *edge, MatchNode *node) {
        edges_.emplace_back(edge);
        nodes_.emplace_back(node);
    }

    void setAlias(std::string *alias) {
        alias_.reset(alias);
    }

    const std::string* alias() const {
        return alias_.get();
    }

    const auto& nodes() const {
        return nodes_;
    }

    const auto& edges() const {
        return edges_;
    }

    size_t steps() const {
        return edges_.size();
    }

    const MatchNode* node(size_t i) const {
        return nodes_[i].get();
    }

    const MatchEdge* edge(size_t i) const {
        return edges_[i].get();
    }

    std::string toString() const;

private:
    std::unique_ptr<std::string>                    alias_;
    std::vector<std::unique_ptr<MatchNode>>         nodes_;
    std::vector<std::unique_ptr<MatchEdge>>         edges_;
};

class MatchReturnItems final {
public:
    explicit MatchReturnItems(bool includeExisting, YieldColumns* columns = nullptr)
        : includeExisting_(includeExisting), columns_(columns) {}

    bool includeExisting() const { return includeExisting_; }

    YieldColumns* columns() { return columns_.get(); }

    const YieldColumns* columns() const { return columns_.get(); }

    std::string toString() const;

private:
    bool includeExisting_{false};  // `*` indicates include all existing variables
    std::unique_ptr<YieldColumns> columns_;
};


class MatchReturn final {
public:
    explicit MatchReturn(MatchReturnItems* returnItems = nullptr,
                         OrderFactors* orderFactors = nullptr,
                         Expression* skip = nullptr,
                         Expression* limit = nullptr,
                         bool distinct = false) {
        returnItems_.reset(returnItems);
        orderFactors_.reset(orderFactors);
        skip_ = skip;
        limit_ = limit;
        isDistinct_ = distinct;
    }

    MatchReturnItems* returnItems() {
        return returnItems_.get();
    }

    const MatchReturnItems* returnItems() const {
        return returnItems_.get();
    }

    bool isDistinct() const {
        return isDistinct_;
    }

    const Expression* skip() const {
        return skip_;
    }

    const Expression* limit() const {
        return limit_;
    }

    OrderFactors* orderFactors() {
        return orderFactors_.get();
    }

    const OrderFactors* orderFactors() const {
        return orderFactors_.get();
    }

    std::string toString() const;

private:
    std::unique_ptr<MatchReturnItems>   returnItems_;
    bool                                isDistinct_{false};
    std::unique_ptr<OrderFactors>       orderFactors_;
    Expression*                         skip_{nullptr};
    Expression*                         limit_{nullptr};
};


class ReadingClause {
public:
    enum class Kind: uint8_t {
        kMatch,
        kUnwind,
        kWith,
    };
    explicit ReadingClause(Kind kind) {
        kind_ = kind;
    }
    virtual ~ReadingClause() = default;

    auto kind() const {
        return kind_;
    }

    bool isMatch() const {
        return kind() == Kind::kMatch;
    }

    bool isUnwind() const {
        return kind() == Kind::kUnwind;
    }

    bool isWith() const {
        return kind() == Kind::kWith;
    }

    virtual std::string toString() const = 0;

private:
    Kind            kind_;
};


class MatchClause final : public ReadingClause {
public:
    MatchClause(MatchPath *path, WhereClause *where, bool optional)
        : ReadingClause(Kind::kMatch) {
        path_.reset(path);
        where_.reset(where);
        isOptional_ = optional;
    }

    MatchPath* path() {
        return path_.get();
    }

    const MatchPath* path() const {
        return path_.get();
    }

    WhereClause* where() {
        return where_.get();
    }

    const WhereClause* where() const {
        return where_.get();
    }

    bool isOptional() const {
        return isOptional_;
    }

    std::string toString() const override;

private:
    bool                                isOptional_{false};
    std::unique_ptr<MatchPath>          path_;
    std::unique_ptr<WhereClause>        where_;
};


class UnwindClause final : public ReadingClause {
public:
    UnwindClause(Expression *expr, const std::string &alias)
        : ReadingClause(Kind::kUnwind) {
        expr_ = expr;
        alias_ = alias;
    }

    Expression* expr() {
        return expr_;
    }

    const Expression* expr() const {
        return expr_;
    }

    const std::string& alias() const {
        return alias_;
    }

    std::string toString() const override;

private:
    Expression* expr_{nullptr};
    std::string alias_;
};

class WithClause final : public ReadingClause {
public:
    explicit WithClause(MatchReturnItems *returnItems,
                       OrderFactors *orderFactors = nullptr,
                       Expression *skip = nullptr,
                       Expression *limit = nullptr,
                       WhereClause *where = nullptr,
                       bool distinct = false)
        : ReadingClause(Kind::kWith) {
        returnItems_.reset(returnItems);
        orderFactors_.reset(orderFactors);
        skip_ = skip;
        limit_ = limit;
        where_.reset(where);
        isDistinct_ = distinct;
    }

    MatchReturnItems* returnItems() {
        return returnItems_.get();
    }

    const MatchReturnItems* returnItems() const {
        return returnItems_.get();
    }

    OrderFactors* orderFactors() {
        return orderFactors_.get();
    }

    const OrderFactors* orderFactors() const {
        return orderFactors_.get();
    }

    Expression* skip() {
        return skip_;
    }

    const Expression* skip() const {
        return skip_;
    }

    Expression* limit() {
        return limit_;
    }

    const Expression* limit() const {
        return limit_;
    }

    WhereClause* where() {
        return where_.get();
    }

    const WhereClause* where() const {
        return where_.get();
    }

    bool isDistinct() const {
        return isDistinct_;
    }

    std::string toString() const override;

private:
    std::unique_ptr<MatchReturnItems>   returnItems_;
    std::unique_ptr<OrderFactors>       orderFactors_;
    Expression*                         skip_{nullptr};
    Expression*                         limit_{nullptr};
    std::unique_ptr<WhereClause>        where_;
    bool                                isDistinct_;
};


class MatchClauseList final {
public:
    void add(ReadingClause *clause) {
        clauses_.emplace_back(clause);
    }

    void add(MatchClauseList *list) {
        DCHECK(list != nullptr);
        for (auto &clause : list->clauses_) {
            clauses_.emplace_back(std::move(clause));
        }
        delete list;
    }

    auto clauses() && {
        return std::move(clauses_);
    }

private:
    std::vector<std::unique_ptr<ReadingClause>>     clauses_;
};


class MatchSentence final : public Sentence {
public:
    MatchSentence(MatchClauseList *clauses, MatchReturn *ret)
        : Sentence(Kind::kMatch) {
        clauses_ = std::move(*clauses).clauses();
        delete clauses;
        return_.reset(ret);
    }

    auto& clauses() {
        return clauses_;
    }

    const auto& clauses() const {
        return clauses_;
    }

    const MatchReturn* ret() const {
        return return_.get();
    }

    MatchReturn* ret() {
        return return_.get();
    }

    std::string toString() const override;

private:
    std::vector<std::unique_ptr<ReadingClause>>     clauses_;
    std::unique_ptr<MatchReturn>                    return_;
};

}   // namespace nebula

#endif  // PARSER_MATCHSENTENCE_H_
