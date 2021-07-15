/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_CLAUSES_H_
#define PARSER_CLAUSES_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "common/interface/gen-cpp2/storage_types.h"
#include "util/ExpressionUtils.h"

namespace nebula {
class StepClause final {
public:
    explicit StepClause(uint32_t steps = 1) {
        mSteps_ = steps;
        nSteps_ = steps;
    }

    StepClause(uint32_t m, uint32_t n) {
        mSteps_ = m;
        nSteps_ = n;
    }

    uint32_t steps() const {
        return mSteps_;
    }

    bool isMToN() const {
        return mSteps_ != nSteps_;
    }

    uint32_t mSteps() const {
        return mSteps_;
    }

    uint32_t nSteps() const {
        return nSteps_;
    }

    void setMSteps(uint32_t m) {
        mSteps_ = m;
    }

    void setNSteps(uint32_t n) {
        nSteps_ = n;
    }

    std::string toString() const;

private:
    uint32_t             mSteps_{0};
    uint32_t             nSteps_{0};
};


class VertexIDList final {
public:
    void add(Expression *expr) {
        vidList_.emplace_back(expr);
    }

    const std::vector<Expression*> &vidList() const {
        return vidList_;
    }

    std::string toString() const;

private:
    std::vector<Expression*>    vidList_;
};


class VerticesClause {
public:
    explicit VerticesClause(VertexIDList *vidList) {
        vidList_.reset(vidList);
    }

    explicit VerticesClause(Expression *ref) {
        ref_ = ref;
    }

    auto vidList() const {
        return vidList_->vidList();
    }

    auto isRef() const {
        return ref_ != nullptr;
    }

    auto ref() const {
        return ref_;
    }

    std::string toString() const;

protected:
    std::unique_ptr<VertexIDList> vidList_;
    Expression *ref_{nullptr};
};

class FromClause final : public VerticesClause {
public:
    explicit FromClause(VertexIDList *vidList) : VerticesClause(vidList) {
    }

    explicit FromClause(Expression *ref) : VerticesClause(ref) {
    }

    std::string toString() const;
};

class ToClause final : public VerticesClause {
public:
    explicit ToClause(VertexIDList *vidList) : VerticesClause(vidList) {
    }

    explicit ToClause(Expression *ref) : VerticesClause(ref) {
    }

    std::string toString() const;
};

class OverEdge final {
public:
    explicit OverEdge(std::string *edge, std::string *alias = nullptr) {
        edge_.reset(edge);
        alias_.reset(alias);
    }

    std::string *edge() const { return edge_.get(); }

    std::string *alias() const { return alias_.get(); }

    std::string toString() const;

private:
    std::unique_ptr<std::string> edge_;
    std::unique_ptr<std::string> alias_;
};

class OverEdges final {
public:
    void addEdge(OverEdge *edge) { edges_.emplace_back(edge); }

    std::vector<OverEdge *> edges() {
        std::vector<OverEdge *> result;

        std::transform(edges_.cbegin(), edges_.cend(),
                       std::insert_iterator<std::vector<OverEdge *>>(result, result.begin()),
                       [](auto &edge) { return edge.get(); });
        return result;
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<OverEdge>> edges_;
};

class OverClause final {
public:
    OverClause(OverEdges *edges,
               storage::cpp2::EdgeDirection direction = storage::cpp2::EdgeDirection::OUT_EDGE) {
        overEdges_.reset(edges);
        direction_ = direction;
    }

    OverClause(bool isOverAll,
               storage::cpp2::EdgeDirection direction = storage::cpp2::EdgeDirection::OUT_EDGE) {
        isOverAll_ = isOverAll;
        direction_ = direction;
        overEdges_ = std::make_unique<OverEdges>();
    }

    std::vector<OverEdge *> edges() const { return overEdges_->edges(); }

    std::string toString() const;

    storage::cpp2::EdgeDirection direction() const {
        return direction_;
    }

    bool isOverAll() const {
        return isOverAll_;
    }

private:
    storage::cpp2::EdgeDirection                  direction_;
    std::unique_ptr<OverEdges>                    overEdges_;
    bool                                          isOverAll_{false};
};

class TruncateClause {
public:
    TruncateClause(Expression* expr, bool isSample) {
        truncate_ = expr;
        isSample_ = isSample;
    }

    Expression* truncate() const {
        return truncate_;
    }

    std::unique_ptr<TruncateClause> clone() const {
        return std::make_unique<TruncateClause>(truncate_->clone(), isSample_);
    }

    bool isSample() const {
        return isSample_;
    }

    std::string toString() const;

private:
    bool isSample_{false};
    Expression* truncate_{nullptr};
};

class WhereClause {
public:
    explicit WhereClause(Expression *filter) {
        filter_ = filter;
    }

    Expression* filter() const {
        return filter_;
    }

    void setFilter(Expression* expr) {
        filter_ = expr;
    }

    std::unique_ptr<WhereClause> clone() const {
        return std::make_unique<WhereClause>(filter_->clone());
    }

    std::string toString() const;

private:
    Expression*                 filter_{nullptr};
};

class WhenClause : public WhereClause {
public:
    explicit WhenClause(Expression *filter) : WhereClause(filter) {}

    std::string toString() const;
};

class YieldColumn final {
public:
    explicit YieldColumn(Expression *expr, const std::string &alias = "") {
        expr_ = expr;
        alias_ = alias;
    }

    std::unique_ptr<YieldColumn> clone() const {
        return std::make_unique<YieldColumn>(expr_->clone(), alias_);
    }

    void setExpr(Expression* expr) {
        expr_ = expr;
    }

    Expression* expr() const {
        return expr_;
    }

    void setAlias(const std::string& alias) {
        alias_ = alias;
    }

    const std::string& alias() const {
        return alias_;
    }

    std::string name() const {
        return alias_.empty() ? toString() : alias();
    }

    std::string toString() const;

private:
    Expression* expr_{nullptr};
    std::string alias_;
};

bool operator==(const YieldColumn &l, const YieldColumn &r);
inline bool operator!=(const YieldColumn &l, const YieldColumn &r) {
    return !(l == r);
}

class YieldColumns final {
public:
    void addColumn(YieldColumn *field) {
        columns_.emplace_back(field);
    }

    std::vector<YieldColumn*> columns() const {
        std::vector<YieldColumn*> result;
        result.resize(columns_.size());
        auto get = [] (auto &column) { return column.get(); };
        std::transform(columns_.begin(), columns_.end(), result.begin(), get);
        return result;
    }

    std::vector<std::string> names() const {
        std::vector<std::string> names;
        names.reserve(columns_.size());
        for (auto &col : columns_) {
            names.emplace_back(col->name());
        }
        return names;
    }

    size_t size() const {
        return columns_.size();
    }

    bool empty() const {
        return columns_.empty();
    }

    std::unique_ptr<YieldColumns> clone() const {
        auto cols = std::make_unique<YieldColumns>();
        for (auto& col : columns_) {
            cols->addColumn(col->clone().release());
        }
        return cols;
    }

    std::string toString() const;

    const YieldColumn* back() const {
        return columns_.back().get();
    }

    YieldColumn* back() {
        return columns_.back().get();
    }

    bool hasAgg() const {
        for (auto& col : columns_) {
            if (graph::ExpressionUtils::findAny(col->expr(), {Expression::Kind::kAggregate})) {
                return true;
            }
        }
        return false;
    }

private:
    std::vector<std::unique_ptr<YieldColumn>>   columns_;
};


class YieldClause final {
public:
    explicit YieldClause(YieldColumns *yields, bool distinct = false) {
        yieldColumns_.reset(yields);
        distinct_ = distinct;
    }

    std::vector<YieldColumn*> columns() const {
        return yieldColumns_->columns();
    }

    YieldColumns* yields() const {
        return yieldColumns_.get();
    }

    bool isDistinct() const {
        return distinct_;
    }

    std::unique_ptr<YieldClause> clone() const {
        auto cols = yieldColumns_->clone();
        return std::make_unique<YieldClause>(cols.release(), distinct_);
    }

    std::string toString() const;

private:
    std::unique_ptr<YieldColumns>               yieldColumns_;
    bool                                        distinct_;
};

class GroupClause final {
public:
    explicit GroupClause(YieldColumns *fields) {
        groupColumns_.reset(fields);
    }

    std::vector<YieldColumn*> columns() const {
        return groupColumns_->columns();
    }


    std::string toString() const;

private:
    std::unique_ptr<YieldColumns>               groupColumns_;
};

class BoundClause final {
public:
    enum BoundType : uint8_t {
        IN,
        OUT,
        BOTH
    };

    explicit BoundClause(OverEdges *edges, BoundType type) {
        overEdges_.reset(edges);
        boundType_ = type;
    }

    std::vector<OverEdge *> edges() const { return overEdges_->edges(); }

    std::string toString() const;

private:
    std::unique_ptr<OverEdges> overEdges_;
    BoundType boundType_;
};

using InBoundClause = BoundClause;
using OutBoundClause = BoundClause;
using BothInOutClause = BoundClause;

class NameLabelList {
public:
    NameLabelList() = default;

    void add(std::string *label) {
        labels_.emplace_back(label);
    }

    bool empty() const {
        return labels_.empty();
    }

    std::size_t size() const {
        return labels_.size();
    }

    const std::string* front() const {
        return labels_.front().get();
    }

    std::vector<const std::string*> labels() const {
        std::vector<const std::string*> labels;
        labels.reserve(labels_.size());
        for (const auto& it : labels_) {
            labels.emplace_back(it.get());
        }
        return labels;
    }

    std::string toString() const {
        std::stringstream ss;
        for (std::size_t i = 0; i < labels_.size() - 1; ++i) {
            ss << *labels_[i].get() << ",";
        }
        ss << *labels_.back();
        return ss.str();
    }

private:
    std::vector<std::unique_ptr<std::string>> labels_;
};

}   // namespace nebula
#endif  // PARSER_CLAUSES_H_
