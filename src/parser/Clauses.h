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
    struct MToN {
        uint32_t mSteps;
        uint32_t nSteps;
    };

    explicit StepClause(uint32_t steps = 1) {
        steps_ = steps;
    }

    StepClause(uint32_t m, uint32_t n) {
        mToN_ = std::make_unique<MToN>();
        mToN_->mSteps = m;
        mToN_->nSteps = n;
    }

    uint32_t steps() const {
        return steps_;
    }

    MToN* mToN() const {
        return mToN_.get();
    }

    bool isMToN() const {
        return mToN_ != nullptr;
    }

    std::string toString() const;

private:
    uint32_t                                    steps_{1};
    std::unique_ptr<MToN>                       mToN_;
};


class SourceNodeList final {
public:
    void addNodeId(int64_t id) {
        nodes_.emplace_back(id);
    }

    const std::vector<int64_t>& nodeIds() const {
        return nodes_;
    }

    std::string toString() const;

private:
    std::vector<int64_t>                        nodes_;
};


class VertexIDList final {
public:
    void add(Expression *expr) {
        vidList_.emplace_back(expr);
    }

    std::vector<Expression*> vidList() const {
        std::vector<Expression*> result;
        result.reserve(vidList_.size());
        for (auto &expr : vidList_) {
            result.push_back(expr.get());
        }
        return result;
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<Expression>>    vidList_;
};


class VerticesClause {
public:
    explicit VerticesClause(VertexIDList *vidList) {
        vidList_.reset(vidList);
    }

    explicit VerticesClause(Expression *ref) {
        ref_.reset(ref);
    }

    auto vidList() const {
        return vidList_->vidList();
    }

    auto isRef() const {
        return ref_ != nullptr;
    }

    auto ref() const {
        return ref_.get();
    }

protected:
    std::unique_ptr<VertexIDList>               vidList_;
    std::unique_ptr<Expression>                 ref_;
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

class WhereClause final {
public:
    explicit WhereClause(Expression *filter) {
        filter_.reset(filter);
    }

    Expression* filter() const {
        return filter_.get();
    }

    void setFilter(Expression* expr) {
        filter_.reset(expr);
    }

    std::string toString() const;

private:
    std::unique_ptr<Expression>                 filter_;
};

using WhenClause = WhereClause;

class YieldColumn final {
public:
    explicit YieldColumn(Expression *expr, std::string *alias = nullptr) {
        expr_.reset(expr);
        alias_.reset(alias);
    }

    std::unique_ptr<YieldColumn> clone() const {
        auto col = std::make_unique<YieldColumn>(
            graph::ExpressionUtils::clone(expr_.get()).release());
        if (alias_ != nullptr) {
            col->setAlias(new std::string(*alias_));
        }
        if (aggFunName_ != nullptr) {
            col->setAggFunction(new std::string(*aggFunName_));
        }
        return col;
    }

    void setExpr(Expression* expr) {
        expr_.reset(expr);
    }

    Expression* expr() const {
        return expr_.get();
    }

    void setAlias(std::string* alias) {
        alias_.reset(alias);
    }

    std::string* alias() const {
        return alias_.get();
    }

    void setAggFunction(std::string* fun = nullptr) {
        if (fun == nullptr) {
            return;
        }
        aggFunName_.reset(fun);
    }

    std::string getAggFunName() const {
        if (aggFunName_ == nullptr) {
            return "";
        }
        return *aggFunName_;
    }

    std::string toString() const;

private:
    std::unique_ptr<Expression>                 expr_;
    std::unique_ptr<std::string>                alias_;
    std::unique_ptr<std::string>                aggFunName_{nullptr};
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

    std::string toString() const;

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

class InBoundClause final {
public:
    explicit InBoundClause(OverEdges *edges) {
        overEdges_.reset(edges);
    }

    std::vector<OverEdge *> edges() const { return overEdges_->edges(); }

    std::string toString() const;

private:
    std::unique_ptr<OverEdges> overEdges_;
};

using OutBoundClause = InBoundClause;
using BothInOutClause = InBoundClause;

}   // namespace nebula
#endif  // PARSER_CLAUSES_H_
