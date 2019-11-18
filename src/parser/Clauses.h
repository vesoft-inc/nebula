/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_CLAUSES_H_
#define PARSER_CLAUSES_H_

#include "base/Base.h"
#include "filter/Expressions.h"

namespace nebula {

class OverEdge;
class Clause {
public:
    struct Vertices {
        std::string             *colname_{nullptr};
        std::string             *varname_{nullptr};
        std::vector<VertexID>    vids_;
    };

    struct Over {
        std::vector<OverEdge*>  edges_{nullptr};
        std::vector<EdgeType>   edgeTypes_;
        std::vector<EdgeType>   oppositeTypes_;
    };

    struct Step {
        uint32_t steps_{0};
        bool     upto_{false};
    };

    struct Where {
        Expression *filter_{nullptr};
    };

protected:
    enum Kind : uint8_t {
        kUnknown = 0,

        kStepClause,
        kOverClause,
        kFromClause,
        kToClause,
        kWhereClause,
        kYieldClause,

        kMax,
    };

protected:
    Kind    kind_{kUnknown};
};

class StepClause final : public Clause {
public:
    explicit StepClause(uint64_t steps = 1, bool isUpto = false) {
        steps_ = steps;
        isUpto_ = isUpto;
        kind_ = Kind::kStepClause;
    }

    Status prepare(Step &step) const {
        step.steps_ = steps_;
        step.upto_ = isUpto_;
        return Status::OK();
    }

    uint32_t steps() const {
        return steps_;
    }

    bool isUpto() const {
        return isUpto_;
    }

    std::string toString() const;

private:
    uint32_t                                    steps_{1};
    bool                                        isUpto_{false};
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


class VerticesClause : public Clause {
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

    Status prepare(Vertices &vertices) const;

protected:
    std::unique_ptr<VertexIDList>               vidList_;
    std::unique_ptr<Expression>                 ref_;
};

class FromClause final : public VerticesClause {
public:
    explicit FromClause(VertexIDList *vidList) : VerticesClause(vidList) {
        kind_ = kFromClause;
    }

    explicit FromClause(Expression *ref) : VerticesClause(ref) {
        kind_ = kFromClause;
    }

    std::string toString() const;
};

class ToClause final : public VerticesClause {
public:
    explicit ToClause(VertexIDList *vidList) : VerticesClause(vidList) {
        kind_ = kToClause;
    }

    explicit ToClause(Expression *ref) : VerticesClause(ref) {
        kind_ = kToClause;
    }

    std::string toString() const;
};

class OverEdge final {
public:
    explicit OverEdge(std::string *edge, std::string *alias = nullptr, bool isReversely = false) {
        edge_.reset(edge);
        alias_.reset(alias);
        isReversely_ = isReversely;
    }

    bool isReversely() const { return isReversely_; }

    bool isOverAll() const { return *edge_ == "*"; }

    std::string *edge() const { return edge_.get(); }

    std::string *alias() const { return alias_.get(); }

    std::string toString() const;

private:
    bool isReversely_{false};
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

class OverClause final : public Clause {
public:
    explicit OverClause(OverEdges *edges) {
        kind_ = kOverClause;
        overEdges_.reset(edges);
    }

    std::vector<OverEdge *> edges() const { return overEdges_->edges(); }

    Status prepare(Over &over) const;

    std::string toString() const;

private:
    std::unique_ptr<OverEdges> overEdges_;
};

class WhereClause final : public Clause {
public:
    explicit WhereClause(Expression *filter) {
        filter_.reset(filter);
    }

    Expression* filter() const {
        return filter_.get();
    }

    Status prepare(Where &where) const;

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

    Expression* expr() const {
        return expr_.get();
    }

    std::string* alias() const {
        return alias_.get();
    }

    void setFunction(std::string* fun = nullptr) {
        if (fun == nullptr) {
            return;
        }
        funName_.reset(fun);
    }

    std::string getFunName() {
        if (funName_ == nullptr) {
            return "";
        }
        return *funName_;
    }

    std::string toString() const;

private:
    std::unique_ptr<Expression>                 expr_;
    std::unique_ptr<std::string>                alias_;
    std::unique_ptr<std::string>                funName_{nullptr};
};


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
    explicit YieldClause(YieldColumns *fields, bool distinct = false) {
        yieldColumns_.reset(fields);
        distinct_ = distinct;
    }

    std::vector<YieldColumn*> columns() const {
        return yieldColumns_->columns();
    }

    bool isDistinct() const {
        return distinct_;
    }

    std::string toString() const;

private:
    std::unique_ptr<YieldColumns>               yieldColumns_;
    bool                                        distinct_;
    // this member will hold the reference
    // which is expand by *
    std::unique_ptr<YieldColumns>               yieldColHolder_;
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
}   // namespace nebula
#endif  // PARSER_CLAUSES_H_

