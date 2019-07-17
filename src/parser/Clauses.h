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

class StepClause final {
public:
    explicit StepClause(uint64_t steps = 1, bool isUpto = false) {
        steps_ = steps;
        isUpto_ = isUpto;
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


class FromClause final {
public:
    explicit FromClause(VertexIDList *vidList) {
        vidList_.reset(vidList);
    }

    explicit FromClause(Expression *ref) {
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

    std::string toString() const;

private:
    std::unique_ptr<VertexIDList>               vidList_;
    std::unique_ptr<Expression>                 ref_;
};


class OverClause final {
public:
    explicit OverClause(std::string *edge,
                        std::string *alias = nullptr,
                        bool isReversely = false) {
        edge_.reset(edge);
        alias_.reset(alias);
        isReversely_ = isReversely;
    }

    bool isReversely() const {
        return isReversely_;
    }

    std::string* edge() const {
        return edge_.get();
    }

    std::string* alias() const {
        return alias_.get();
    }

    std::string toString() const;

private:
    bool                                        isReversely_{false};
    std::unique_ptr<std::string>                edge_;
    std::unique_ptr<std::string>                alias_;
};


class WhereClause final {
public:
    explicit WhereClause(Expression *filter) {
        filter_.reset(filter);
    }

    Expression* filter() const {
        return filter_.get();
    }

    std::string toString() const;

private:
    std::unique_ptr<Expression>                 filter_;
};


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

private:
    std::unique_ptr<Expression>                 expr_;
    std::unique_ptr<std::string>                alias_;
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
};

}   // namespace nebula

#endif  // PARSER_CLAUSES_H_
