/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_CLAUSES_H_
#define PARSER_CLAUSES_H_

#include "base/Base.h"
#include "parser/Expressions.h"

namespace vesoft {

class StepClause final {
public:
    explicit StepClause(uint64_t steps = 1, bool isUpto = false) {
        steps_ = steps;
        isUpto_ = isUpto;
    }

    bool isUpto() const {
        return isUpto_;
    }

    std::string toString() const;

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

    std::string toString(bool isRef) const;

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

    std::string toString() const;

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

    std::string toString() const;

private:
    std::unique_ptr<std::string>                edge_;
    bool                                        isReversely_{false};
};

class WhereClause final {
public:
    explicit WhereClause(Expression *filter) {
        filter_.reset(filter);
    }

    std::string toString() const;

private:
    std::unique_ptr<Expression>                 filter_;
};

class ReturnFields final {
public:
    void addColumn(Expression *field) {
        fields_.emplace_back(field);
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<Expression>>    fields_;
};

class ReturnClause final {
public:
    explicit ReturnClause(ReturnFields *fields) {
        returnFields_.reset(fields);
    }

    std::string toString() const;

private:
    std::unique_ptr<ReturnFields>               returnFields_;
};

}

#endif  // PARSER_CLAUSES_H_
