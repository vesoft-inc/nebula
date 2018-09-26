/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef PARSER_TRAVERSESENTENCES_H_
#define PARSER_TRAVERSESENTENCES_H_

#include "parser/Sentence.h"
#include "parser/Clauses.h"

namespace vesoft {

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

    std::string toString() const override;

private:
    std::unique_ptr<StepClause>                 stepClause_;
    std::unique_ptr<FromClause>                 fromClause_;
    std::unique_ptr<OverClause>                 overClause_;
    std::unique_ptr<WhereClause>                whereClause_;
    std::unique_ptr<ReturnClause>               returnClause_;
};

class MatchSentence final : public Sentence {
public:
    std::string toString() const override;
};

class UseSentence final : public Sentence {
public:
    explicit UseSentence(std::string *ns) {
        ns_.reset(ns);
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>        ns_;
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

    std::string toString() const override;

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

    std::string toString() const override;

private:
    std::unique_ptr<Sentence>                   left_;
    std::unique_ptr<Sentence>                   right_;
};

class AssignmentSentence final : public Sentence {
public:
    AssignmentSentence(std::string *variable, Sentence *sentence) {
        variable_.reset(variable);
        sentence_.reset(sentence);
    }

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                variable_;
    std::unique_ptr<Sentence>                   sentence_;
};


}   // namespace vesoft

#endif  // PARSER_TRAVERSESENTENCES_H_
