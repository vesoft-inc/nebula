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


}   // namespace nebula

#endif  // PARSER_TRAVERSESENTENCES_H_
