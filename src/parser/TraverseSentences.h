/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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

    YieldClause* yieldClause() const {
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


enum class FindKind : uint32_t {
    kUnknown,
    kFindVertex,
    kFindEdge,
};


class FindSentence final : public Sentence {
public:
    FindSentence(FindKind findKind, std::string *name, PropertyList *props) {
        findKind_ = findKind;
        name_.reset(name);
        properties_.reset(props);
        kind_ = Kind::kFind;
    }

    std::string* name() const {
        return name_.get();
    }

    std::vector<std::string*> properties() const {
        return properties_->properties();
    }

    void setWhereClause(WhereClause *whereClause) {
        whereClause_.reset(whereClause);
    }

    WhereClause* whereClause() const {
        return whereClause_.get();
    }

    FindKind findKind() const {
        return findKind_;
    }

    std::string toString() const override;

private:
    FindKind                                    findKind_{FindKind::kUnknown};
    std::unique_ptr<std::string>                name_;
    std::unique_ptr<PropertyList>               properties_;
    std::unique_ptr<WhereClause>                whereClause_;
};

inline std::ostream& operator<<(std::ostream &os, FindKind kind) {
    return os << static_cast<uint32_t>(kind);
}

class UseSentence final : public Sentence {
public:
    explicit UseSentence(std::string *space) {
        kind_ = Kind::kUse;
        space_.reset(space);
    }

    const std::string& space() const {
        return *space_;
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

    std::string toString() const override;

private:
    std::unique_ptr<std::string>                variable_;
    std::unique_ptr<Sentence>                   sentence_;
};


}   // namespace nebula

#endif  // PARSER_TRAVERSESENTENCES_H_
