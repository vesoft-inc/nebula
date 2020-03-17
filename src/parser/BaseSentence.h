/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PARSER_BASESENTENCES_H_
#define PARSER_BASESENTENCES_H_

#include "parser/Clauses.h"
#include "parser/Sentence.h"

namespace nebula {

class BaseVerticesSentence : public Sentence {
public:
    explicit BaseVerticesSentence(VertexIDList *vidList) {
        vidList_.reset(vidList);
    }

    explicit BaseVerticesSentence(Expression *ref) {
        vidRef_.reset(ref);
    }

    BaseVerticesSentence() {}

    virtual ~BaseVerticesSentence() {}

    auto vidList() const {
        return vidList_->vidList();
    }

    bool isRef() const {
        return vidRef_ != nullptr;
    }

    Expression* ref() const {
        return vidRef_.get();
    }

protected:
    std::unique_ptr<VertexIDList>   vidList_;
    std::unique_ptr<Expression>     vidRef_;
};


class EdgeKey final {
public:
    EdgeKey(Expression *srcid, Expression *dstid, int64_t rank) {
        srcid_.reset(srcid);
        dstid_.reset(dstid);
        rank_ = rank;
    }

    Expression* srcid() const {
        return srcid_.get();
    }

    Expression* dstid() const {
        return dstid_.get();
    }

    int64_t rank() {
        return rank_;
    }

    std::string toString() const;

private:
    std::unique_ptr<Expression>     srcid_;
    std::unique_ptr<Expression>     dstid_;
    EdgeRanking                     rank_;
};


class EdgeKeys final {
public:
    EdgeKeys() = default;

    void addEdgeKey(EdgeKey *key) {
        keys_.emplace_back(key);
    }

    std::vector<EdgeKey*> keys() {
        std::vector<EdgeKey*> result;
        result.resize(keys_.size());
        auto get = [](const auto&key) { return key.get(); };
        std::transform(keys_.begin(), keys_.end(), result.begin(), get);
        return result;
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<EdgeKey>>   keys_;
};


class EdgeKeyRef final {
public:
    EdgeKeyRef(
            Expression *srcid,
            Expression *dstid,
            Expression *rank,
            bool isInputExpr = true) {
        srcid_.reset(srcid);
        dstid_.reset(dstid);
        rank_.reset(rank);
        isInputExpr_ = isInputExpr;
    }

    StatusOr<std::string> varname() const;

    std::string* srcid();

    std::string* dstid();

    std::string* rank();

    bool isInputExpr() const {
        return isInputExpr_;
    }

    std::string toString() const;

private:
    std::unique_ptr<Expression>             srcid_;
    std::unique_ptr<Expression>             dstid_;
    std::unique_ptr<Expression>             rank_;
    std::unordered_set<std::string>         uniqVar_;
    bool                                    isInputExpr_;
};


class BaseEdgesSentence : public Sentence {
public:
    explicit BaseEdgesSentence(EdgeKeys *keys) {
        edgeKeys_.reset(keys);
    }

    explicit BaseEdgesSentence(EdgeKeyRef  *ref) {
        keyRef_.reset(ref);
    }

    virtual ~BaseEdgesSentence() {}

    void setKeyRef(EdgeKeyRef *ref) {
        keyRef_.reset(ref);
    }

    bool isRef() const  {
        return keyRef_ != nullptr;
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

protected:
    std::unique_ptr<EdgeKeys>       edgeKeys_;
    std::unique_ptr<EdgeKeyRef>     keyRef_;
};
}  // namespace nebula
#endif  // PARSER_BASESENTENCES_H_
