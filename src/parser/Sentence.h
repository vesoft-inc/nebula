/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef PARSER_SENTENCE_H_
#define PARSER_SENTENCE_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "common/expression/SymbolPropertyExpression.h"
#include "common/expression/ArithmeticExpression.h"
#include "common/expression/ConstantExpression.h"
#include "common/expression/FunctionCallExpression.h"
#include "common/expression/LogicalExpression.h"
#include "common/expression/RelationalExpression.h"
#include "common/expression/TypeCastingExpression.h"
#include "common/expression/UnaryExpression.h"
#include "common/expression/UUIDExpression.h"

namespace nebula {

class Sentence {
public:
    virtual ~Sentence() {}
    virtual std::string toString() const = 0;

    enum class Kind : uint32_t {
        kUnknown,
        kExplain,
        kSequential,
        kGo,
        kSet,
        kPipe,
        kUse,
        kMatch,
        kAssignment,
        kCreateTag,
        kAlterTag,
        kCreateEdge,
        kAlterEdge,
        kDescribeTag,
        kDescribeEdge,
        kCreateTagIndex,
        kCreateEdgeIndex,
        kDropTagIndex,
        kDropEdgeIndex,
        kDescribeTagIndex,
        kDescribeEdgeIndex,
        kRebuildTagIndex,
        kRebuildEdgeIndex,
        kDropTag,
        kDropEdge,
        kInsertVertices,
        kUpdateVertex,
        kInsertEdges,
        kUpdateEdge,
        kShowHosts,
        kShowSpaces,
        kShowParts,
        kShowTags,
        kShowEdges,
        kShowTagIndexes,
        kShowEdgeIndexes,
        kShowUsers,
        kShowRoles,
        kShowCreateSpace,
        kShowCreateTag,
        kShowCreateEdge,
        kShowCreateTagIndex,
        kShowCreateEdgeIndex,
        kShowTagIndexStatus,
        kShowEdgeIndexStatus,
        kShowSnapshots,
        kShowCharset,
        kShowCollation,
        kDeleteVertices,
        kDeleteEdges,
        kLookup,
        kCreateSpace,
        kDropSpace,
        kDescribeSpace,
        kYield,
        kCreateUser,
        kDropUser,
        kAlterUser,
        kGrant,
        kRevoke,
        kChangePassword,
        kDownload,
        kIngest,
        kOrderBy,
        kConfig,
        kFetchVertices,
        kFetchEdges,
        kBalance,
        kFindPath,
        kLimit,
        KGroupBy,
        kReturn,
        kCreateSnapshot,
        kDropSnapshot,
        kAdmin,
        kGetSubgraph,
    };

    Kind kind() const {
        return kind_;
    }

protected:
    Sentence() = default;
    explicit Sentence(Kind kind) : kind_(kind) {}

    Kind                kind_{Kind::kUnknown};
};

class CreateSentence : public Sentence {
public:
    explicit CreateSentence(bool ifNotExist) : ifNotExist_{ifNotExist} {}
    virtual ~CreateSentence() {}

    bool isIfNotExist() {
        return ifNotExist_;
    }

private:
    bool ifNotExist_{false};
};

class DropSentence : public Sentence {
public:
    explicit  DropSentence(bool ifExists) : ifExists_{ifExists} {}
    virtual ~DropSentence() = default;

    bool isIfExists() {
        return ifExists_;
    }
private:
    bool ifExists_{false};
};

inline std::ostream& operator<<(std::ostream &os, Sentence::Kind kind) {
    return os << static_cast<uint32_t>(kind);
}

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

    std::vector<EdgeKey*> keys() const {
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

    Expression* srcid() const {
        return srcid_.get();
    }

    Expression* dstid() const {
        return dstid_.get();
    }

    Expression* rank() const {
        return rank_.get();
    }

    Expression* type() const {
        return type_.get();
    }

    void setType(Expression *type) {
        type_.reset(type);
    }

    bool isInputExpr() const {
        return isInputExpr_;
    }

    std::string toString() const;

private:
    std::unique_ptr<Expression>             srcid_;
    std::unique_ptr<Expression>             dstid_;
    std::unique_ptr<Expression>             rank_;
    std::unique_ptr<Expression>             type_;
    std::unordered_set<std::string>         uniqVar_;
    bool                                    isInputExpr_;
};

}   // namespace nebula

#endif  // PARSER_SENTENCE_H_
