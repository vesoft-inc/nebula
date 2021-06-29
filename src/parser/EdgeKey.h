/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PARSER_EDGEKEY_H_
#define PARSER_EDGEKEY_H_

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/thrift/ThriftTypes.h"

namespace nebula {

class Expression;

class EdgeKey final {
public:
    EdgeKey(Expression *srcid, Expression *dstid, int64_t rank) {
        srcid_ = srcid;
        dstid_ = dstid;
        rank_ = rank;
    }

    Expression *srcid() const {
        return srcid_;
    }

    Expression *dstid() const {
        return dstid_;
    }

    int64_t rank() {
        return rank_;
    }

    std::string toString() const;

private:
    Expression* srcid_{nullptr};
    Expression* dstid_{nullptr};
    EdgeRanking rank_;
};

class EdgeKeys final {
public:
    EdgeKeys() = default;

    void addEdgeKey(EdgeKey *key) {
        keys_.emplace_back(key);
    }

    std::vector<EdgeKey *> keys() const {
        std::vector<EdgeKey *> result;
        result.resize(keys_.size());
        auto get = [](const auto &key) { return key.get(); };
        std::transform(keys_.begin(), keys_.end(), result.begin(), get);
        return result;
    }

    std::string toString() const;

private:
    std::vector<std::unique_ptr<EdgeKey>> keys_;
};

class EdgeKeyRef final {
public:
    EdgeKeyRef(Expression *srcid, Expression *dstid, Expression *rank, bool isInputExpr = true) {
        srcid_ = srcid;
        dstid_ = dstid;
        rank_ = rank;
        isInputExpr_ = isInputExpr;
    }

    Expression *srcid() const {
        return srcid_;
    }

    Expression *dstid() const {
        return dstid_;
    }

    Expression *rank() const {
        return rank_;
    }

    Expression *type() const {
        return type_;
    }

    void setType(Expression *type) {
        type_ = type;
    }

    bool isInputExpr() const {
        return isInputExpr_;
    }

    std::string toString() const;

private:
    Expression *srcid_{nullptr};
    Expression *dstid_{nullptr};
    Expression *rank_{nullptr};
    Expression *type_{nullptr};
    std::unordered_set<std::string> uniqVar_;
    bool isInputExpr_;
};

}   // namespace nebula

#endif   // PARSER_EDGEKEY_H_
