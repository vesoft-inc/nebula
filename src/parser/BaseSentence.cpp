/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "BaseSentence.h"

namespace nebula {

std::string EdgeKey::toString() const {
    return folly::stringPrintf("%s->%s@%ld,",
                               srcid_->toString().c_str(), dstid_->toString().c_str(), rank_);
}

std::string EdgeKeys::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &key : keys_) {
        buf += key->toString();
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }

    return buf;
}

std::string* EdgeKeyRef::srcid() {
    if (isInputExpr_) {
        auto *iSrcExpr = static_cast<InputPropertyExpression*>(srcid_.get());
        auto *colname = iSrcExpr->prop();
        return colname;
    } else {
        auto *vSrcExpr = static_cast<VariablePropertyExpression*>(srcid_.get());
        auto *var = vSrcExpr->alias();
        auto *colname = vSrcExpr->prop();
        uniqVar_.emplace(*var);
        return colname;
    }
}

std::string* EdgeKeyRef::dstid() {
    if (isInputExpr_) {
        auto *iDstExpr = static_cast<InputPropertyExpression*>(dstid_.get());
        auto *colname = iDstExpr->prop();
        return colname;
    } else {
        auto *vDstExpr = static_cast<VariablePropertyExpression*>(dstid_.get());
        auto *var = vDstExpr->alias();
        auto *colname = vDstExpr->prop();
        uniqVar_.emplace(*var);
        return colname;
    }
}

std::string* EdgeKeyRef::rank() {
    if (rank_ == nullptr) {
        return nullptr;
    }

    if (isInputExpr_) {
        auto *iRankExpr = static_cast<InputPropertyExpression*>(rank_.get());
        auto *colname = iRankExpr->prop();
        return colname;
    } else {
        auto *vRankExpr = static_cast<VariablePropertyExpression*>(rank_.get());
        auto *var = vRankExpr->alias();
        auto *colname = vRankExpr->prop();
        uniqVar_.emplace(*var);
        return colname;
    }
}

StatusOr<std::string> EdgeKeyRef::varname() const {
    std::string result = "";
    if (isInputExpr_) {
        return result;
    }

    if (uniqVar_.size() != 1) {
        return Status::SyntaxError("Near %s, Only support single data source.", toString().c_str());
    }

    for (auto &var : uniqVar_) {
        result = std::move(var);
    }

    return result;
}

std::string EdgeKeyRef::toString() const {
    std::string buf;
    buf.reserve(256);
    if (srcid_ != nullptr) {
        buf += srcid_->toString();
    }
    if (dstid_ != nullptr) {
        buf += "->";
        buf += dstid_->toString();
    }
    if (rank_ != nullptr) {
        buf += "@";
        buf += rank_->toString();
    }
    return buf;
}
}  // namespace nebula
