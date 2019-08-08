/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "parser/TraverseSentences.h"

namespace nebula {

std::string GoSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "GO ";
    if (stepClause_ != nullptr) {
        buf += stepClause_->toString();
    }
    if (fromClause_ != nullptr) {
        buf += " ";
        buf += fromClause_->toString();
    }
    if (overClause_ != nullptr) {
        buf += " ";
        buf += overClause_->toString();
    }
    if (whereClause_ != nullptr) {
        buf += " ";
        buf += whereClause_->toString();
    }
    if (yieldClause_ != nullptr) {
        buf += " ";
        buf += yieldClause_->toString();
    }

    return buf;
}

std::string MatchSentence::toString() const {
    return "MATCH sentence";
}

std::string FindSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "FIND ";
    buf += properties_->toString();
    buf += " FROM ";
    buf += *type_;
    if (whereClause_ != nullptr) {
        buf += " WHERE ";
        buf += whereClause_->toString();
    }
    return buf;
}

std::string UseSentence::toString() const {
    return "USE " + *space_;
}

std::string SetSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf = "(";
    buf += left_->toString();
    switch (op_) {
        case UNION:
            buf += " UNION ";
            break;
        case INTERSECT:
            buf += " INTERSECT ";
            break;
        case MINUS:
            buf += " MINUS ";
            break;
    }
    buf += right_->toString();
    buf += ")";
    return buf;
}

std::string PipedSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += left_->toString();
    buf += " | ";
    buf += right_->toString();
    return buf;
}

std::string AssignmentSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "$";
    buf += *variable_;
    buf += " = ";
    buf += sentence_->toString();
    return buf;
}

std::string OrderFactor::toString() const {
    switch (orderType_) {
        case ASCEND:
            return folly::stringPrintf("%s ASC,", expr_->toString().c_str());
        case DESCEND:
            return folly::stringPrintf("%s DESC,", expr_->toString().c_str());
        default:

            LOG(FATAL) << "Unkown Order Type: " << orderType_;
            return "";
    }
}

std::string OrderFactors::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &factor : factors_) {
        buf += factor->toString();
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}

std::string OrderBySentence::toString() const {
    return folly::stringPrintf("ORDER BY %s", orderFactors_->toString().c_str());
}

std::string FetchVerticesSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "FETCH PROP ON ";
    if (isRef()) {
        buf += vidRef_->toString();
    } else {
        buf += vidList_->toString();
    }
    if (yieldClause_ != nullptr) {
        buf += " ";
        buf += yieldClause_->toString();
    }
    return buf;
}

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
        return Status::SyntaxError(
            "Near %s, Only support single data source.", toString());
    }

    for (auto &var : uniqVar_) {
        result = std::move(var);
    }

    return result;
}

std::string EdgeKeyRef::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "FETCH PTOP ON ";
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

std::string FetchEdgesSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "FETCH PROP ON ";
    if (isRef()) {
        buf += keyRef_->toString();
    } else {
        buf += edgeKeys_->toString();
    }

    if (yieldClause_ != nullptr) {
        buf += " ";
        buf += yieldClause_->toString();
    }
    return buf;
}
}   // namespace nebula
