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
}   // namespace nebula
