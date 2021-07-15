/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
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
    if (truncateClause_ != nullptr) {
        buf += " ";
        buf += truncateClause_->toString();
    }
    return buf;
}

LookupSentence::LookupSentence(std::string *from, WhereClause *where, YieldClause *yield)
    : Sentence(Kind::kLookup),
      from_(DCHECK_NOTNULL(from)),
      whereClause_(where),
      yieldClause_(yield) {}

std::string LookupSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "LOOKUP ON ";
    buf += *from_;
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
            LOG(FATAL) << "Unknown Order Type: " << orderType_;
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
    buf += "FETCH PROP ON";
    buf += " ";
    if (tags_->empty()) {
        buf += "*";
    } else {
        buf += tags_->toString();
    }
    buf += " ";
    buf += vertices_->toString();
    if (yieldClause_ != nullptr) {
        buf += " ";
        buf += yieldClause_->toString();
    }
    return buf;
}

std::string FetchEdgesSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "FETCH PROP ON ";
    buf += edge_->toString();
    buf += " ";
    if (isRef()) {
        buf += keyRef_->toString();
    } else {
        buf += edgeKeys_->toString();
    }
    return buf;
}

std::string GroupBySentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "GROUP BY";
    if (groupClause_ != nullptr) {
        buf += " ";
        buf += groupClause_->toString();
    }
    if (yieldClause_ != nullptr) {
        buf += " ";
        buf += yieldClause_->toString();
    }
    return buf;
}

std::string FindPathSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "FIND ";
    if (noLoop_) {
        buf += "NOLOOP PATH ";
    } else if (isShortest_) {
        buf += "SHORTEST PATH ";
    } else {
        buf += "ALL PATH ";
    }

    if (from_ != nullptr) {
        buf += from_->toString();
        buf += " ";
    }
    if (to_ != nullptr) {
        buf += to_->toString();
        buf += " ";
    }
    if (over_ != nullptr) {
        buf += over_->toString();
        buf += " ";
    }
    if (where_ != nullptr) {
        buf += where_->toString();
        buf += " ";
    }
    if (step_ != nullptr) {
        buf += "UPTO ";
        buf += step_->toString();
        buf += " ";
    }
    return buf;
}

std::string LimitSentence::toString() const {
    if (offset_ == 0) {
        return folly::stringPrintf("LIMIT %ld", count_);
    }

    return folly::stringPrintf("LIMIT %ld,%ld", offset_, count_);
}

std::string YieldSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += yieldClause_->toString();
    if (whereClause_ != nullptr) {
        buf += " ";
        buf += whereClause_->toString();
    }
    return buf;
}

std::string GetSubgraphSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "GET SUBGRAPH ";
    if (step_ != nullptr) {
        buf += step_->toString();
    }
    if (from_ != nullptr) {
        buf += " ";
        buf += from_->toString();
    }
    if (in_ != nullptr) {
        buf += " ";
        buf += in_->toString();
    }
    if (out_ != nullptr) {
        buf += " ";
        buf += out_->toString();
    }
    if (both_ != nullptr) {
        buf += " ";
        buf += both_->toString();
    }
    return buf;
}
}   // namespace nebula
