/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "parser/Clauses.h"

namespace nebula {


std::string StepClause::toString() const {
    std::string buf;
    buf.reserve(256);
    if (isMToN()) {
        buf += std::to_string(mSteps_);
        buf += " TO ";
        buf += std::to_string(nSteps_);
    } else {
        buf += std::to_string(steps());
    }
    buf += " STEPS";
    return buf;
}


std::string VertexIDList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &expr : vidList_) {
        buf += expr->toString();
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}


std::string VerticesClause::toString() const {
    std::string buf;
    buf.reserve(256);
    if (isRef()) {
        buf += ref_->toString();
    } else {
        buf += vidList_->toString();
    }
    return buf;
}

std::string FromClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "FROM ";
    buf += VerticesClause::toString();
    return buf;
}

std::string ToClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "TO ";
    buf += VerticesClause::toString();
    return buf;
}

std::string OverEdge::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += *edge_;
    if (alias_ != nullptr) {
        buf += " AS ";
        buf += *alias_;
    }

    return buf;
}

std::string OverEdges::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &e : edges_) {
        buf += e->toString();
        buf += ",";
    }

    if (!buf.empty()) {
        buf.pop_back();
    }

    return buf;
}

std::string OverClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "OVER ";
    if (isOverAll_) {
        buf += "*";
    } else {
        buf += overEdges_->toString();
    }

    if (direction_ == storage::cpp2::EdgeDirection::IN_EDGE) {
        buf += " REVERSELY";
    } else if (direction_ == storage::cpp2::EdgeDirection::BOTH) {
        buf += " BIDIRECT";
    }

    return buf;
}

std::string TruncateClause::toString() const {
    std::string buf;
    buf.reserve(256);
    if (isSample_) {
        buf += "SAMPLE ";
    } else {
        buf += "LIMIT ";
    }
    buf += truncate_->toString();
    return buf;
}

std::string WhereClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "WHERE ";
    buf += filter_->toString();
    return buf;
}

std::string WhenClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "WHEN ";
    buf += filter()->toString();
    return buf;
}

std::string YieldColumn::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += expr_->rawString();

    if (!alias_.empty()) {
        buf += " AS ";
        buf += alias_;
    }

    return buf;
}

std::string YieldColumns::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &col : columns_) {
        buf += col->toString();
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}

bool operator==(const YieldColumn &l, const YieldColumn &r) {
    if (!l.alias().empty() && !r.alias().empty()) {
        if (l.alias() != r.alias()) {
            return false;
        }
    } else if (l.alias() != r.alias()) {
        return false;
    }

    if (l.expr() != nullptr && r.expr() != nullptr) {
        if (*l.expr() != *r.expr()) {
            return false;
        }
    } else if (l.expr() != r.expr()) {
        return false;
    }

    return true;
}

std::string YieldClause::toString() const {
    std::string buf;
    if (yieldColumns_->empty()) {
        return buf;
    }
    buf.reserve(256);
    buf += "YIELD ";
    if (distinct_) {
        buf += "DISTINCT ";
    }
    buf += yieldColumns_->toString();
    return buf;
}

std::string GroupClause::toString() const {
    return groupColumns_->toString();
}

std::string BoundClause::toString() const {
    std::string buf;
    buf.reserve(256);
    switch (boundType_) {
        case IN:
            buf += "IN ";
            break;
        case OUT:
            buf += "OUT ";
            break;
        case BOTH:
            buf += "BOTH ";
            break;
    }
    buf += overEdges_->toString();
    return buf;
}

}   // namespace nebula
