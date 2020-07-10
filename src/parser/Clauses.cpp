/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "parser/Clauses.h"

namespace nebula {


std::string StepClause::toString() const {
    std::string buf;
    buf.reserve(256);
    if (isUpto()) {
        buf += "UPTO ";
    }
    buf += std::to_string(steps_);
    buf += " STEPS";
    return buf;
}


std::string SourceNodeList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto id : nodes_) {
        buf += std::to_string(id);
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
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

std::string FromClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "FROM ";
    if (isRef()) {
        buf += ref_->toString();
    } else {
        buf += vidList_->toString();
    }
    return buf;
}

std::string ToClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "TO ";
    if (isRef()) {
        buf += ref_->toString();
    } else {
        buf += vidList_->toString();
    }
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
    buf += overEdges_->toString();

    if (direction_ == storage::cpp2::EdgeDirection::IN_EDGE) {
        buf += " REVERSELY";
    } else if (direction_ == storage::cpp2::EdgeDirection::BOTH) {
        buf += " BIDIRECT";
    }

    return buf;
}

std::string WhereClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "WHERE ";
    buf += filter_->toString();
    return buf;
}

std::string YieldColumn::toString() const {
    std::string buf;
    buf.reserve(256);
    if (aggFunName_ != nullptr) {
        buf += *aggFunName_;
        buf += "(";
        buf += expr_->toString();
        buf += ")";
    } else {
        buf += expr_->toString();
    }

    return buf;
}

std::string YieldColumns::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &col : columns_) {
        buf += col->toString();
        if (col->alias() != nullptr) {
            buf += " AS ";
            buf += *col->alias();
        }
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}

std::string YieldClause::toString() const {
    std::string buf;
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

}   // namespace nebula
