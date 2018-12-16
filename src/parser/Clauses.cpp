/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
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
    buf.resize(buf.size() - 1);
    return buf;
}

std::string FromClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "FROM ";
    if (!isRef_) {
        buf += srcNodeList_->toString();
    } else {
        buf += ref_->toString();
    }
    if (alias_ != nullptr) {
        buf += " AS ";
        buf += *alias_;
    }
    return buf;
}


std::string EdgeList::toString() const {
    std::string buf;
    buf.reserve(256);

    for (auto &item : edges_) {
        buf += *item->edge();
        if (item->alias() != nullptr) {
            buf += " AS ";
            buf += *item->alias();
        }
        buf += ",";
    }
    buf.resize(buf.size() - 1);

    return buf;
}


std::string OverClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "OVER ";
    buf += edges_->toString();
    if (isReversely_) {
        buf += " REVERSELY";
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

std::string YieldColumns::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &col : columns_) {
        auto *expr = col->expr();
        buf += expr->toString();
        if (col->alias() != nullptr) {
            buf += " AS ";
            buf += *col->alias();
        }
        buf += ",";
    }
    buf.resize(buf.size() -1 );
    return buf;
}

std::string YieldClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "YIELD ";
    buf += yieldColumns_->toString();
    return buf;
}

}   // namespace nebula
