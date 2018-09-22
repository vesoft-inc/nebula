/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "parser/Clauses.h"

namespace vesoft {


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

std::string SourceNodeList::toString(bool isRef) const {
    std::string buf;
    buf.reserve(256);
    if (isRef) {
        buf += "[";
    }
    for (auto id : nodes_) {
        if (isRef) {
            buf += "$";
        }
        buf += std::to_string(id);
        buf += ",";
    }
    buf.resize(buf.size() - 1);
    if (isRef) {
        buf += "]";
    }
    return buf;
}

std::string FromClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "FROM ";
    buf += srcNodeList_->toString(isRef_);
    buf += " AS ";
    buf += *alias_;
    return buf;
}

std::string OverClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "OVER ";
    buf += *edge_;
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

std::string ReturnFields::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &expr : fields_) {
        buf += expr->toString();
        buf += ",";
    }
    buf.resize(buf.size() -1 );
    return buf;
}

std::string ReturnClause::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "RETURN ";
    buf += returnFields_->toString();
    return buf;
}

}   // namespace vesoft
