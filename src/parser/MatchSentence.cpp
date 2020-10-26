/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "parser/MatchSentence.h"

namespace nebula {

std::string MatchEdge::toString() const {
    std::string buf;
    buf.reserve(256);

    std::string end;
    if (direction_ == Direction::OUT_EDGE) {
        buf += '-';
        end = "->";
    } else if (direction_ == Direction::IN_EDGE) {
        buf += "<-";
        end = "-";
    } else {
        buf += '-';
        end = "-";
    }

    if (alias_ != nullptr || type_ != nullptr || props_ != nullptr) {
        buf += '[';
        if (alias_ != nullptr) {
            buf += *alias_;
        }
        if (type_ != nullptr) {
            buf += ':';
            buf += *type_;
        }
        if (props_ != nullptr) {
            buf += props_->toString();
        }
        buf += ']';
    }

    buf += end;

    return buf;
}


std::string MatchNode::toString() const {
    std::string buf;
    buf.reserve(64);

    buf += '(';
    if (alias_ != nullptr) {
        buf += *alias_;
    }
    if (label_ != nullptr) {
        buf += ':';
        buf += *label_;
    }
    if (props_ != nullptr) {
        buf += props_->toString();
    }
    buf += ')';

    return buf;
}

std::string MatchPath::toString() const {
    std::string buf;
    buf.reserve(256);

    buf += node(0)->toString();
    for (auto i = 0u; i < edges_.size(); i++) {
        buf += edge(i)->toString();
        buf += node(i + 1)->toString();
    }

    return buf;
}

std::string MatchReturn::toString() const {
    std::string buf;
    buf.reserve(64);

    buf += "RETURN ";
    if (isAll()) {
        buf += '*';
    } else {
        buf += columns_->toString();
    }

    return buf;
}

std::string MatchSentence::toString() const {
    std::string buf;
    buf.reserve(256);

    buf += "MATCH ";
    buf += path_->toString();
    if (where_ != nullptr) {
        buf += ' ';
        buf += where_->toString();
    }
    if (return_ != nullptr) {
        buf += ' ';
        buf += return_->toString();
    }

    return buf;
}

}   // namespace nebula
