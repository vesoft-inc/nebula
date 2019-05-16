/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "parser/MaintainSentences.h"

namespace nebula {

std::string CreateTagSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "CREATE TAG ";
    buf += *name_;
    buf += " (";
    auto colSpecs = std::move(columns_->columnSpecs());
    for (auto *col : colSpecs) {
        buf += *col->name();
        buf += " ";
        buf += columnTypeToString(col->type());
        if (col->hasTTL()) {
            buf += " TTL = ";
            buf += std::to_string(col->ttl());
        }
        buf += ",";
    }
    if (!colSpecs.empty()) {
        buf.resize(buf.size() - 1);
    }
    buf += ")";
    return buf;
}

std::string CreateEdgeSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "CREATE EDGE ";
    buf += *name_;
    buf += " (";
    auto colSpecs = std::move(columns_->columnSpecs());
    for (auto &col : colSpecs) {
        buf += *col->name();
        buf += " ";
        buf += columnTypeToString(col->type());
        if (col->hasTTL()) {
            buf += " TTL = ";
            buf += std::to_string(col->ttl());
        }
        buf += ",";
    }
    if (!colSpecs.empty()) {
        buf.resize(buf.size() - 1);
    }
    buf += ")";
    return buf;
}

std::string AlterTagOptItem::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += getOptTypeStr();
    buf += " (";
    auto colSpecs = std::move(columns_->columnSpecs());
    for (auto &col : colSpecs) {
        buf += *col->name();
        buf += " ";
        buf += columnTypeToString(col->type());
        if (col->hasTTL()) {
            buf += " TTL = ";
            buf += std::to_string(col->ttl());
        }
        buf += ",";
    }
    if (!colSpecs.empty()) {
       buf.resize(buf.size() - 1);
    }
    buf += ")";
    return buf;
}

std::string AlterTagOptList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (uint32_t i = 0; i < alterTagitems_.size(); i++) {
        auto &item = alterTagitems_[i];
        if (i > 0) {
            buf += ",";
        }
        buf += item->toString();
    }
    return buf;
}

std::string AlterTagSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "ALTER TAG ";
    buf += *name_;
    for (auto &tagOpt : opts_->alterTagItems()) {
        buf += " ";
        buf += tagOpt->toString();
    }
    return buf;
}

std::string AlterEdgeSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "ALTER EDGE ";
    buf += *name_;
    buf += "(";
    auto colSpecs = std::move(columns_->columnSpecs());
    for (auto &col : colSpecs) {
        buf += *col->name();
        buf += " ";
        buf += columnTypeToString(col->type());
        if (col->hasTTL()) {
            buf += " TTL = ";
            buf += std::to_string(col->ttl());
        }
        buf += ",";
    }
    if (!colSpecs.empty()) {
        buf.resize(buf.size() - 1);
    }
    buf += ")";
    return buf;
}

std::string DescribeTagSentence::toString() const {
    return folly::stringPrintf("DESCRIBE TAG %s", name_.get()->c_str());
}

std::string DescribeEdgeSentence::toString() const {
    return folly::stringPrintf("DESCRIBE EDGE %s", name_.get()->c_str());
}

std::string RemoveTagSentence::toString() const {
    return folly::stringPrintf("REMOVE TAG %s", name_.get()->c_str());
}


std::string RemoveEdgeSentence::toString() const {
    return folly::stringPrintf("REMOVE TAG %s", name_.get()->c_str());
}

std::string YieldSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "YIELD ";
    buf += yieldColumns_->toString();
    return buf;
}

}   // namespace nebula
