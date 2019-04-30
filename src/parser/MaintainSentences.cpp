/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "parser/MaintainSentences.h"

namespace nebula {

std::string DefineTagSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "DEFINE TAG ";
    buf += *name_;
    buf += " (";
    for (auto *col : columns_->columnSpecs()) {
        buf += *col->name();
        buf += " ";
        buf += columnTypeToString(col->type());
        if (col->hasTTL()) {
            buf += " TTL = ";
            buf += std::to_string(col->ttl());
        }
        buf += ",";
    }
    buf.resize(buf.size() - 1);
    buf += ")";
    return buf;
}


std::string DefineEdgeSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "DEFINE EDGE ";
    buf += *name_;
    buf += " (";
    for (auto &col : columns_->columnSpecs()) {
        buf += *col->name();
        buf += " ";
        buf += columnTypeToString(col->type());
        if (col->hasTTL()) {
            buf += " TTL = ";
            buf += std::to_string(col->ttl());
        }
        buf += ",";
    }
    buf.resize(buf.size() - 1);
    buf += ")";
    return buf;
}


std::string AlterTagOptItem::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += getOptTypeStr();
    buf += " (";
    for (auto &col : columns_->columnSpecs()) {
        buf += *col->name();
        buf += " ";
        buf += columnTypeToString(col->type());
        if (col->hasTTL()) {
            buf += " TTL = ";
            buf += std::to_string(col->ttl());
        }
        buf += ",";
    }
    buf.resize(buf.size() - 1);
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
    for (auto &col : columns_->columnSpecs()) {
        buf += *col->name();
        buf += " ";
        buf += columnTypeToString(col->type());
        if (col->hasTTL()) {
            buf += " TTL = ";
            buf += std::to_string(col->ttl());
        }
        buf += ",";
    }
    buf.resize(buf.size() - 1);
    buf += ")";
    return buf;
}


std::string DescribeTagSentence::toString() const {
    std::string buf = "DESCRIBE TAG ";
    buf += *name_;
    return buf;
}


std::string DescribeEdgeSentence::toString() const {
    std::string buf = "DESCRIBE EDGE ";
    buf += *name_;
    return buf;
}


std::string YieldSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "YIELD ";
    buf += yieldColumns_->toString();
    return buf;
}

}   // namespace nebula
