/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
    std::vector<std::string> colStrs;
    for (auto *col : columns_->columnSpecs()) {
        std::string colStr = *col->name();
        colStr += " ";
        colStr += columnTypeToString(col->type());
        if (col->hasTTL()) {
            colStr += " TTL = ";
            colStr += std::to_string(col->ttl());
        }
        colStrs.push_back(colStr);
    }
    buf += folly::join(",", colStrs);
    buf += ")";
    return buf;
}


std::string CreateEdgeSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "CREATE EDGE ";
    buf += *name_;
    buf += " (";
    std::vector<std::string> colStrs;
    for (auto &col : columns_->columnSpecs()) {
        std::string colStr = *col->name();
        colStr += " ";
        colStr += columnTypeToString(col->type());
        if (col->hasTTL()) {
            colStr += " TTL = ";
            colStr += std::to_string(col->ttl());
        }
        colStrs.push_back(colStr);
    }
    buf += folly::join(",", colStrs);
    buf += ")";
    return buf;
}


std::string AlterTagOptItem::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += getOptTypeStr();
    buf += " (";
    std::vector<std::string> colStrs;
    for (auto &col : columns_->columnSpecs()) {
        std::string colStr = *col->name();
        colStr += " ";
        colStr += columnTypeToString(col->type());
        if (col->hasTTL()) {
            colStr += " TTL = ";
            colStr += std::to_string(col->ttl());
        }
        colStrs.push_back(colStr);
    }
    buf += folly::join(",", colStrs);
    buf += ")";
    return buf;
}


std::string AlterTagOptList::toString() const {
    std::vector<std::string> alterTagStrs;
    for (auto &item : alterTagitems_) {
        alterTagStrs.push_back(item->toString());
    }
    return folly::join(",", alterTagStrs);;
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
    std::vector<std::string> colStrs;
    for (auto &col : columns_->columnSpecs()) {
        std::string colStr = *col->name();
        colStr += " ";
        colStr += columnTypeToString(col->type());
        if (col->hasTTL()) {
            colStr += " TTL = ";
            colStr += std::to_string(col->ttl());
        }
        colStrs.push_back(colStr);
    }
    buf += folly::join(",", colStrs);
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
