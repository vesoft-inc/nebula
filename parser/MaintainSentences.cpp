/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "parser/MaintainSentences.h"

namespace vesoft {

std::string DefineTagSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "DEFINE TAG ";
    buf += *name_;
    buf += " (";
    for (auto &col : columns_->columns_) {
        buf += *col->name_;
        buf += " ";
        buf += columnTypeToString(col->type_);
        if (col->hasTTL_) {
            buf += " TTL = ";
            buf += std::to_string(col->ttl_);
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
    for (auto &col : columns_->columns_) {
        buf += *col->name_;
        buf += " ";
        buf += columnTypeToString(col->type_);
        if (col->hasTTL_) {
            buf += " TTL = ";
            buf += std::to_string(col->ttl_);
        }
        buf += ",";
    }
    buf.resize(buf.size() - 1);
    buf += ")";
    return buf;
}

std::string AlterTagSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "ALTER TAG ";
    buf += *name_;
    buf += "(";
    for (auto &col : columns_->columns_) {
        buf += *col->name_;
        buf += " ";
        buf += columnTypeToString(col->type_);
        if (col->hasTTL_) {
            buf += " TTL = ";
            buf += std::to_string(col->ttl_);
        }
        buf += ",";
    }
    buf.resize(buf.size() - 1);
    buf += ")";
    return buf;
}

std::string AlterEdgeSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "ALTER EDGE ";
    buf += *name_;
    buf += "(";
    for (auto &col : columns_->columns_) {
        buf += *col->name_;
        buf += " ";
        buf += columnTypeToString(col->type_);
        if (col->hasTTL_) {
            buf += " TTL = ";
            buf += std::to_string(col->ttl_);
        }
        buf += ",";
    }
    buf.resize(buf.size() - 1);
    buf += ")";
    return buf;
}

}   // namespace vesoft
