/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "parser/MaintainSentences.h"

namespace nebula {

std::string SchemaOptItem::toString() const {
    switch (optType_) {
        case TTL_DURATION:
            return folly::stringPrintf("ttl_duration = %ld",
                                       boost::get<int64_t>(optValue_));
        case TTL_COL:
            return folly::stringPrintf("ttl_col = %s",
                                       boost::get<std::string>(optValue_).c_str());
        // TODO(YT) The following features will be supported in the future
        case COMMENT:
            return folly::stringPrintf("comment = \"%s\"",
                                       boost::get<std::string>(optValue_).c_str());
        case ENGINE:
            return folly::stringPrintf("engine = %s",
                                       boost::get<std::string>(optValue_).c_str());
        case ENCRYPT:
            return folly::stringPrintf("encrypt = \"%s\"",
                                       boost::get<std::string>(optValue_).c_str());
        case COMPRESS:
            return folly::stringPrintf("compress = \"%s\"",
                                       boost::get<std::string>(optValue_).c_str());
        case CHARACTER_SET:
            return folly::stringPrintf("character set = %s",
                                       boost::get<std::string>(optValue_).c_str());
        case COLLATE:
            return folly::stringPrintf("collate = %s",
                                       boost::get<std::string>(optValue_).c_str());
        default:
            FLOG_FATAL("Schema option illegal");
    }
    return "Unknown";
}


 std::string SchemaOptList::toString() const {
    std::string buf;
    buf.reserve(512);
    for (auto &item : items_) {
        buf += " ";
        buf += item->toString();
        buf += ",";
    }
    buf.resize(buf.size() - 1);
    return buf;
}


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
        buf += ",";
    }
    if (!colSpecs.empty()) {
        buf.resize(buf.size() - 1);
    }
    buf += ")";
    if (schemaOpts_ != nullptr) {
        buf +=  schemaOpts_->toString();
    }
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
        buf += ",";
    }
    if (!colSpecs.empty()) {
        buf.resize(buf.size() - 1);
    }
    buf += ")";
    if (schemaOpts_ != nullptr) {
        buf +=  schemaOpts_->toString();
    }
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
    if (schemaOpts_ != nullptr) {
        buf +=  schemaOpts_->toString();
    }
    if (opts_ == nullptr) {
        return buf;
    }
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
        buf += ",";
    }
    if (!colSpecs.empty()) {
        buf.resize(buf.size() - 1);
    }
    buf += ")";
    if (schemaOpts_ != nullptr) {
        buf +=  schemaOpts_->toString();
    }
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
