/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "parser/MaintainSentences.h"

namespace nebula {

std::string SchemaPropItem::toString() const {
    switch (propType_) {
        case TTL_DURATION:
            return folly::stringPrintf("ttl_duration = %ld",
                                       boost::get<int64_t>(propValue_));
        case TTL_COL:
            return folly::stringPrintf("ttl_col = %s",
                                       boost::get<std::string>(propValue_).c_str());
        default:
            FLOG_FATAL("Schema property type illegal");
    }
    return "Unknown";
}


 std::string SchemaPropList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &item : items_) {
        buf += " ";
        buf += item->toString();
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}


std::string CreateTagSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "CREATE TAG ";
    buf += *name_;
    buf += " (";
    auto colSpecs = columns_->columnSpecs();
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
    if (schemaProps_ != nullptr) {
        buf +=  schemaProps_->toString();
    }
    return buf;
}


std::string CreateEdgeSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "CREATE EDGE ";
    buf += *name_;
    buf += " (";
    auto colSpecs = columns_->columnSpecs();
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
    if (schemaProps_ != nullptr) {
        buf +=  schemaProps_->toString();
    }
    return buf;
}


std::string AlterSchemaOptItem::toString() const {
    std::string buf;
    buf.reserve(256);
    switch (optType_) {
        case ADD:
            buf += "ADD";
            break;
        case CHANGE:
            buf += "CHANGE";
            break;
        case DROP:
            buf += "DROP";
            break;
    }
    buf += " (";
    auto colSpecs = columns_->columnSpecs();
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


nebula::meta::cpp2::AlterSchemaOp
AlterSchemaOptItem::toType() {
    switch (optType_) {
        case ADD:
            return nebula::meta::cpp2::AlterSchemaOp::ADD;
        case CHANGE:
            return nebula::meta::cpp2::AlterSchemaOp::CHANGE;
        case DROP:
            return nebula::meta::cpp2::AlterSchemaOp::DROP;
        default:
            return nebula::meta::cpp2::AlterSchemaOp::UNKNOWN;
    }
}


std::string AlterSchemaOptList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &item : alterSchemaItems_) {
        buf += " ";
        buf += item->toString();
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}


std::string AlterTagSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "ALTER TAG ";
    buf += *name_;
    if (opts_ != nullptr) {
        for (auto &schemaOpt : opts_->alterSchemaItems()) {
            buf += " ";
            buf += schemaOpt->toString();
        }
    }
    if (schemaProps_ != nullptr) {
        buf +=  schemaProps_->toString();
    }
    return buf;
}


std::string AlterEdgeSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "ALTER EDGE ";
    buf += *name_;
    if (opts_ != nullptr) {
        for (auto &schemaOpt : opts_->alterSchemaItems()) {
            buf += " ";
            buf += schemaOpt->toString();
        }
    }
    if (schemaProps_ != nullptr) {
        buf +=  schemaProps_->toString();
    }
    return buf;
}


std::string DescribeTagSentence::toString() const {
    return folly::stringPrintf("DESCRIBE TAG %s", name_.get()->c_str());
}


std::string DescribeEdgeSentence::toString() const {
    return folly::stringPrintf("DESCRIBE EDGE %s", name_.get()->c_str());
}


std::string DropTagSentence::toString() const {
    return folly::stringPrintf("DROP TAG %s", name_.get()->c_str());
}


std::string DropEdgeSentence::toString() const {
    return folly::stringPrintf("DROP EDGE %s", name_.get()->c_str());
}
}   // namespace nebula
