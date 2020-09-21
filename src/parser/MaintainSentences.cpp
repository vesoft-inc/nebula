/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "parser/MaintainSentences.h"

namespace nebula {

std::ostream& operator<<(std::ostream& os, meta::cpp2::PropertyType type) {
    switch (type) {
        case meta::cpp2::PropertyType::INT64:
            os << "INT64";
            break;
        case meta::cpp2::PropertyType::BOOL:
            os << "BOOL";
            break;
        case meta::cpp2::PropertyType::DOUBLE:
            os << "DOUBLE";
            break;
        case meta::cpp2::PropertyType::STRING:
            os << "STRING";
            break;
        // TODO:
        default:
            break;
    }
    return os;
}

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
        std::stringstream ss;
        ss << col->type();
        buf += ss.str();
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
        std::stringstream ss;
        ss << col->type();
        buf += ss.str();
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
    if (columns_ != nullptr) {
        auto colSpecs = columns_->columnSpecs();
        for (auto &col : colSpecs) {
            buf += *col->name();
            buf += " ";
            std::stringstream ss;
            ss << col->type();
            buf += ss.str();
            buf += ",";
        }
        if (!colSpecs.empty()) {
            buf.resize(buf.size() - 1);
        }
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


std::string CreateTagIndexSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "CREATE TAG INDEX ";
    buf += *indexName_;
    buf += " ON ";
    buf += *tagName_;
    buf += " (";
    std::string columns;
    folly::join(", ", this->columns(), columns);
    buf += columns;
    buf += ")";
    return buf;
}


std::string CreateEdgeIndexSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "CREATE EDGE INDEX ";
    buf += *indexName_;
    buf += " ON ";
    buf += *edgeName_;
    buf += " (";
    std::string columns;
    folly::join(", ", this->columns(), columns);
    buf += columns;
    buf += ")";
    return buf;
}


std::string DescribeTagIndexSentence::toString() const {
    return folly::stringPrintf("DESCRIBE TAG INDEX %s", indexName_.get()->c_str());
}


std::string DescribeEdgeIndexSentence::toString() const {
    return folly::stringPrintf("DESCRIBE EDGE INDEX %s", indexName_.get()->c_str());
}


std::string DropTagIndexSentence::toString() const {
    return folly::stringPrintf("DROP TAG INDEX %s", indexName_.get()->c_str());
}


std::string DropEdgeIndexSentence::toString() const {
    return folly::stringPrintf("DROP EDGE INDEX %s", indexName_.get()->c_str());
}


std::string RebuildTagIndexSentence::toString() const {
    return folly::stringPrintf("BUILD TAG INDEX %s", indexName_.get()->c_str());
}


std::string RebuildEdgeIndexSentence::toString() const {
    return folly::stringPrintf("BUILD EDGE INDEX %s", indexName_.get()->c_str());
}

std::string ShowTagsSentence::toString() const {
    return folly::stringPrintf("SHOW TAGS");
}

std::string ShowEdgesSentence::toString() const {
    return folly::stringPrintf("SHOW EDGES");
}

std::string ShowCreateTagSentence::toString() const {
    return folly::stringPrintf("SHOW CREATE TAG %s", name_.get()->c_str());
}

std::string ShowCreateEdgeSentence::toString() const {
    return folly::stringPrintf("SHOW CREATE EDGE %s", name_.get()->c_str());
}

std::string ShowTagIndexesSentence::toString() const {
    return folly::stringPrintf("SHOW TAG INDEXES");
}

std::string ShowEdgeIndexesSentence::toString() const {
    return folly::stringPrintf("SHOW EDGE INDEXES");
}

std::string ShowCreateTagIndexSentence::toString() const {
    return folly::stringPrintf("SHOW CREATE TAG INDEX %s", indexName_.get()->c_str());
}

std::string ShowCreateEdgeIndexSentence::toString() const {
    return folly::stringPrintf("SHOW CREATE EDGE INDEX %s", indexName_.get()->c_str());
}

}   // namespace nebula
