/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include "parser/MutateSentences.h"

namespace nebula {

std::string PropertyList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &prop : properties_) {
        buf += *prop;
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}


std::string VertexTagItem::toString() const {
    std::string buf;
    buf.reserve(256);

    buf += *tagName_;
    if (properties_ != nullptr) {
        buf += "(";
        buf += properties_->toString();
        buf += ")";
    }

    return buf;
}


std::string VertexTagList::toString() const {
    std::string buf;
    buf.reserve(256);

    for (auto &item : tagItems_) {
        buf += item->toString();
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}


std::string ValueList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &expr : values_) {
        buf += expr->toString();
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}


std::string VertexRowItem::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += id_->toString();
    buf += ":";
    buf += "(";
    buf += values_->toString();
    buf += ")";
    return buf;
}


std::string VertexRowList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &item : rows_) {
        buf += item->toString();
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}


std::string InsertVertexSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "INSERT VERTEX ";
    buf += tagList_->toString();
    buf += " VALUES ";
    buf += rows_->toString();
    return buf;
}


std::string EdgeRowItem::toString() const {
    std::string buf;
    buf.reserve(256);

    buf += srcid_->toString();
    buf += "->";
    buf += dstid_->toString();
    if (rank_ != 0) {
        buf += "@";
        buf += std::to_string(rank_);
    }
    buf += ":";

    buf += "(";
    buf += values_->toString();
    buf += ")";
    return buf;
}


std::string EdgeRowList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &item : rows_) {
        buf += item->toString();
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}


std::string InsertEdgeSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "INSERT EDGE ";
    if (!overwritable_) {
        buf += "NO OVERWRITE ";
    }
    buf += *edge_;
    buf += "(";
    buf += properties_->toString();
    buf += ") VALUES";
    buf += rows_->toString();
    return buf;
}


std::string UpdateItem::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += *field_;
    buf += "=";
    buf += value_->toString();
    return buf;
}


std::string UpdateList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &item : items_) {
        buf += item->toString();
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}


std::string UpdateVertexSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    if (insertable_) {
        buf += "UPSERT ";
    } else {
        buf += "UPDATE ";
    }
    buf += "VERTEX ";
    buf += vid_->toString();
    buf += " SET ";
    buf += updateList_->toString();
    if (whenClause_ != nullptr) {
        buf += " ";
        buf += whenClause_->toString();
    }
    if (yieldClause_ != nullptr) {
        buf += " ";
        buf += yieldClause_->toString();
    }

    return buf;
}


std::string UpdateEdgeSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    if (insertable_) {
        buf += "UPSERT ";
    } else {
        buf += "UPDATE ";
    }
    buf += "EDGE ";
    buf += srcid_->toString();
    buf += "->";
    buf += dstid_->toString();
    if (hasRank_) {
        buf += " AT" + std::to_string(rank_);
    }
    buf += " OF " + *edgeType_;
    buf += " SET ";
    buf += updateList_->toString();
    if (whenClause_ != nullptr) {
        buf += " ";
        buf += whenClause_->toString();
    }
    if (yieldClause_ != nullptr) {
        buf += " ";
        buf += yieldClause_->toString();
    }

    return buf;
}

std::string DeleteVertexSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "DELETE VERTEX ";
    buf += vid_->toString();
    return buf;
}

std::string EdgeList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &edge : edges_) {
        buf += edge.first->toString();
        buf += "->";
        buf += edge.second->toString();
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}

std::string DeleteEdgeSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "DELETE EDGE ";
    buf += edgeList_->toString();
    if (whereClause_ != nullptr) {
        buf += " ";
        buf += whereClause_->toString();
    }
    return buf;
}

std::string DownloadSentence::toString() const {
    return folly::stringPrintf("DOWNLOAD HDFS \"%s:%d/%s\"", host_.get()->c_str(),
                               port_, path_.get()->c_str());
}

std::string IngestSentence::toString() const {
    return "INGEST";
}

std::string CompactionSentence::toString() const {
    return "COMPACTION";
}
}   // namespace nebula
