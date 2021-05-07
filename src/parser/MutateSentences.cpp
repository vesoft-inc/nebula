/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
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
    buf += "(";
    if (properties_ != nullptr) {
        buf += properties_->toString();
    }
    buf += ")";

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


std::string InsertVerticesSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "INSERT VERTEX ";
    if (ifNotExists_) {
        buf += "IF NOT EXISTS ";
    }
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


std::string InsertEdgesSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "INSERT EDGE ";
    if (ifNotExists_) {
        buf += "IF NOT EXISTS ";
    }
    buf += *edge_;
    buf += "(";
    buf += properties_->toString();
    buf += ") VALUES ";
    buf += rows_->toString();
    return buf;
}


std::string UpdateItem::toString() const {
    std::string buf;
    buf.reserve(256);
    if (fieldStr_ != nullptr)  {
        buf += *fieldStr_;
    } else if (fieldExpr_ != nullptr) {
        buf += fieldExpr_->toString();
    }

    buf += "=";
    buf += value_->toString();
    return buf;
}

StatusOr<std::string> UpdateItem::toEvaledString() const {
    return Status::Error(std::string(""));
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

StatusOr<std::string> UpdateList::toEvaledString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &item : items_) {
        auto ret = item->toEvaledString();
        if (!ret.ok()) {
            return ret.status();
        }
        buf += ret.value();
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
    if (name_ != nullptr) {
        buf += "ON " + *name_ + " ";
    }
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
    buf += srcId_->toString();
    buf += "->";
    buf += dstId_->toString();
    buf += "@" + std::to_string(rank_);
    buf += " OF " + *name_;
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

std::string DeleteVerticesSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "DELETE VERTEX ";
    buf += vertices_->toString();
    return buf;
}

std::string DeleteEdgesSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "DELETE EDGE ";
    buf += *edge_;
    buf += " ";
    if (edgeKeyRef_ != nullptr) {
        buf += edgeKeyRef_->toString();
    } else {
        buf += edgeKeys_->toString();
    }
    return buf;
}

std::string DownloadSentence::toString() const {
    return folly::stringPrintf("DOWNLOAD HDFS \"hdfs://%s:%d%s\"", host_.get()->c_str(),
                               port_, path_.get()->c_str());
}

std::string IngestSentence::toString() const {
    return "INGEST";
}

}   // namespace nebula
