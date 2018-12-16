/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
    buf.resize(buf.size() - 1);
    return buf;
}

std::string ValueList::toString() const {
    std::string buf;
    buf.reserve(256);
    for (auto &expr : values_) {
        buf += expr->toString();
        buf += ",";
    }
    buf.resize(buf.size() - 1);
    return buf;
}

std::string InsertVertexSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "INSERT VERTEX ";
    buf += *vertex_;
    buf += "(";
    buf += properties_->toString();
    buf += ") VALUES(";
    buf += std::to_string(id_);
    buf += ": ";
    buf += values_->toString();
    buf += ")";
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
    buf += ") ";
    buf += "VALUES(";
    buf += std::to_string(srcid_);
    buf += " -> ";
    buf += std::to_string(dstid_);
    if (rank_ != 0) {
        buf += " @";
        buf += std::to_string(rank_);
    }
    buf += ": ";
    buf += values_->toString();
    buf += ")";
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
    buf.resize(buf.size() - 1);
    return buf;
}

std::string UpdateVertexSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "UPDATE ";
    if (insertable_) {
        buf += "OR INSERT ";
    }
    buf += "VERTEX ";
    buf += std::to_string(vid_);
    buf += " SET ";
    buf += updateItems_->toString();
    if (whereClause_ != nullptr) {
        buf += " ";
        buf += whereClause_->toString();
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
    buf += "UPDATE ";
    if (insertable_) {
        buf += "OR INSERT ";
    }
    buf += "EDGE ";
    buf += std::to_string(srcid_);
    buf += "->";
    buf += std::to_string(dstid_);
    buf += " SET ";
    buf += updateItems_->toString();
    if (whereClause_ != nullptr) {
        buf += " ";
        buf += whereClause_->toString();
    }
    if (yieldClause_ != nullptr) {
        buf += " ";
        buf += yieldClause_->toString();
    }

    return buf;
}

}   // namespace nebula
