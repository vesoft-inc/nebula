/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "base/Base.h"
#include "parser/MutateSentences.h"

namespace nebula {

std::string PropertyList::toString() const {
    std::vector<std::string> propStrs;
    for (auto &prop : properties_) {
        propStrs.push_back(*prop);
    }
    return folly::join(",", propStrs);
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
    buf.resize(buf.size() - 1);

    return buf;
}


std::string ValueList::toString() const {
    std::vector<std::string> valueStrs;
    for (auto &expr : values_) {
        valueStrs.push_back(expr->toString());
    }
    return folly::join(",", valueStrs);
}


std::string VertexRowItem::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += std::to_string(id_);
    buf += ":";
    buf += "(";
    buf += values_->toString();
    buf += ")";
    return buf;
}


std::string VertexRowList::toString() const {
    std::vector<std::string> vertexRowStrs;
    for (auto &item : rows_) {
        vertexRowStrs.push_back(item->toString());
    }
    return folly::join(",", vertexRowStrs);
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

    buf += std::to_string(srcid_);
    buf += "->";
    buf += std::to_string(dstid_);
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
    std::vector<std::string> edgeRowStrs;
    for (auto &item : rows_) {
        edgeRowStrs.push_back(item->toString());
    }
    return folly::join(",", edgeRowStrs);
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
    std::vector<std::string> updateItemstrs;
    for (auto &item : items_) {
        updateItemstrs.push_back(item->toString());
    }
    return folly::join(",", updateItemstrs);
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

std::string DeleteVertexSentence::toString() const {
    std::string buf;
    buf.reserve(256);
    buf += "DELETE VERTEX ";
    buf += srcNodeList_->toString();
    if (whereClause_ != nullptr) {
        buf += " ";
        buf += whereClause_->toString();
    }
    return buf;
}

std::string EdgeList::toString() const {
    std::vector<std::string> edgeStrs;
    for (auto edge : edges_) {
        std::string edgeStr = std::to_string(edge.first);
        edgeStr += "->";
        edgeStr += std::to_string(edge.second);
        edgeStrs.push_back(edgeStr);
    }
    return folly::join(",", edgeStrs);
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

}   // namespace nebula
