/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "context/Iterator.h"

namespace nebula {
namespace graph {

GetNeighborsIter::GetNeighborsIter(std::shared_ptr<Value> value) : Iterator(value) {
    DCHECK(value->isList());
    int64_t segment = 0;
    for (auto& val : value_->getList().values) {
        DCHECK(val.type() == Value::Type::DATASET);
        auto& ds = val.getDataSet();
        auto& colNames = ds.colNames;
        auto edgeStartIndex = buildIndex(colNames);
        segments_.emplace_back(&ds);
        for (auto& row : ds.rows) {
            auto& cols = row.columns;
            for (size_t column = edgeStartIndex; column < cols.size(); ++column) {
                if (!cols[column].isList()) {
                    continue;
                }
                for (auto& edge : cols[column].getList().values) {
                    DCHECK(edge.isList());
                    edges_.emplace_back(std::make_tuple(segment, &row, column, &edge.getList()));
                }
            }
        }
        ++segment;
    }
    iter_ = edges_.begin();
}

int64_t GetNeighborsIter::buildIndex(const std::vector<std::string>& colNames) {
    std::unordered_map<std::string, int64_t> kv;
    int64_t edgeStartIndex = -1;
    tagPropIndex_.emplace_back();
    edgePropIndex_.emplace_back();
    for (size_t i = 0; i < colNames.size(); ++i) {
        kv.emplace(colNames[i], i);
        if (colNames[i].find("_tag") == 0) {
            auto ret = buildPropIndex(colNames[i]);
            tagPropIndex_.back().emplace(std::move(ret));
            continue;
        }
        if (colNames[i].find("_edge") == 0) {
            auto ret = buildPropIndex(colNames[i]);
            edgePropIndex_.back().emplace(std::move(ret));
            if (edgeStartIndex < 0) {
                edgeStartIndex = i;
            }
        }
    }
    colIndex_.emplace_back(std::move(kv));
    return edgeStartIndex;
}

std::pair<std::string, std::unordered_map<std::string, int64_t>>
GetNeighborsIter::buildPropIndex(const std::string& props) {
    std::vector<std::string> pieces;
    folly::split(":", props, pieces);
    std::unordered_map<std::string, int64_t> kv;
    DCHECK_GE(pieces.size(), 2);
    for (size_t i = 2; i < pieces.size(); ++i) {
        kv.emplace(pieces[i], i - 2);
    }
    return std::make_pair(pieces[1], std::move(kv));
}

const Value& GetNeighborsIter::getColumn(const std::string& col) const {
    if (!valid()) {
        return kNullValue;
    }
    auto& current = *iter_;
    auto segment = std::get<0>(current);
    auto& index = colIndex_[segment];
    auto found = index.find(col);
    if (found == index.end()) {
        return kNullValue;
    }
    auto row = std::get<1>(current);
    return row->columns[found->second];
}

const Value& GetNeighborsIter::getTagProp(const std::string& tag,
                                          const std::string& prop) const {
    if (!valid()) {
        return kNullValue;
    }
    auto& current = *iter_;
    auto segment = std::get<0>(current);
    auto index = tagPropIndex_[segment].find(tag);
    if (index == tagPropIndex_[segment].end()) {
        return kNullValue;
    }
    auto propIndex = index->second.find(prop);
    if (propIndex == index->second.end()) {
        return kNullValue;
    }
    auto& list = std::get<3>(current);
    return list->values[propIndex->second];
}

const Value& GetNeighborsIter::getEdgeProp(const std::string& edge,
                                           const std::string& prop) const {
    if (!valid()) {
        return kNullValue;
    }
    auto& current = *iter_;
    auto segment = std::get<0>(current);
    auto index = edgePropIndex_[segment].find(edge);
    if (index == edgePropIndex_[segment].end()) {
        return kNullValue;
    }
    auto propIndex = index->second.find(prop);
    if (propIndex == index->second.end()) {
        return kNullValue;
    }
    auto& list = std::get<3>(current);
    return list->values[propIndex->second];
}


}  // namespace graph
}  // namespace nebula
