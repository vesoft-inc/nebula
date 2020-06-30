/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "context/Iterator.h"

#include "common/datatypes/Vertex.h"
#include "common/datatypes/Edge.h"
#include "common/interface/gen-cpp2/common_types.h"

namespace nebula {
namespace graph {

GetNeighborsIter::GetNeighborsIter(std::shared_ptr<Value> value)
    : Iterator(value, Kind::kGetNeighbors) {
    if (!value->isList()) {
        clear();
        return;
    }
    int64_t segment = 0;
    for (auto& val : value_->getList().values) {
        if (!val.isDataSet()) {
            clear();
            return;
        }
        auto& ds = val.getDataSet();
        auto& colNames = ds.colNames;
        auto buildResult = buildIndex(colNames);
        if (!buildResult.ok()) {
            LOG(ERROR) << "Build index error: " << buildResult.status();
            clear();
            return;
        }
        size_t edgeStartIndex = buildResult.value();
        segments_.emplace_back(&ds);
        for (auto& row : ds.rows) {
            auto& cols = row.values;
            for (size_t column = edgeStartIndex; column < cols.size() - 1; ++column) {
                if (!cols[column].isList()) {
                    // Ignore the bad value.
                    continue;
                }
                for (auto& edge : cols[column].getList().values) {
                    if (!edge.isList()) {
                        // Ignore the bad value.
                        continue;
                    }
                    auto& tagEdgeNameIndex = tagEdgeNameIndices_[segment];
                    auto edgeName = tagEdgeNameIndex.find(column);
                    DCHECK(edgeName != tagEdgeNameIndex.end());
                    logicalRows_.emplace_back(
                            std::make_tuple(segment, &row, edgeName->second, &edge.getList()));
                }
            }
        }
        ++segment;
    }
    iter_ = logicalRows_.begin();
    valid_ = true;
}

StatusOr<int64_t> GetNeighborsIter::buildIndex(const std::vector<std::string>& colNames) {
    if (colNames.size() < 3
            || colNames[0] != "_vid"
            || colNames[1].find("_stats") != 0
            || colNames.back().find("_expr") != 0) {
        return Status::Error("Bad column names.");
    }
    Status status;
    std::unordered_map<std::string, size_t> colIndex;
    TagEdgeNameIdxMap tagEdgeNameIndex;
    int64_t edgeStartIndex = -1;
    tagPropIndices_.emplace_back();
    edgePropIndices_.emplace_back();
    tagPropMaps_.emplace_back();
    edgePropMaps_.emplace_back();
    for (size_t i = 0; i < colNames.size(); ++i) {
        colIndex.emplace(colNames[i], i);
        auto& colName = colNames[i];
        if (colName.find("_tag") == 0) {
            status = buildPropIndex(colName, i, false,
                    tagEdgeNameIndex, tagPropIndices_.back(), tagPropMaps_.back());
            if (!status.ok()) {
                return status;
            }
        } else if (colName.find("_edge") == 0) {
            status = buildPropIndex(colName, i, true,
                    tagEdgeNameIndex, edgePropIndices_.back(), edgePropMaps_.back());
            if (!status.ok()) {
                return status;
            }
            if (edgeStartIndex < 0) {
                edgeStartIndex = i;
            }
        } else {
            // It is "_vid", "_stats", "_expr" in this situation.
        }
    }
    tagEdgeNameIndices_.emplace_back(std::move(tagEdgeNameIndex));
    colIndices_.emplace_back(std::move(colIndex));
    return edgeStartIndex;
}

Status GetNeighborsIter::buildPropIndex(const std::string& props,
                                       size_t columnId,
                                       bool isEdge,
                                       TagEdgeNameIdxMap& tagEdgeNameIndex,
                                       TagEdgePropIdxMap& tagEdgePropIdxMap,
                                       TagEdgePropMap& tagEdgePropMap) {
    std::vector<std::string> pieces;
    folly::split(":", props, pieces);
    PropIdxMap kv;
    if (pieces.size() < 2) {
        return Status::Error("Bad column name format: %s", props.c_str());
    }

    // if size == 2, it is the tag defined without props.
    if (pieces.size() > 2) {
        for (size_t i = 2; i < pieces.size(); ++i) {
            kv.emplace(pieces[i], i - 2);
        }
    }

    std::string name = pieces[1];
    if (isEdge) {
        // The first character of the tag/edge name is +/-.
        // It's not used for now.
        if (name.find("+") != 0 && name.find("-") != 0) {
            return Status::Error("Bad edge name: %s", name.c_str());
        }
        auto edgeName = name.substr(1, name.size());
        tagEdgePropIdxMap.emplace(edgeName, std::make_pair(columnId, std::move(kv)));
        pieces.erase(pieces.begin(), pieces.begin() + 2);
        auto propList = std::make_pair(columnId, std::move(pieces));
        tagEdgePropMap.emplace(edgeName, std::move(propList));
        tagEdgeNameIndex.emplace(columnId, edgeName);
    } else {
        tagEdgePropIdxMap.emplace(name, std::make_pair(columnId, std::move(kv)));
        pieces.erase(pieces.begin(), pieces.begin() + 2);
        auto propList = std::make_pair(columnId, std::move(pieces));
        tagEdgePropMap.emplace(name, std::move(propList));
        tagEdgeNameIndex.emplace(columnId, name);
    }

    return Status::OK();
}

const Value& GetNeighborsIter::getColumn(const std::string& col) const {
    if (!valid()) {
        return Value::kNullValue;
    }
    auto segment = currentSeg();
    auto& index = colIndices_[segment];
    auto found = index.find(col);
    if (found == index.end()) {
        return Value::kNullValue;
    }
    auto row = currentRow();
    return row->values[found->second];
}

const Value& GetNeighborsIter::getTagProp(const std::string& tag,
                                          const std::string& prop) const {
    if (!valid()) {
        return Value::kNullValue;
    }

    auto segment = currentSeg();
    auto index = tagPropIndices_[segment].find(tag);
    if (index == tagPropIndices_[segment].end()) {
        return Value::kNullValue;
    }
    auto propIndex = index->second.second.find(prop);
    if (propIndex == index->second.second.end()) {
        return Value::kNullValue;
    }
    auto colId = index->second.first;
    auto& row = *currentRow();
    DCHECK_GT(row.size(), colId);
    if (!row[colId].isList()) {
        return Value::kNullBadType;
    }
    auto& list = row[colId].getList();
    return list.values[propIndex->second];
}

const Value& GetNeighborsIter::getEdgeProp(const std::string& edge,
                                           const std::string& prop) const {
    if (!valid()) {
        return Value::kNullValue;
    }

    auto currentEdge = currentEdgeName();
    if (edge != "*" && currentEdge != edge) {
        VLOG(1) << "Current edge: " << currentEdgeName() << " Wanted: " << edge;
        return Value::kNullValue;
    }
    auto segment = currentSeg();
    auto index = edgePropIndices_[segment].find(currentEdge);
    if (index == edgePropIndices_[segment].end()) {
        VLOG(1) << "No edge found: " << edge;
        return Value::kNullValue;
    }
    auto propIndex = index->second.second.find(prop);
    if (propIndex == index->second.second.end()) {
        VLOG(1) << "No edge prop found: " << prop;
        return Value::kNullValue;
    }
    auto* list = currentEdgeProps();
    return list->values[propIndex->second];
}

Value GetNeighborsIter::getVertex() const {
    if (!valid()) {
        return Value::kNullValue;
    }

    auto segment = currentSeg();
    auto vidVal = getColumn("_vid");
    if (!vidVal.isStr()) {
        return Value::kNullBadType;
    }
    Vertex vertex;
    vertex.vid = vidVal.getStr();
    auto& tagPropMap = tagPropMaps_[segment];
    for (auto& tagProp : tagPropMap) {
        auto& row = *currentRow();
        auto& tagPropNameList = tagProp.second.second;
        auto tagColId = tagProp.second.first;
        if (!row[tagColId].isList()) {
            // Ignore the bad value.
            continue;
        }
        DCHECK_GE(row.size(), tagColId);
        auto& propList = row[tagColId].getList();
        DCHECK_EQ(tagPropNameList.size(), propList.values.size());
        Tag tag;
        tag.name = tagProp.first;
        for (size_t i = 0; i < propList.size(); ++i) {
            tag.props.emplace(tagPropNameList[i], propList[i]);
        }
        vertex.tags.emplace_back(std::move(tag));
    }
    return Value(std::move(vertex));
}

Value GetNeighborsIter::getEdge() const {
    if (!valid()) {
        return Value::kNullValue;
    }

    auto segment = currentSeg();
    Edge edge;
    auto& edgeName = currentEdgeName();
    edge.name = edgeName;
    auto& src = getColumn("_vid");
    if (!src.isStr()) {
        return Value::kNullBadType;
    }
    edge.src = src.getStr();

    auto& dst = getEdgeProp(edgeName, _DST);
    if (!dst.isStr()) {
        return Value::kNullBadType;
    }
    edge.dst = dst.getStr();

    auto& rank = getEdgeProp(edgeName, _RANK);
    if (!rank.isInt()) {
        return Value::kNullBadType;
    }
    edge.ranking = rank.getInt();
    edge.type = 0;

    auto& edgePropMap = edgePropMaps_[segment];
    auto edgeProp = edgePropMap.find(edgeName);
    if (edgeProp == edgePropMap.end()) {
        return Value::kNullValue;
    }
    auto& edgeNamePropList = edgeProp->second.second;
    auto& propList = currentEdgeProps()->values;
    DCHECK_EQ(edgeNamePropList.size(), propList.size());
    for (size_t i = 0; i < propList.size(); ++i) {
        auto propName = edgeNamePropList[i];
        if (propName == _SRC || propName == _DST
                || propName == _RANK || propName == _TYPE) {
            continue;
        }
        edge.props.emplace(edgeNamePropList[i], propList[i]);
    }
    return Value(std::move(edge));
}
}  // namespace graph
}  // namespace nebula
