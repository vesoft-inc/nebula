/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/context/iterator/PropIter.h"

#include "common/datatypes/Edge.h"
#include "common/datatypes/Vertex.h"
#include "graph/util/SchemaUtil.h"

namespace nebula {
namespace graph {

PropIter::PropIter(const PropIter& iter) : SequentialIter(iter) {
  dsIndex_ = iter.dsIndex_;
  kind_ = Kind::kProp;
}

PropIter::PropIter(std::shared_ptr<Value> value, bool checkMemory)
    : SequentialIter(value, checkMemory) {
  DCHECK(value->isDataSet());
  auto& ds = value->getDataSet();
  auto status = makeDataSetIndex(ds);
  if (UNLIKELY(!status.ok())) {
    DLOG(FATAL) << status;
    clear();
    return;
  }
  kind_ = Kind::kProp;
}

Status PropIter::makeDataSetIndex(const DataSet& ds) {
  dsIndex_.ds = &ds;
  auto& colNames = ds.colNames;
  for (size_t i = 0; i < colNames.size(); ++i) {
    dsIndex_.colIndices.emplace(colNames[i], i);
    auto& colName = colNames[i];
    if (colName.find(".") != std::string::npos) {
      NG_RETURN_IF_ERROR(buildPropIndex(colName, i));
    }
  }
  return Status::OK();
}

Status PropIter::buildPropIndex(const std::string& props, size_t columnId) {
  std::vector<std::string> pieces;
  folly::split(".", props, pieces);
  if (UNLIKELY(pieces.size() != 2)) {
    return Status::Error("Bad column name format: %s", props.c_str());
  }
  std::string name = pieces[0];
  auto& propsMap = dsIndex_.propsMap;
  if (propsMap.find(name) != propsMap.end()) {
    propsMap[name].emplace(pieces[1], columnId);
  } else {
    std::unordered_map<std::string, size_t> propIndices;
    propIndices.emplace(pieces[1], columnId);
    propsMap.emplace(name, std::move(propIndices));
  }
  return Status::OK();
}

const Value& PropIter::getColumn(const std::string& col) const {
  if (!valid()) {
    return Value::kNullValue;
  }

  auto index = dsIndex_.colIndices.find(col);
  if (index == dsIndex_.colIndices.end()) {
    return Value::kNullValue;
  }
  auto& row = *iter_;
  DCHECK_LT(index->second, row.values.size());
  return row.values[index->second];
}

StatusOr<std::size_t> PropIter::getColumnIndex(const std::string& col) const {
  auto index = dsIndex_.colIndices.find(col);
  if (index == dsIndex_.colIndices.end()) {
    return Status::Error("Don't exist column `%s'.", col.c_str());
  }
  return index->second;
}

const Value& PropIter::getProp(const std::string& name, const std::string& prop) const {
  if (!valid()) {
    return Value::kNullValue;
  }
  auto& propsMap = dsIndex_.propsMap;
  size_t colId = 0;
  auto& row = *iter_;
  if (name == "*") {
    for (auto& index : propsMap) {
      auto propIndex = index.second.find(prop);
      if (propIndex == index.second.end()) {
        continue;
      }
      colId = propIndex->second;
      DCHECK_GT(row.size(), colId);
      auto& val = row[colId];
      if (val.empty()) {
        continue;
      } else {
        return val;
      }
    }
    return Value::kNullValue;
  } else {
    auto index = propsMap.find(name);
    if (index == propsMap.end()) {
      return Value::kEmpty;
    }
    auto propIndex = index->second.find(prop);
    if (propIndex == index->second.end()) {
      return Value::kNullValue;
    }
    colId = propIndex->second;
    DCHECK_GT(row.size(), colId);
    return row[colId];
  }
}

Value PropIter::getVertex(const std::string& name) {
  UNUSED(name);
  if (!valid()) {
    return Value::kNullValue;
  }

  auto vidVal = getColumn(nebula::kVid);
  if (!SchemaUtil::isValidVid(vidVal)) {
    return Value::kNullValue;
  }
  Vertex vertex;
  vertex.vid = vidVal;
  auto& tagPropsMap = dsIndex_.propsMap;
  bool isVertexProps = true;
  auto& row = *iter_;
  // tagPropsMap -> <std::string, std::unordered_map<std::string, size_t> >
  for (auto& tagProp : tagPropsMap) {
    // propIndex -> std::unordered_map<std::string, size_t>
    for (auto& propIndex : tagProp.second) {
      if (row[propIndex.second].empty()) {
        // Not current vertex's prop
        isVertexProps = false;
        break;
      }
    }
    if (!isVertexProps) {
      isVertexProps = true;
      continue;
    }
    Tag tag;
    tag.name = tagProp.first;
    for (auto& propIndex : tagProp.second) {
      if (propIndex.first == nebula::kTag) {  // "_tag"
        continue;
      } else {
        tag.props.emplace(propIndex.first, row[propIndex.second]);
      }
    }
    vertex.tags.emplace_back(std::move(tag));
  }
  return Value(std::move(vertex));
}

Value PropIter::getEdge() const {
  if (!valid()) {
    return Value::kNullValue;
  }
  Edge edge;
  auto& edgePropsMap = dsIndex_.propsMap;
  bool isEdgeProps = true;
  auto& row = *iter_;
  for (auto& edgeProp : edgePropsMap) {
    for (auto& propIndex : edgeProp.second) {
      if (row[propIndex.second].empty()) {
        // Not current edge's prop
        isEdgeProps = false;
        break;
      }
    }
    if (!isEdgeProps) {
      isEdgeProps = true;
      continue;
    }
    auto edgeName = edgeProp.first;
    edge.name = edgeProp.first;

    auto type = getEdgeProp(edgeName, kType);
    if (!type.isInt()) {
      return Value::kNullBadType;
    }
    edge.type = type.getInt();

    auto& srcVal = getEdgeProp(edgeName, kSrc);
    if (!SchemaUtil::isValidVid(srcVal)) {
      return Value::kNullBadType;
    }
    edge.src = srcVal;

    auto& dstVal = getEdgeProp(edgeName, kDst);
    if (!SchemaUtil::isValidVid(dstVal)) {
      return Value::kNullBadType;
    }
    edge.dst = dstVal;

    auto rank = getEdgeProp(edgeName, kRank);
    if (!rank.isInt()) {
      return Value::kNullBadType;
    }
    edge.ranking = rank.getInt();

    for (auto& propIndex : edgeProp.second) {
      if (propIndex.first == kSrc || propIndex.first == kDst || propIndex.first == kType ||
          propIndex.first == kRank) {
        continue;
      }
      DCHECK_LT(propIndex.second, row.size());
      edge.props.emplace(propIndex.first, row[propIndex.second]);
    }
    return Value(std::move(edge));
  }
  return Value::kNullValue;
}

List PropIter::getVertices() {
  DCHECK(iter_ == rows_->begin());
  List vertices;
  for (; valid(); next()) {
    vertices.values.emplace_back(getVertex());
  }
  reset();
  return vertices;
}

List PropIter::getEdges() {
  DCHECK(iter_ == rows_->begin());
  List edges;
  edges.values.reserve(size());
  for (; valid(); next()) {
    auto edge = getEdge();
    if (edge.isEdge()) {
      const_cast<Edge&>(edge.getEdge()).format();
    }
    edges.values.emplace_back(std::move(edge));
  }
  reset();
  return edges;
}

const Value& PropIter::getColumn(int32_t index) const {
  return getColumnByIndex(index, iter_);
}

}  // namespace graph
}  // namespace nebula
