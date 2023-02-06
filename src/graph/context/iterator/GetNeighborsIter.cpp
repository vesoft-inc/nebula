/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/context/iterator/GetNeighborsIter.h"

#include "common/algorithm/ReservoirSampling.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/Vertex.h"
#include "graph/util/SchemaUtil.h"

namespace nebula {
namespace graph {

GetNeighborsIter::GetNeighborsIter(std::shared_ptr<Value> value, bool checkMemory)
    : Iterator(value, Kind::kGetNeighbors, checkMemory) {
  if (value == nullptr) {
    return;
  }
  auto status = processList(value);
  if (UNLIKELY(!status.ok())) {
    LOG(ERROR) << status;
    clear();
    return;
  }

  goToFirstEdge();
}

void GetNeighborsIter::goToFirstEdge() {
  // Go to first edge
  for (currentDs_ = dsIndices_.begin(); currentDs_ < dsIndices_.end(); ++currentDs_) {
    if (noEdge_) {
      currentRow_ = currentDs_->ds->rows.begin();
      valid_ = true;
      break;
    }
    for (currentRow_ = currentDs_->ds->rows.begin(); currentRow_ < currentDs_->ds->rows.end();
         ++currentRow_) {
      colIdx_ = currentDs_->colLowerBound + 1;
      while (colIdx_ < currentDs_->colUpperBound && !valid_) {
        const auto& currentCol = (*currentRow_)[colIdx_];
        if (!currentCol.isList() || currentCol.getList().empty()) {
          ++colIdx_;
          continue;
        }

        currentCol_ = &currentCol.getList();
        edgeIdxUpperBound_ = currentCol_->size();
        edgeIdx_ = 0;
        while (edgeIdx_ < edgeIdxUpperBound_ && !valid_) {
          const auto& currentEdge = currentCol_->operator[](edgeIdx_);
          if (!currentEdge.isList()) {
            ++edgeIdx_;
            continue;
          }
          currentEdge_ = &currentEdge.getList();
          valid_ = true;
        }  // `edgeIdx_'
        if (!valid_) {
          ++colIdx_;
        }
      }  // `colIdx_'
      if (valid_) {
        break;
      }
    }  // `currentRow_'
    if (valid_) {
      break;
    }
  }  // `currentDs_'

  if (valid_) {
    rowsUpperBound_ = currentDs_->ds->rows.end();
    bitIdx_ = 0;
    if (bitset_.empty()) {
      bitset_.push_back(true);
    } else if (!bitset_[bitIdx_]) {
      next();
    }
  }
}

Status GetNeighborsIter::processList(std::shared_ptr<Value> value) {
  if (UNLIKELY(!value->isList())) {
    std::stringstream ss;
    ss << "Value type is not list, type: " << value->type();
    return Status::Error(ss.str());
  }
  for (auto& val : value->getList().values) {
    if (UNLIKELY(!val.isDataSet())) {
      return Status::Error("There is a value in list which is not a data set.");
    }
    const auto& dataSet = val.getDataSet();
    if (dataSet.rowSize() != 0) {
      auto status = makeDataSetIndex(dataSet);
      NG_RETURN_IF_ERROR(status);
      dsIndices_.emplace_back(std::move(status).value());
    }
  }
  return Status::OK();
}

StatusOr<GetNeighborsIter::DataSetIndex> GetNeighborsIter::makeDataSetIndex(const DataSet& ds) {
  DataSetIndex dsIndex;
  dsIndex.ds = &ds;
  auto buildResult = buildIndex(&dsIndex);
  NG_RETURN_IF_ERROR(buildResult);
  return dsIndex;
}

bool checkColumnNames(const std::vector<std::string>& colNames) {
  return colNames.size() < 3 || colNames[0] != nebula::kVid || colNames[1].find("_stats") != 0 ||
         colNames.back().find("_expr") != 0;
}

StatusOr<int64_t> GetNeighborsIter::buildIndex(DataSetIndex* dsIndex) {
  auto& colNames = dsIndex->ds->colNames;
  if (UNLIKELY(checkColumnNames(colNames))) {
    return Status::Error("Bad column names.");
  }
  int64_t edgeStartIndex = -1;
  for (size_t i = 0; i < colNames.size(); ++i) {
    dsIndex->colIndices.emplace(colNames[i], i);
    auto& colName = colNames[i];
    if (colName.find(nebula::kTag) == 0) {  // "_tag"
      NG_RETURN_IF_ERROR(buildPropIndex(colName, i, false, dsIndex));
    } else if (colName.find("_edge") == 0) {
      NG_RETURN_IF_ERROR(buildPropIndex(colName, i, true, dsIndex));
      if (edgeStartIndex < 0) {
        edgeStartIndex = i;
      }
    } else {
      // It is "_vid", "_stats", "_expr" in this situation.
    }
  }
  if (edgeStartIndex == -1) {
    noEdge_ = true;
  }
  dsIndex->colLowerBound = edgeStartIndex - 1;
  dsIndex->colUpperBound = colNames.size() - 1;
  return edgeStartIndex;
}

Status GetNeighborsIter::buildPropIndex(const std::string& props,
                                        size_t columnId,
                                        bool isEdge,
                                        DataSetIndex* dsIndex) {
  std::vector<std::string> pieces;
  folly::split(":", props, pieces);
  if (UNLIKELY(pieces.size() < 2)) {
    return Status::Error("Bad column name format: %s", props.c_str());
  }

  PropIndex propIdx;
  // if size == 2, it is the tag defined without props.
  if (pieces.size() > 2) {
    for (size_t i = 2; i < pieces.size(); ++i) {
      propIdx.propIndices.emplace(pieces[i], i - 2);
    }
  }

  propIdx.colIdx = columnId;
  propIdx.propList.resize(pieces.size() - 2);
  std::move(pieces.begin() + 2, pieces.end(), propIdx.propList.begin());
  std::string name = pieces[1];
  if (isEdge) {
    // The first character of the edge name is +/-.
    if (UNLIKELY(name.empty() || (name[0] != '+' && name[0] != '-'))) {
      return Status::Error("Bad edge name: %s", name.c_str());
    }
    dsIndex->tagEdgeNameIndices.emplace(columnId, name);
    dsIndex->edgePropsMap.emplace(name, std::move(propIdx));
  } else {
    dsIndex->tagEdgeNameIndices.emplace(columnId, name);
    dsIndex->tagPropsMap.emplace(name, std::move(propIdx));
  }

  return Status::OK();
}

bool GetNeighborsIter::valid() const {
  return Iterator::valid() && valid_ && currentDs_ < dsIndices_.end() &&
         currentRow_ < rowsUpperBound_ && colIdx_ < currentDs_->colUpperBound;
}

void GetNeighborsIter::next() {
  if (!valid()) {
    return;
  }

  numRowsModN_++;

  if (noEdge_) {
    if (++currentRow_ < rowsUpperBound_) {
      return;
    }

    // go to next dataset
    if (++currentDs_ < dsIndices_.end()) {
      currentRow_ = currentDs_->ds->begin();
      rowsUpperBound_ = currentDs_->ds->end();
    }
    return;
  }

  while (++edgeIdx_ > -1) {
    if (edgeIdx_ < edgeIdxUpperBound_) {
      const auto& currentEdge = (*currentCol_)[edgeIdx_];
      if (!currentEdge.isList()) {
        continue;
      }
      ++bitIdx_;
      DCHECK_GT(bitIdx_, -1);
      if (static_cast<size_t>(bitIdx_) >= bitset_.size()) {
        bitset_.push_back(true);
      } else if (!bitset_[bitIdx_]) {
        VLOG(1) << "Filtered: " << currentEdge << " bitidx: " << bitIdx_;
        // current edge had been filtered.
        continue;
      }
      currentEdge_ = &currentEdge.getList();
      break;
    }

    // go to next column
    while (++colIdx_) {
      if (colIdx_ < currentDs_->colUpperBound) {
        const auto& currentCol = currentRow_->operator[](colIdx_);
        if (!currentCol.isList() || currentCol.getList().empty()) {
          continue;
        }

        currentCol_ = &currentCol.getList();
        edgeIdxUpperBound_ = currentCol_->size();
        edgeIdx_ = -1;
        break;
      }
      // go to next row
      if (++currentRow_ < rowsUpperBound_) {
        colIdx_ = currentDs_->colLowerBound;
        continue;
      }

      // go to next dataset
      if (++currentDs_ < dsIndices_.end()) {
        colIdx_ = currentDs_->colLowerBound;
        currentRow_ = currentDs_->ds->begin();
        rowsUpperBound_ = currentDs_->ds->end();
        continue;
      }
      break;
    }
    if (currentDs_ == dsIndices_.end()) {
      break;
    }
  }
}

size_t GetNeighborsIter::size() const {
  size_t count = 0;
  for (const auto& dsIdx : dsIndices_) {
    for (const auto& row : dsIdx.ds->rows) {
      for (const auto& edgeIdx : dsIdx.edgePropsMap) {
        const auto& cell = row[edgeIdx.second.colIdx];
        if (LIKELY(cell.isList())) {
          count += cell.getList().size();
        }
      }
    }
  }
  return count;
}

size_t GetNeighborsIter::numRows() const {
  size_t count = 0;
  for (const auto& dsIdx : dsIndices_) {
    count += dsIdx.ds->size();
  }
  return count;
}

void GetNeighborsIter::erase() {
  DCHECK_GE(bitIdx_, 0);
  DCHECK_LT(bitIdx_, bitset_.size());
  bitset_[bitIdx_] = false;
  next();
}

void GetNeighborsIter::sample(const int64_t count) {
  algorithm::ReservoirSampling<std::tuple<List*, List>> sampler_(count);
  doReset(0);
  for (; valid(); next()) {
    // <current List of Edge, value of Edge>
    std::tuple<List*, List> t =
        std::make_tuple(const_cast<List*>(currentCol_), std::move(*currentEdge_));
    sampler_.sampling(std::move(t));
  }
  doReset(0);
  clearEdges();
  auto samples = std::move(sampler_).samples();
  for (auto& sample : samples) {
    auto* col = std::get<0>(sample);
    col->emplace_back(std::move(std::get<1>(sample)));
  }
  doReset(0);
}

const Value& GetNeighborsIter::getColumn(const std::string& col) const {
  if (!valid()) {
    return Value::kNullValue;
  }
  auto& index = currentDs_->colIndices;
  auto found = index.find(col);
  if (found == index.end()) {
    return Value::kEmpty;
  }

  return currentRow_->values[found->second];
}

const Value& GetNeighborsIter::getColumn(int32_t index) const {
  DCHECK_LT(index, currentRow_->values.size());
  return currentRow_->values[index];
}

StatusOr<std::size_t> GetNeighborsIter::getColumnIndex(const std::string& col) const {
  auto& index = currentDs_->colIndices;
  auto found = index.find(col);
  if (found == index.end()) {
    return Status::Error("Don't exist column `%s'.", col.c_str());
  }
  return found->second;
}

const Value& GetNeighborsIter::getTagProp(const std::string& tag, const std::string& prop) const {
  if (!valid()) {
    return Value::kNullValue;
  }

  size_t colId = 0;
  size_t propId = 0;
  auto& row = *currentRow_;
  if (tag == "*") {
    for (auto& index : currentDs_->tagPropsMap) {
      auto propIndexIter = index.second.propIndices.find(prop);
      if (propIndexIter != index.second.propIndices.end()) {
        colId = index.second.colIdx;
        propId = propIndexIter->second;
        DCHECK_GT(row.size(), colId);
        if (row[colId].empty()) {
          continue;
        }
        if (!row[colId].isList()) {
          return Value::kNullBadType;
        }
        auto& list = row[colId].getList();
        auto& val = list.values[propId];
        if (val.empty()) {
          continue;
        } else {
          return val;
        }
      }
    }
    return Value::kEmpty;
  } else {
    auto& tagPropIndices = currentDs_->tagPropsMap;
    auto index = tagPropIndices.find(tag);
    if (index == tagPropIndices.end()) {
      return Value::kEmpty;
    }
    auto propIndexIter = index->second.propIndices.find(prop);
    if (propIndexIter == index->second.propIndices.end()) {
      return Value::kEmpty;
    }
    colId = index->second.colIdx;
    propId = propIndexIter->second;
    DCHECK_GT(row.size(), colId);
    if (row[colId].empty()) {
      return Value::kEmpty;
    }
    if (!row[colId].isList()) {
      return Value::kNullBadType;
    }
    auto& list = row[colId].getList();
    return list.values[propId];
  }
}

const Value& GetNeighborsIter::getEdgeProp(const std::string& edge, const std::string& prop) const {
  if (!valid()) {
    return Value::kNullValue;
  }

  if (noEdge_) {
    return Value::kEmpty;
  }

  auto& currentEdge = currentEdgeName();
  if (edge != "*" && (currentEdge.compare(1, std::string::npos, edge) != 0)) {
    DLOG(INFO) << "Current edge: " << currentEdgeName() << " Wanted: " << edge;
    return Value::kEmpty;
  }
  auto index = currentDs_->edgePropsMap.find(currentEdge);
  if (index == currentDs_->edgePropsMap.end()) {
    DLOG(INFO) << "No edge found: " << edge << " Current edge: " << currentEdge;
    return Value::kEmpty;
  }
  auto propIndex = index->second.propIndices.find(prop);
  if (propIndex == index->second.propIndices.end()) {
    VLOG(1) << "No edge prop found: " << prop;
    return Value::kEmpty;
  }
  return currentEdge_->values[propIndex->second];
}

Value GetNeighborsIter::getVertex(const std::string& name) {
  UNUSED(name);
  if (!valid()) {
    return Value::kNullValue;
  }
  auto vidVal = getColumn(0);
  if (!prevVertex_.empty() && prevVertex_.getVertex().vid == vidVal) {
    return prevVertex_;
  }

  Vertex vertex;
  vertex.vid = vidVal;
  auto& tagPropMap = currentDs_->tagPropsMap;
  for (auto& tagProp : tagPropMap) {
    auto& row = *currentRow_;
    auto& tagPropNameList = tagProp.second.propList;
    auto tagColId = tagProp.second.colIdx;
    if (UNLIKELY(!row[tagColId].isList())) {
      // Ignore the bad value.
      continue;
    }
    DCHECK_GE(row.size(), tagColId);
    auto& propList = row[tagColId].getList();
    DCHECK_EQ(tagPropNameList.size(), propList.values.size());
    Tag tag;
    tag.name = tagProp.first;
    for (size_t i = 0; i < propList.size(); ++i) {
      if (tagPropNameList[i] != nebula::kTag) {
        tag.props.emplace(tagPropNameList[i], propList[i]);
      }
    }
    vertex.tags.emplace_back(std::move(tag));
  }
  prevVertex_ = Value(std::move(vertex));
  return prevVertex_;
}

std::vector<Value> GetNeighborsIter::vids() {
  std::vector<Value> vids;
  vids.reserve(numRows());
  valid_ = true;
  colIdx_ = -2;
  for (currentDs_ = dsIndices_.begin(); currentDs_ < dsIndices_.end(); ++currentDs_) {
    rowsUpperBound_ = currentDs_->ds->rows.end();
    for (currentRow_ = currentDs_->ds->rows.begin(); currentRow_ < currentDs_->ds->rows.end();
         ++currentRow_) {
      vids.emplace_back(getColumn(0));
    }
  }
  reset();
  return vids;
}

List GetNeighborsIter::getVertices() {
  List vertices;
  vertices.reserve(numRows());
  valid_ = true;
  colIdx_ = -2;
  for (currentDs_ = dsIndices_.begin(); currentDs_ < dsIndices_.end(); ++currentDs_) {
    rowsUpperBound_ = currentDs_->ds->rows.end();
    for (currentRow_ = currentDs_->ds->rows.begin(); currentRow_ < currentDs_->ds->rows.end();
         ++currentRow_) {
      vertices.values.emplace_back(getVertex());
    }
  }
  reset();
  return vertices;
}

Value GetNeighborsIter::getEdge() const {
  if (!valid()) {
    return Value::kNullValue;
  }

  if (noEdge_) {
    return Value::kEmpty;
  }

  Edge edge;
  auto edgeName = currentEdgeName().substr(1, std::string::npos);
  edge.name = edgeName;

  auto type = getEdgeProp(edgeName, kType);
  if (type.isInt()) {
    edge.type = type.getInt();
  } else {
    edge.type = 0;
  }

  auto& srcVal = getColumn(0);
  if (!SchemaUtil::isValidVid(srcVal)) {
    return Value::kNullBadType;
  }
  edge.src = srcVal;

  auto& dstVal = getEdgeProp(edgeName, kDst);
  if (!SchemaUtil::isValidVid(dstVal)) {
    return Value::kNullBadType;
  }
  edge.dst = dstVal;

  auto& rank = getEdgeProp(edgeName, kRank);
  if (rank.isInt()) {
    edge.ranking = rank.getInt();
  } else {
    edge.ranking = 0;
  }

  auto& edgePropMap = currentDs_->edgePropsMap;
  auto edgeProp = edgePropMap.find(currentEdgeName());
  if (edgeProp == edgePropMap.end()) {
    return Value::kNullValue;
  }
  auto& edgeNamePropList = edgeProp->second.propList;
  auto& propList = currentEdge_->values;
  DCHECK_EQ(edgeNamePropList.size(), propList.size());
  for (size_t i = 0; i < propList.size(); ++i) {
    auto propName = edgeNamePropList[i];
    if (propName == kDst || propName == kRank || propName == kType || propName == kSrc) {
      continue;
    }
    edge.props.emplace(edgeNamePropList[i], propList[i]);
  }
  return Value(std::move(edge));
}

List GetNeighborsIter::getEdges() {
  List edges;
  edges.reserve(size());
  for (; valid(); next()) {
    auto edge = getEdge();
    if (edge.isEdge()) {
      const_cast<Edge&>(edge.getEdge()).format();
      edges.values.emplace_back(std::move(edge));
    }
  }
  reset();
  return edges;
}

void GetNeighborsIter::nextCol() {
  if (!valid()) {
    return;
  }
  DCHECK(!noEdge_);

  // go to next column
  while (++colIdx_) {
    if (colIdx_ < currentDs_->colUpperBound) {
      const auto& currentCol = currentRow_->operator[](colIdx_);
      if (!currentCol.isList() || currentCol.getList().empty()) {
        continue;
      }

      currentCol_ = &currentCol.getList();
      // edgeIdxUpperBound_ = currentCol_->size();
      // edgeIdx_ = -1;
      break;
    }
    // go to next row
    if (++currentRow_ < rowsUpperBound_) {
      colIdx_ = currentDs_->colLowerBound;
      continue;
    }

    // go to next dataset
    if (++currentDs_ < dsIndices_.end()) {
      colIdx_ = currentDs_->colLowerBound;
      currentRow_ = currentDs_->ds->begin();
      rowsUpperBound_ = currentDs_->ds->end();
      continue;
    }
    break;
  }
  if (currentDs_ == dsIndices_.end()) {
    return;
  }
}

void GetNeighborsIter::clearEdges() {
  for (; colValid(); nextCol()) {
    const_cast<List*>(currentCol_)->clear();
  }
}

}  // namespace graph
}  // namespace nebula
