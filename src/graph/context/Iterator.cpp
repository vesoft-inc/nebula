/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/context/Iterator.h"

#include <robin_hood.h>

#include "common/datatypes/Edge.h"
#include "common/datatypes/Vertex.h"
#include "common/memory/MemoryUtils.h"
#include "graph/util/SchemaUtil.h"
#include "interface/gen-cpp2/common_types.h"

DECLARE_int32(num_rows_to_check_memory);
DECLARE_double(system_memory_high_watermark_ratio);

namespace nebula {
namespace graph {

bool Iterator::hitsSysMemoryHighWatermark() const {
  if (checkMemory_) {
    if (numRowsModN_ >= FLAGS_num_rows_to_check_memory) {
      numRowsModN_ -= FLAGS_num_rows_to_check_memory;
    }
    if (UNLIKELY(numRowsModN_ == 0)) {
      if (memory::MemoryUtils::kHitMemoryHighWatermark.load()) {
        throw std::runtime_error(
            folly::sformat("Used memory hits the high watermark({}) of total system memory.",
                           FLAGS_system_memory_high_watermark_ratio));
      }
    }
  }
  return false;
}

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

SequentialIter::SequentialIter(const SequentialIter& iter)
    : Iterator(iter.valuePtr(), Kind::kSequential) {
  auto valuePtr = iter.valuePtr();
  auto& ds = valuePtr->mutableDataSet();
  iter_ = ds.rows.begin();
  rows_ = &ds.rows;
  colIndices_ = iter.getColIndices();
}

SequentialIter::SequentialIter(std::shared_ptr<Value> value, bool checkMemory)
    : Iterator(value, Kind::kSequential, checkMemory) {
  DCHECK(value->isDataSet());
  auto& ds = value->mutableDataSet();
  iter_ = ds.rows.begin();
  rows_ = &ds.rows;
  for (size_t i = 0; i < ds.colNames.size(); ++i) {
    colIndices_.emplace(ds.colNames[i], i);
  }
}

SequentialIter::SequentialIter(std::unique_ptr<Iterator> left, std::unique_ptr<Iterator> right)
    : Iterator(left->valuePtr(), Kind::kSequential) {
  std::vector<std::unique_ptr<Iterator>> iterators;
  iterators.emplace_back(std::move(left));
  iterators.emplace_back(std::move(right));
  init(std::move(iterators));
}

SequentialIter::SequentialIter(std::vector<std::unique_ptr<Iterator>> inputList)
    : Iterator(inputList.front()->valuePtr(), Kind::kSequential, inputList.front()->checkMemory()) {
  init(std::move(inputList));
}

void SequentialIter::init(std::vector<std::unique_ptr<Iterator>>&& iterators) {
  DCHECK(!iterators.empty());
  const auto& firstIter = iterators.front();
  DCHECK(firstIter->isSequentialIter());
  colIndices_ = static_cast<const SequentialIter*>(firstIter.get())->getColIndices();
  DataSet ds;
  for (auto& iter : iterators) {
    DCHECK(iter->isSequentialIter());
    auto inputIter = static_cast<SequentialIter*>(iter.get());
    ds.rows.insert(ds.rows.end(),
                   std::make_move_iterator(inputIter->begin()),
                   std::make_move_iterator(inputIter->end()));
  }
  value_ = std::make_shared<Value>(std::move(ds));
  rows_ = &value_->mutableDataSet().rows;
  iter_ = rows_->begin();
}

bool SequentialIter::valid() const {
  return Iterator::valid() && iter_ < rows_->end();
}

void SequentialIter::next() {
  if (valid()) {
    ++numRowsModN_;
    ++iter_;
  }
}

void SequentialIter::erase() {
  iter_ = rows_->erase(iter_);
}

void SequentialIter::unstableErase() {
  std::swap(rows_->back(), *iter_);
  rows_->pop_back();
}

void SequentialIter::eraseRange(size_t first, size_t last) {
  if (first >= last || first >= size()) {
    return;
  }
  if (last > size()) {
    rows_->erase(rows_->begin() + first, rows_->end());
  } else {
    rows_->erase(rows_->begin() + first, rows_->begin() + last);
  }
  reset();
}

void SequentialIter::doReset(size_t pos) {
  DCHECK((pos == 0 && size() == 0) || (pos < size()));
  iter_ = rows_->begin() + pos;
}

const Value& SequentialIter::getColumn(const std::string& col) const {
  if (!valid()) {
    return Value::kNullValue;
  }
  auto& row = *iter_;
  auto index = colIndices_.find(col);
  if (index == colIndices_.end()) {
    return Value::kNullValue;
  }

  DCHECK_LT(index->second, row.values.size()) << "index: " << index->second << " row" << row;
  return row.values[index->second];
}

const Value& SequentialIter::getColumn(int32_t index) const {
  return getColumnByIndex(index, iter_);
}

StatusOr<std::size_t> SequentialIter::getColumnIndex(const std::string& col) const {
  auto index = colIndices_.find(col);
  if (index == colIndices_.end()) {
    return Status::Error("Don't exist column `%s'.", col.c_str());
  }
  return index->second;
}

Value SequentialIter::getVertex(const std::string& name) {
  return getColumn(name);
}

Value SequentialIter::getEdge() const {
  return getColumn("EDGE");
}

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

std::ostream& operator<<(std::ostream& os, Iterator::Kind kind) {
  switch (kind) {
    case Iterator::Kind::kDefault:
      os << "default";
      break;
    case Iterator::Kind::kSequential:
      os << "sequential";
      break;
    case Iterator::Kind::kGetNeighbors:
      os << "get neighbors";
      break;
    case Iterator::Kind::kProp:
      os << "Prop";
      break;
  }
  os << " iterator";
  return os;
}

}  // namespace graph
}  // namespace nebula
