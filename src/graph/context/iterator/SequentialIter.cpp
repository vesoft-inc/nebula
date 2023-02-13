/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/context/iterator/SequentialIter.h"

#include "common/algorithm/ReservoirSampling.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/Vertex.h"

namespace nebula {
namespace graph {

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

void SequentialIter::sample(int64_t count) {
  DCHECK_GE(count, 0);
  algorithm::ReservoirSampling<Row> sampler(count);
  for (auto& row : *rows_) {
    sampler.sampling(std::move(row));
  }
  *rows_ = std::move(sampler).samples();
  iter_ = rows_->begin();
}

}  // namespace graph
}  // namespace nebula
