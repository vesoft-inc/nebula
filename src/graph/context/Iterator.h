/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_CONTEXT_ITERATOR_H_
#define GRAPH_CONTEXT_ITERATOR_H_

#include <gtest/gtest_prod.h>

#include <boost/dynamic_bitset.hpp>

#include "common/algorithm/ReservoirSampling.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/List.h"
#include "common/datatypes/Value.h"
#include "parser/TraverseSentences.h"

namespace nebula {
namespace graph {

class Iterator {
 public:
  template <typename T>
  using RowsType = std::vector<T>;
  template <typename T>
  using RowsIter = typename RowsType<T>::iterator;

  // Warning this will break the origin order of elements!
  template <typename T>
  static RowsIter<T> eraseBySwap(RowsType<T>& rows, RowsIter<T> i) {
    DCHECK(!rows.empty());
    std::swap(rows.back(), *i);
    rows.pop_back();
    return i;
  }

  enum class Kind : uint8_t {
    kDefault,
    kGetNeighbors,
    kSequential,
    kProp,
  };

  Iterator(std::shared_ptr<Value> value, Kind kind, bool checkMemory = false)
      : checkMemory_(checkMemory), kind_(kind), numRowsModN_(0), value_(value) {}

  virtual ~Iterator() = default;

  Kind kind() const {
    return kind_;
  }

  virtual std::unique_ptr<Iterator> copy() const = 0;

  virtual bool valid() const {
    return !hitsSysMemoryHighWatermark();
  }

  virtual void next() = 0;

  // erase current iter
  virtual void erase() = 0;

  // Warning this will break the origin order of elements!
  virtual void unstableErase() = 0;

  // remain the select data in range
  virtual void select(std::size_t offset, std::size_t count) = 0;

  // Sample the elements
  virtual void sample(int64_t count) = 0;

  virtual const Row* row() const = 0;

  virtual Row moveRow() = 0;

  // erase range, no include last position, if last > size(), erase to the end
  // position
  virtual void eraseRange(size_t first, size_t last) = 0;

  // Reset iterator position to `pos' from begin. Must be sure that the `pos'
  // position is lower than `size()' before resetting
  void reset(size_t pos = 0) {
    numRowsModN_ = 0;
    doReset(pos);
  }

  virtual void clear() = 0;

  void operator++() {
    next();
  }

  virtual std::shared_ptr<Value> valuePtr() const {
    return value_;
  }

  virtual size_t size() const = 0;

  bool empty() const {
    return size() == 0;
  }

  bool isDefaultIter() const {
    return kind_ == Kind::kDefault;
  }

  bool isGetNeighborsIter() const {
    return kind_ == Kind::kGetNeighbors;
  }

  bool isSequentialIter() const {
    return kind_ == Kind::kSequential;
  }

  bool isPropIter() const {
    return kind_ == Kind::kProp;
  }

  // The derived class should rewrite get prop if the Value is kind of dataset.
  virtual const Value& getColumn(const std::string& col) const = 0;

  virtual const Value& getColumn(int32_t index) const = 0;

  // Get index of the column in tuple
  virtual StatusOr<std::size_t> getColumnIndex(const std::string& col) const = 0;

  template <typename Iter>
  const Value& getColumnByIndex(int32_t index, Iter iter) const {
    int32_t size = static_cast<int32_t>(iter->size());
    if ((index > 0 && index >= size) || (index < 0 && -index > size)) {
      return Value::kNullBadType;
    }
    return iter->operator[]((size + index) % size);
  }

  virtual const Value& getTagProp(const std::string&, const std::string&) const {
    DLOG(FATAL) << "Shouldn't call the unimplemented method";
    return Value::kEmpty;
  }

  virtual const Value& getEdgeProp(const std::string&, const std::string&) const {
    DLOG(FATAL) << "Shouldn't call the unimplemented method";
    return Value::kEmpty;
  }

  virtual Value getVertex(const std::string& name = "") const {
    UNUSED(name);
    return Value();
  }

  virtual Value getEdge() const {
    return Value();
  }

  bool checkMemory() const {
    return checkMemory_;
  }
  void setCheckMemory(bool checkMemory) {
    checkMemory_ = checkMemory;
  }

 protected:
  virtual void doReset(size_t pos) = 0;
  bool hitsSysMemoryHighWatermark() const;

  bool checkMemory_{false};
  Kind kind_;
  mutable int64_t numRowsModN_{0};
  std::shared_ptr<Value> value_;
};

class DefaultIter final : public Iterator {
 public:
  explicit DefaultIter(std::shared_ptr<Value> value, bool checkMemory = false)
      : Iterator(value, Kind::kDefault, checkMemory) {}

  std::unique_ptr<Iterator> copy() const override {
    return std::make_unique<DefaultIter>(*this);
  }

  bool valid() const override {
    return Iterator::valid() && !(counter_ > 0);
  }

  void next() override {
    numRowsModN_++;
    counter_++;
  }

  void erase() override {
    counter_--;
  }

  void unstableErase() override {
    DLOG(ERROR) << "Unimplemented default iterator.";
    counter_--;
  }

  void eraseRange(size_t, size_t) override {
    return;
  }

  void select(std::size_t, std::size_t) override {
    DLOG(FATAL) << "Unimplemented method for default iterator.";
  }

  void sample(int64_t) override {
    DLOG(FATAL) << "Unimplemented default iterator.";
  }

  void clear() override {
    reset();
  }

  size_t size() const override {
    return 1;
  }

  const Value& getColumn(const std::string& /* col */) const override {
    DLOG(FATAL) << "This method should not be invoked";
    return Value::kEmpty;
  }

  const Value& getColumn(int32_t) const override {
    DLOG(FATAL) << "This method should not be invoked";
    return Value::kEmpty;
  }

  StatusOr<std::size_t> getColumnIndex(const std::string&) const override {
    DLOG(FATAL) << "This method should not be invoked";
    return Status::Error("Unimplemented method");
  }

  const Row* row() const override {
    DLOG(FATAL) << "This method should not be invoked";
    return nullptr;
  }

  Row moveRow() override {
    DLOG(FATAL) << "This method should not be invoked";
    return Row{};
  }

 private:
  void doReset(size_t pos) override {
    DCHECK((pos == 0 && size() == 0) || (pos < size()));
    counter_ = pos;
  }

  int64_t counter_{0};
};

class GetNeighborsIter final : public Iterator {
 public:
  explicit GetNeighborsIter(std::shared_ptr<Value> value, bool checkMemory = false);

  std::unique_ptr<Iterator> copy() const override {
    auto copy = std::make_unique<GetNeighborsIter>(*this);
    copy->reset();
    return copy;
  }

  bool valid() const override;

  void next() override;

  void clear() override {
    valid_ = false;
    dsIndices_.clear();
    reset();
  }

  void erase() override;

  void unstableErase() override {
    erase();
  }

  // erase [first, last)
  void eraseRange(size_t first, size_t last) override {
    for (std::size_t i = 0; valid() && i < last; ++i) {
      if (i >= first || i < last) {
        erase();
      } else {
        next();
      }
    }
    doReset(0);
  }

  void select(std::size_t offset, std::size_t count) override {
    for (std::size_t i = 0; valid(); ++i) {
      if (i < offset || i > (offset + count - 1)) {
        erase();
      } else {
        next();
      }
    }
    doReset(0);
  }

  void sample(int64_t count) override;

  // num of edges
  size_t size() const override;

  // num of vertices
  size_t numRows() const;

  const Value& getColumn(const std::string& col) const override;

  const Value& getColumn(int32_t index) const override;

  StatusOr<std::size_t> getColumnIndex(const std::string& col) const override;

  const Value& getTagProp(const std::string& tag, const std::string& prop) const override;

  const Value& getEdgeProp(const std::string& edge, const std::string& prop) const override;

  Value getVertex(const std::string& name = "") const override;

  Value getEdge() const override;

  // getVertices and getEdges arg batch interface use for subgraph
  // Its unique based on the plan
  List getVertices();

  // return start vids
  std::vector<Value> vids();

  // Its unique based on the GN interface dedup
  List getEdges();

  // only return currentEdge, not currentRow, for test
  const Row* row() const override {
    return currentEdge_;
  }

  Row moveRow() override {
    return std::move(*currentEdge_);
  }

 private:
  void doReset(size_t pos) override {
    UNUSED(pos);
    valid_ = false;
    bitIdx_ = -1;
    goToFirstEdge();
  }

  inline const std::string& currentEdgeName() const {
    DCHECK(currentDs_->tagEdgeNameIndices.find(colIdx_) != currentDs_->tagEdgeNameIndices.end());
    return currentDs_->tagEdgeNameIndices.find(colIdx_)->second;
  }

  bool colValid() {
    return !noEdge_ && valid();
  }

  // move to next List of Edge data
  void nextCol();

  void clearEdges();

  struct PropIndex {
    size_t colIdx;
    std::vector<std::string> propList;
    std::unordered_map<std::string, size_t> propIndices;
  };

  struct DataSetIndex {
    const DataSet* ds;
    // | _vid | _stats | _tag:t1:p1:p2 | _edge:e1:p1:p2 |
    // -> {_vid : 0, _stats : 1, _tag:t1:p1:p2 : 2, _edge:d1:p1:p2 : 3}
    std::unordered_map<std::string, size_t> colIndices;
    // | _vid | _stats | _tag:t1:p1:p2 | _edge:e1:p1:p2 |
    // -> {2 : t1, 3 : e1}
    std::unordered_map<size_t, std::string> tagEdgeNameIndices;
    // _tag:t1:p1:p2  ->  {t1 : [column_idx, [p1, p2], {p1 : 0, p2 : 1}]}
    std::unordered_map<std::string, PropIndex> tagPropsMap;
    // _edge:e1:p1:p2  ->  {e1 : [column_idx, [p1, p2], {p1 : 0, p2 : 1}]}
    std::unordered_map<std::string, PropIndex> edgePropsMap;

    int64_t colLowerBound{-1};
    int64_t colUpperBound{-1};
  };

  Status processList(std::shared_ptr<Value> value);

  void goToFirstEdge();

  StatusOr<int64_t> buildIndex(DataSetIndex* dsIndex);

  Status buildPropIndex(const std::string& props,
                        size_t columnId,
                        bool isEdge,
                        DataSetIndex* dsIndex);

  StatusOr<DataSetIndex> makeDataSetIndex(const DataSet& ds);

  FRIEND_TEST(IteratorTest, TestHead);

  bool valid_{false};
  std::vector<DataSetIndex> dsIndices_;

  std::vector<DataSetIndex>::iterator currentDs_;

  std::vector<Row>::const_iterator currentRow_;
  std::vector<Row>::const_iterator rowsUpperBound_;

  int64_t colIdx_{-1};
  const List* currentCol_{nullptr};

  bool noEdge_{false};
  int64_t edgeIdx_{-1};
  int64_t edgeIdxUpperBound_{-1};
  const List* currentEdge_{nullptr};

  boost::dynamic_bitset<> bitset_;
  int64_t bitIdx_{-1};
};

class SequentialIter : public Iterator {
 public:
  explicit SequentialIter(std::shared_ptr<Value> value, bool checkMemory = false);
  explicit SequentialIter(const SequentialIter& iter);

  // Union multiple sequential iterators
  explicit SequentialIter(std::vector<std::unique_ptr<Iterator>> inputList);
  // Union two sequential iterators.
  SequentialIter(std::unique_ptr<Iterator> left, std::unique_ptr<Iterator> right);

  std::unique_ptr<Iterator> copy() const override {
    auto copy = std::make_unique<SequentialIter>(*this);
    copy->reset();
    return copy;
  }

  bool valid() const override;

  void next() override;

  void erase() override;

  void unstableErase() override;

  void eraseRange(size_t first, size_t last) override;

  void select(std::size_t offset, std::size_t count) override {
    auto size = this->size();
    if (size <= static_cast<size_t>(offset)) {
      clear();
    } else if (size > static_cast<size_t>(offset + count)) {
      eraseRange(0, offset);
      eraseRange(count, size - offset);
    } else if (size > static_cast<size_t>(offset) && size <= static_cast<size_t>(offset + count)) {
      eraseRange(0, offset);
    }
  }

  void sample(int64_t count) override {
    DCHECK_GE(count, 0);
    algorithm::ReservoirSampling<Row> sampler(count);
    for (auto& row : *rows_) {
      sampler.sampling(std::move(row));
    }
    *rows_ = std::move(sampler).samples();
    iter_ = rows_->begin();
  }

  void clear() override {
    rows_->clear();
    reset();
  }

  std::vector<Row>::iterator begin() {
    return CHECK_NOTNULL(rows_)->begin();
  }

  std::vector<Row>::iterator end() {
    return CHECK_NOTNULL(rows_)->end();
  }

  const std::unordered_map<std::string, size_t>& getColIndices() const {
    return colIndices_;
  }

  size_t size() const override {
    return rows_->size();
  }

  const Value& getColumn(const std::string& col) const override;

  const Value& getColumn(int32_t index) const override;

  StatusOr<std::size_t> getColumnIndex(const std::string& col) const override;

  Value getVertex(const std::string& name = "") const override;

  Value getEdge() const override;

  Row moveRow() override {
    return std::move(*iter_);
  }

  const Row* row() const override {
    return &*iter_;
  }

 protected:
  // Notice: We only use this interface when return results to client.
  friend class DataCollectExecutor;
  friend class AppendVerticesExecutor;
  friend class TraverseExecutor;
  friend class ShortestPathExecutor;

  void doReset(size_t pos) override;

  std::vector<Row>::iterator iter_;
  std::vector<Row>* rows_{nullptr};

 private:
  void init(std::vector<std::unique_ptr<Iterator>>&& iterators);

  std::unordered_map<std::string, size_t> colIndices_;
};

class PropIter final : public SequentialIter {
 public:
  explicit PropIter(std::shared_ptr<Value> value, bool checkMemory = false);
  explicit PropIter(const PropIter& iter);

  std::unique_ptr<Iterator> copy() const override {
    auto copy = std::make_unique<PropIter>(*this);
    copy->reset();
    return copy;
  }

  const std::unordered_map<std::string, size_t>& getColIndices() const {
    return dsIndex_.colIndices;
  }

  const Value& getColumn(const std::string& col) const override;

  const Value& getColumn(int32_t index) const override;

  StatusOr<std::size_t> getColumnIndex(const std::string& col) const override;

  Value getVertex(const std::string& name = "") const override;

  Value getEdge() const override;

  List getVertices();

  List getEdges();

  const Value& getProp(const std::string& name, const std::string& prop) const;

  const Value& getTagProp(const std::string& tag, const std::string& prop) const override {
    return getProp(tag, prop);
  }

  const Value& getEdgeProp(const std::string& edge, const std::string& prop) const override {
    return getProp(edge, prop);
  }

 private:
  Status makeDataSetIndex(const DataSet& ds);

  Status buildPropIndex(const std::string& props, size_t columnIdx);

  struct DataSetIndex {
    const DataSet* ds;
    // vertex | _vid | tag1.prop1 | tag1.prop2 | tag2,prop1 | tag2,prop2 | ...
    //        |_vid : 0 | tag1.prop1 : 1 | tag1.prop2 : 2 | tag2.prop1 : 3 |...
    // edge   |_src | _type| _ranking | _dst | edge1.prop1 | edge1.prop2 |...
    //        |_src : 0 | _type : 1| _ranking : 2 | _dst : 3| edge1.prop1 :
    //        4|...
    std::unordered_map<std::string, size_t> colIndices;
    // {tag1 : {prop1 : 1, prop2 : 2}
    // {edge1 : {prop1 : 4, prop2 : 5}
    std::unordered_map<std::string, std::unordered_map<std::string, size_t>> propsMap;
  };

 private:
  DataSetIndex dsIndex_;
};

std::ostream& operator<<(std::ostream& os, Iterator::Kind kind);
}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_CONTEXT_ITERATOR_H_
