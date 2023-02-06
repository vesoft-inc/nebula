/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_CONTEXT_ITERATOR_ITERATOR_H_
#define GRAPH_CONTEXT_ITERATOR_ITERATOR_H_

#include "common/base/StatusOr.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Value.h"

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

  virtual Value getVertex(const std::string& name = "") {
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

std::ostream& operator<<(std::ostream& os, Iterator::Kind kind);

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_CONTEXT_ITERATOR_ITERATOR_H_
