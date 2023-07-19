/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_CONTEXT_ITERATOR_DEFAULTITER_H_
#define GRAPH_CONTEXT_ITERATOR_DEFAULTITER_H_

#include "graph/context/iterator/Iterator.h"

namespace nebula {
namespace graph {

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

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_CONTEXT_ITERATOR_DEFAULTITER_H_
