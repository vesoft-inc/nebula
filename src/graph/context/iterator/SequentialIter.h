/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_CONTEXT_ITERATOR_SEQUENTIALITER_H_
#define GRAPH_CONTEXT_ITERATOR_SEQUENTIALITER_H_

#include "graph/context/iterator/Iterator.h"

namespace nebula {
namespace graph {

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

  void sample(int64_t count) override;

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

  const Value& getTagProp(const std::string& tag, const std::string& prop) const override;

  const Value& getEdgeProp(const std::string& edge, const std::string& prop) const override;

  Value getVertex(const std::string& name = "") override;

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

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_CONTEXT_ITERATOR_SEQUENTIALITER_H_
