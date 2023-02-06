/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_CONTEXT_ITERATOR_GETNEIGHBORSITER_H_
#define GRAPH_CONTEXT_ITERATOR_GETNEIGHBORSITER_H_

#include <gtest/gtest_prod.h>

#include <boost/dynamic_bitset.hpp>

#include "graph/context/iterator/Iterator.h"

namespace nebula {
namespace graph {

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

  Value getVertex(const std::string& name = "") override;

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

  // The current edge name has the direction symbol indicated by `-/+`, such as `-like`
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
  Value prevVertex_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_CONTEXT_ITERATOR_GETNEIGHBORSITER_H_
