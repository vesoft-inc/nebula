/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_CONTEXT_ITERATOR_PROPITER_H_
#define GRAPH_CONTEXT_ITERATOR_PROPITER_H_

#include "graph/context/iterator/SequentialIter.h"

namespace nebula {
namespace graph {

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

  Value getVertex(const std::string& name = "") override;

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

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_CONTEXT_ITERATOR_PROPITER_H_
