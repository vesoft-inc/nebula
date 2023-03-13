/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_CONTEXT_ITERATOR_GETNBRSRESPDATASETITER_H_
#define GRAPH_CONTEXT_ITERATOR_GETNBRSRESPDATASETITER_H_

#include "common/datatypes/DataSet.h"
#include "common/datatypes/Value.h"

namespace nebula {
namespace graph {

class GetNbrsRespDataSetIter final {
 public:
  explicit GetNbrsRespDataSetIter(const DataSet* dataset);

  bool valid() const {
    return curRowIdx_ < dataset_->rowSize();
  }
  // Next row in dataset
  void next() {
    curRowIdx_++;
  }

  Value getVertex() const;
  std::vector<Value> getAdjEdges(VidHashSet* dstSet) const;

  std::unordered_set<Value> getAdjDsts() const;

  Value getVid() const;

  size_t size();

 private:
  struct PropIndex {
    size_t colIdx;
    size_t edgeTypeIdx;
    size_t edgeRankIdx;
    size_t edgeDstIdx;
    std::unordered_map<std::string, size_t> propIdxMap;
  };

  void buildPropIndex(const std::string& colName, size_t colIdx);
  Value createEdgeByPropList(const PropIndex& propIdx,
                             const Value& edgeVal,
                             const Value& src,
                             const std::string& edgeName) const;

  const DataSet* dataset_;
  size_t curRowIdx_;

  // _tag:t1:p1:p2  ->  {t1 : [column_idx, [p1, p2], {p1 : 0, p2 : 1}]}
  std::unordered_map<std::string, PropIndex> tagPropsMap_;
  // _edge:e1:p1:p2  ->  {e1 : [column_idx, [p1, p2], {p1 : 0, p2 : 1}]}
  std::unordered_map<std::string, PropIndex> edgePropsMap_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_CONTEXT_ITERATOR_GETNBRSRESPDATASETITER_H_
