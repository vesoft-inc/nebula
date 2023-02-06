/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/context/iterator/GetNbrsRespDataSetIter.h"

#include "common/base/Base.h"
#include "common/datatypes/Edge.h"
#include "common/datatypes/Vertex.h"

namespace nebula {
namespace graph {

bool isDataSetInvalid(const DataSet* dataset) {
  const auto& colNames = dataset->colNames;
  return colNames.size() < 3 ||          // the dataset has vid/_tag/_edge 3 columns at least
         colNames[0] != nebula::kVid ||  // the first column should be Vid
         colNames[1].find(kStatsPrefix) != 0 ||  // the second column could not be _stats column now
         colNames.back().find(kExprPrefix) != 0;  // the last column could  not be _expr column now
}

GetNbrsRespDataSetIter::GetNbrsRespDataSetIter(const DataSet* dataset)
    : dataset_(DCHECK_NOTNULL(dataset)), curRowIdx_(0) {
  DCHECK(!isDataSetInvalid(dataset));
  for (size_t i = 0, e = dataset->colNames.size(); i < e; ++i) {
    buildPropIndex(dataset->colNames[i], i);
  }
}

void GetNbrsRespDataSetIter::buildPropIndex(const std::string& colName, size_t colIdx) {
  std::vector<std::string> pieces;
  folly::split(":", colName, pieces);
  if (pieces.size() < 2) {
    // Skip the column which is not tag or edge
    return;
  }

  PropIndex propIdx;
  propIdx.colIdx = colIdx;

  propIdx.propIdxMap.reserve(pieces.size() - 2);
  // if size == 2, it is the tag defined without props.
  for (size_t i = 2; i < pieces.size(); ++i) {
    const auto& name = pieces[i];
    size_t idx = i - 2;
    if (name == kType) {
      propIdx.edgeTypeIdx = idx;
    } else if (name == kRank) {
      propIdx.edgeRankIdx = idx;
    } else if (name == kDst) {
      propIdx.edgeDstIdx = idx;
    } else if (name == kSrc) {
      // Always skip the src of edge since it is returned by the first column
    } else if (name == kTag) {
      // Skip the _tag prop of vertex since it is not used to create vertex
    } else {
      propIdx.propIdxMap.emplace(name, idx);
    }
  }

  folly::StringPiece prefix(pieces[0]), name(pieces[1]);
  DCHECK(!name.empty()) << "The name of tag/edge is empty";
  if (prefix.startsWith(kEdgePrefix)) {
    DCHECK(name.startsWith("-") || name.startsWith("+")) << "the edge name has to start with '-/+'";
    edgePropsMap_.emplace(name, std::move(propIdx));
  } else if (prefix.startsWith(kTag)) {
    tagPropsMap_.emplace(name, std::move(propIdx));
  }
}

Value GetNbrsRespDataSetIter::getVertex() const {
  // Always check the valid() before getVertex
  DCHECK(valid());
  const Row& curRow = dataset_->rows[curRowIdx_];
  Vertex vertex;
  vertex.vid = curRow[0];
  vertex.tags.reserve(tagPropsMap_.size());
  for (const auto& [tagName, propIdx] : tagPropsMap_) {
    DCHECK_LT(propIdx.colIdx, curRow.size());
    const Value& propColumn = curRow[propIdx.colIdx];
    if (propColumn.isList()) {
      const List& propList = propColumn.getList();

      Tag tag(tagName);
      tag.props.reserve(propIdx.propIdxMap.size());
      for (const auto& [propName, pIdx] : propIdx.propIdxMap) {
        DCHECK_LT(pIdx, propList.size());
        tag.props.emplace(propName, propList[pIdx]);
      }

      vertex.tags.emplace_back(std::move(tag));
    }
  }
  return vertex;
}

Value GetNbrsRespDataSetIter::createEdgeByPropList(const PropIndex& propIdx,
                                                   const Value& edgeVal,
                                                   const Value& src,
                                                   const std::string& edgeName) const {
  if (!edgeVal.isList() || edgeVal.getList().empty()) {
    return Value::kEmpty;
  }
  const List& propList = edgeVal.getList();
  DCHECK_LT(propIdx.edgeDstIdx, propList.size());
  DCHECK_LT(propIdx.edgeTypeIdx, propList.size());
  DCHECK_LT(propIdx.edgeRankIdx, propList.size());

  Edge edge;
  edge.name = edgeName.substr(1);  // skip the edge direction symbol: `-/+`
  edge.src = src;
  edge.dst = propList[propIdx.edgeDstIdx];
  const Value& typeVal = propList[propIdx.edgeTypeIdx];
  edge.type = typeVal.isInt() ? typeVal.getInt() : 0;
  const Value& rankVal = propList[propIdx.edgeRankIdx];
  edge.ranking = rankVal.isInt() ? rankVal.getInt() : 0;

  edge.props.reserve(propIdx.propIdxMap.size());
  for (const auto& [propName, pIdx] : propIdx.propIdxMap) {
    DCHECK_LT(pIdx, propList.size());
    edge.props.emplace(propName, propList[pIdx]);
  }

  return edge;
}

std::vector<Value> GetNbrsRespDataSetIter::getAdjEdges(VidHashSet* dstSet) const {
  DCHECK(valid());

  std::vector<Value> adjEdges;
  const Row& curRow = dataset_->rows[curRowIdx_];
  for (const auto& [edgeName, propIdx] : edgePropsMap_) {
    DCHECK_LT(propIdx.colIdx, curRow.size());
    const Value& edgeColumn = curRow[propIdx.colIdx];
    if (edgeColumn.isList()) {
      for (const Value& edgeVal : edgeColumn.getList().values) {
        Value edge = createEdgeByPropList(propIdx, edgeVal, curRow[0], edgeName);
        if (!edge.empty()) {
          if (dstSet) {
            dstSet->emplace(edge.getEdge().dst);
          }
          adjEdges.emplace_back(std::move(edge));
        }
      }
    }
  }
  return adjEdges;
}

}  // namespace graph
}  // namespace nebula
