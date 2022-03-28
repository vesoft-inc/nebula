/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/algo/BFSShortestPathExecutor.h"

#include "graph/planner/plan/Algo.h"

namespace nebula {
namespace graph {
folly::Future<Status> BFSShortestPathExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* path = asNode<BFSShortestPath>(node());
  auto leftIter = ectx_->getResult(path->leftInputVar()).iter();
  auto rightIter = ectx_->getResult(path->rightInputVar()).iter();
  DCHECK(!!leftIter && !!rightIter);
  leftVidVar_ = path->leftVidVar();
  rightVidVar_ = path->rightVidVar();
  steps_ = path->steps();

  DataSet ds;
  ds.colNames = path->colNames();
  buildPath(leftIter.get(), false);
  buildPath(rightIter.get(), true);
  ds.rows = conjunctPath();
  step_++;
  return finish(ResultBuilder().value(Value(std::move(ds))).build());
}

void BFSShortestPathExecutor::buildPath(Iterator* iter, bool reverse) {
  auto& visitedVids = reverse ? rightVisitedVids_ : leftVisitedVids_;
  auto& allEdges = reverse ? allRightEdges_ : allLeftEdges_;
  allEdges.emplace_back();
  auto& currentEdges = allEdges.back();

  auto iterSize = iter->size();
  visitedVids.reserve(visitedVids.size() + iterSize);

  std::unordered_set<Value> uniqueDst;
  uniqueDst.reserve(iterSize);
  DataSet nextStepVids;
  nextStepVids.colNames = {nebula::kVid};
  if (step_ == 1) {
    if (reverse) {
      auto vid = iter->getColumn(0);
      Edge dumpy;
      currentEdges.emplace(std::move(vid), std::move(dumpy));
      allEdges.emplace_back();
      currentEdges = allEdges.back();
    }

    for (; iter->valid(); iter->next()) {
      auto edgeVal = iter->getEdge();
      if (UNLIKELY(!edgeVal.isEdge())) {
        continue;
      }
      auto& edge = edgeVal.getEdge();
      auto dst = edge.dst;
      visitedVids.emplace(edge.src);
      if (uniqueDst.emplace(dst).second) {
        nextStepVids.rows.emplace_back(Row({dst}));
      }
      currentEdges.emplace(std::move(dst), std::move(edge));
    }
  } else {
    for (; iter->valid(); iter->next()) {
      auto edgeVal = iter->getEdge();
      if (UNLIKELY(!edgeVal.isEdge())) {
        continue;
      }
      auto& edge = edgeVal.getEdge();
      auto dst = edge.dst;
      if (visitedVids.find(dst) != visitedVids.end()) {
        continue;
      }
      if (uniqueDst.emplace(dst).second) {
        nextStepVids.rows.emplace_back(Row({dst}));
      }
      currentEdges.emplace(std::move(dst), std::move(edge));
    }
  }
  // set nextVid
  auto& nextVidVar = reverse ? rightVidVar_ : leftVidVar_;
  ectx_->setResult(nextVidVar, ResultBuilder().value(std::move(nextStepVids)).build());

  visitedVids.insert(std::make_move_iterator(uniqueDst.begin()),
                     std::make_move_iterator(uniqueDst.end()));
}

std::vector<Row> BFSShortestPathExecutor::conjunctPath() {
  const auto& leftEdges = allLeftEdges_.back();
  const auto& preRightEdges = allRightEdges_[step_ - 1];
  std::vector<Value> meetVids;
  for (const auto& edge : leftEdges) {
    if (preRightEdges.find(edge.first) != preRightEdges.end()) {
      meetVids.push_back(edge.first);
    }
  }
  if (meetVids.empty() && step_ * 2 <= steps_) {
    const auto& rightEdges = allRightEdges_.back();
    for (const auto& edge : rightEdges) {
      if (rightEdges.find(edge.first) != rightEdges.end()) {
        meetVids.push_back(edge.first);
      }
    }
  }
  std::vector<Row> rows;
  if (!meetVids.empty()) {
    auto leftPaths = createPath(meetVids, false);
    auto rightPaths = createPath(meetVids, true);
    for (auto& leftPath : leftPaths) {
      auto range = rightPaths.equal_range(leftPath.first);
      for (auto& rightPath = range.first; rightPath != range.second; ++rightPath) {
        Path result = leftPath.second;
        result.reverse();
        result.append(rightPath->second);
        Row row;
        row.emplace_back(std::move(result));
        rows.emplace_back(std::move(row));
      }
    }
  }
  return rows;
}

std::unordered_multimap<Value, Path> BFSShortestPathExecutor::createPath(
    std::vector<Value> meetVids, bool reverse) {
  std::unordered_multimap<Value, Path> result;
  auto& allEdges = reverse ? allRightEdges_ : allLeftEdges_;
  for (auto& meetVid : meetVids) {
    Path start;
    start.src = Vertex(meetVid, {});
    std::vector<Path> interimPaths = {std::move(start)};
    for (auto iter = allEdges.rbegin(); iter != allEdges.rend(); ++iter) {
      std::vector<Path> temp;
      for (auto& interimPath : interimPaths) {
        Value id;
        if (interimPath.steps.empty()) {
          id = interimPath.src.vid;
        } else {
          id = interimPath.steps.back().dst.vid;
        }
        auto range = iter->equal_range(id);
        for (auto edgeIter = range.first; edgeIter != range.second; ++edgeIter) {
          Path p = interimPath;
          auto& edge = edgeIter->second;
          p.steps.emplace_back(Step(Vertex(edge.src, {}), -edge.type, edge.name, edge.ranking, {}));

          if (iter == allEdges.rend() - 1) {
            result.emplace(p.src.vid, std::move(p));
          } else {
            temp.emplace_back(std::move(p));
          }
        }  //  edgeIter
      }    // interimPath
      interimPaths = std::move(temp);
    }
  }
  return result;
}

}  // namespace graph
}  // namespace nebula
