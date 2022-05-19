// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/SingleShortestPath.h"

#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"
#include "sys/sysinfo.h"

using nebula::storage::StorageClient;

DEFINE_uint32(num_path_thread, 0, "number of concurrent threads when do shortest path");
namespace nebula {
namespace graph {
folly::Future<Status> SingleShortestPath::execute() {}

void SingleShortestPath::singleShortestPathInit() {
  size_t rowSize = cartesianProduct_.size();
  leftVids_.reserve(rowSize);
  rightVids_.reserve(rowSize);

  leftVisitedVids_.resize(rowSize);
  rightVisitedVids_.resize(rowSize);

  allLeftPaths_.resize(rowSize);
  allRightPaths_.reserve(rowSize);
  resultDs_.resize(rowSize);
  for (const auto& iter : cartesianProduct_) {
    std::unordered_map<Value, std::vector<Row>> steps;
    std::vector<Row> dummy;
    steps.emplace(iter.second, std::move(dummy));
    HalfPath originRightPath({std::move(steps)});
    allRightPaths_.emplace_back(std::move(originRightPath));

    DataSet startDs, endDs;
    startDs.rows.emplace_back(Row({iter.first}));
    endDs.rows.emplace_back(Row({iter.second}));
    leftVids_.emplace_back(std::move(startDs));
    rightVids_.emplace_back(std::move(endDs));
  }
}

folly::Future<Status> SingleShortestPath::singleShortestPath() {
  singleShortestPathInit();
  size_t rowSize = cartesianProduct_.size();
  std::vector<folly::Future<Status>> futures;
  futures.reserve(rowSize);
  for (size_t rowNum = 0; rowNum < rowSize; ++rowNum) {
    resultDs_[i].colNames = pathNode_->colNames();
    futures.emplace_back(shortestPath(rowNum, 1));
  }
  return folly::collect(futures).via(runner()).thenValue([](auto&& resps) {
    for (auto& resp : resps) {
      NG_RETURN_IF_ERROR(resp);
    }
    DataSet result;
    result.colNames = pathNode_->colNames();
    for (auto& ds : resultDs_) {
      result.append(std::move(ds));
    }
    return finish(ResultBuilder().value(Value(std::move(result))).build());
  });
}

bool SingleShortestPath::conjunctPath(size_t rowNum, size_t stepNum) {
  const auto& leftStep = allLeftPaths_[rowNum].back();
  const auto& prevRightStep = allRightPaths_[rowNum][stepNum - 1];
  std::vector<Value> meetVids;
  for (const auto& step : leftStep) {
    if (prevRightStep.find(step.first) != prevRightStep.end()) {
      meetVids.push_back(step.first);
      if (singleShortest_) {
        break;
      }
    }
  }
  if (!meetVids.empty()) {
    buildOddPath(rowNum, meetVids);
    return true;
  }
  if (stepNum * 2 >= maxStep_) {
    return false;
  }

  const auto& rightStep = allRightPaths_[rowNum].back();
  for (const auto& step : leftStep) {
    if (rightStep.find(step.first) != rightStep.end()) {
      meetVids.push_back(step.first);
      if (singleShortest_) {
        break;
      }
    }
  }
  if (meetVids.empty()) {
    return false;
  }
  return buildEvenPath(rowNum, meetVids);
}

Status SingleShortestPath::buildPath(size_t rowNum, RpcResponse&& resps, bool reverse) {
  auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
  NG_RETURN_IF_ERROR(result);
  auto& responses = std::move(resps).responses();
  List list;
  for (auto& resp : responses) {
    auto dataset = resp.get_vertices();
    if (dataset == nullptr) {
      LOG(INFO) << "Empty dataset in response";
      continue;
    }
    list.values.emplace_back(std::move(*dataset));
  }
  auto listVal = std::make_shared<Value>(std::move(list));
  auto iter = std::make_unique<GetNeighborsIter>(listVal);
  if (singlePair_) {
    return buildSinglePairPath(rowNum, iter.get(), reverse);
  }
  return buildMultiPairPath(rowNum, iter.get(), reverse);
}

Status SingleShortestPath::buildSinglePairPath(size_t rowNum,
                                               GetNeighborsIter* iter,
                                               bool reverse) {
  auto iterSize = iter->size();
  auto& visitedVids = reverse ? rightVisitedVids_[rowNum] : leftVisitedVids_[rowNum];
  visitedVids.reserve(visitedVids.size() + iterSize);
  auto& allSteps = reverse ? allRightPaths_[rowNum] : allLeftPaths_[rowNum];
  allSteps.emplace_back();
  auto& currentStep = allSteps.back();

  std::unordered_set<Value> uniqueDst;
  uniqueDst.reserve(iterSize);
  std::vector<Row> nextStepVids;
  nextStepVids.reserve(iterSize);

  QueryExpressionContext ctx(ectx_);
  for (iter->reset(); iter->valid(); iter->next()) {
    auto edgeVal = iter->getEdge();
    if (UNLIKELY(!edgeVal.isEdge())) {
      continue;
    }
    auto& edge = edgeVal.getEdge();
    auto dst = edge.dst;
    if (visitedVids.find(dst) != visitedVids.end()) {
      continue;
    }
    visitedVids.emplace(edge.src);
    if (uniqueDst.emplace(dst).second) {
      nextStepVids.emplace_back(Row({dst}));
    }
    auto vertex = iter->getVertex();
    Row step;
    step.emplace_back(std::move(vertex));
    step.emplace_back(std::move(edge));
    auto findDst = currentStep.find(dst);
    if (findDst == currentStep.end()) {
      std::vector<Row> steps({std::move(step)});
      currentStep.emplace(std::move(dst), std::move(steps));
    } else {
      auto& steps = findDst->second;
      steps.emplace_back(std::move(step));
    }
  }
  visitedVids.insert(std::make_move_iterator(uniqueDst.begin()),
                     std::make_move_iterator(uniqueDst.end()));
  if (reverse) {
    rightVids_[rowNum].rows.swap(nextStepVids);
  } else {
    leftVids_[rowNum].rows.swap(nextStepVids);
  }
  return Status::OK();
}

void SingleShortestPath::buildOddPath(size_t rowNum, const std::vector<Value>& meetVids) {
  for (auto& meetVid : meetVids) {
    auto leftPaths = createLeftPath(rowNum, meetVid);
    auto rightPaths = createRightPath(rowNum, meetVid, true);
    for (auto& leftPath : leftPaths) {
      for (auto& rightPath : rightPaths) {
        Row path = leftPath;
        auto& steps = path.values.back().mutableList().values;
        steps.insert(steps.end(), rightPath.values.begin(), rightPath.values.end() - 1);
        path.emplace_back(rightPath.values.back());
        resultDs_[rowNum].rows.emplace_back(std::move(path));
        if (singleShortest_) {
          return;
        }
      }
    }
  }
}

bool SingleShortestPath::buildEvenPath(size_t rowNum, const std::vector<Value>& meetVids) {
  std::vector<Value> meetVertices;
  auto status = getMeetVidsProps(meetVids, meetVertices).get();
  if (!status.ok() || meetVertices.empty()) {
    return false;
  }
  for (auto& meetVertex : meetVertices) {
    auto meetVid = meetVertex.getVertex().vid;
    auto leftPaths = createLeftPath(rowNum, meetVid);
    auto rightPaths = createRightPath(rowNum, meetVid, false);
    for (auto& leftPath : leftPaths) {
      for (auto& rightPath : rightPaths) {
        Row path = leftPath;
        auto& steps = path.values.back().mutableList().values;
        steps.emplace_back(meetVertex);
        steps.insert(steps.end(), rightPath.values.begin(), rightPath.values.end() - 1);
        path.emplace_back(rightPath.values.back());
        resultDs_[rowNum].rows.emplace_back(std::move(path));
        if (singleShortest_) {
          return true;
        }
      }
    }
  }
  return true;
}

}  // namespace graph
}  // namespace nebula
