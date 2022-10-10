// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/SingleShortestPath.h"

#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"

using nebula::storage::StorageClient;

namespace nebula {
namespace graph {
folly::Future<Status> SingleShortestPath::execute(const HashSet& startVids,
                                                  const HashSet& endVids,
                                                  DataSet* result) {
  size_t rowSize = startVids.size() * endVids.size();
  init(startVids, endVids, rowSize);
  std::vector<folly::Future<Status>> futures;
  futures.reserve(rowSize);
  for (size_t rowNum = 0; rowNum < rowSize; ++rowNum) {
    resultDs_[rowNum].colNames = pathNode_->colNames();
    futures.emplace_back(shortestPath(rowNum, 1));
  }
  return folly::collect(futures)
      .via(qctx_->rctx()->runner())
      .thenValue([this, result](auto&& resps) {
        for (auto& resp : resps) {
          NG_RETURN_IF_ERROR(resp);
        }
        result->colNames = pathNode_->colNames();
        for (auto& ds : resultDs_) {
          result->append(std::move(ds));
        }
        return Status::OK();
      });
}

void SingleShortestPath::init(const HashSet& startVids, const HashSet& endVids, size_t rowSize) {
  leftVids_.reserve(rowSize);
  rightVids_.reserve(rowSize);

  leftVisitedVids_.resize(rowSize);
  rightVisitedVids_.resize(rowSize);

  allLeftPaths_.resize(rowSize);
  allRightPaths_.reserve(rowSize);
  resultDs_.resize(rowSize);
  for (const auto& startVid : startVids) {
    HashSet srcVid({startVid});
    for (const auto& endVid : endVids) {
      robin_hood::unordered_flat_map<Value, std::vector<Row>, std::hash<Value>> steps;
      std::vector<Row> dummy;
      steps.emplace(endVid, std::move(dummy));
      HalfPath originRightPath({std::move(steps)});
      allRightPaths_.emplace_back(std::move(originRightPath));

      HashSet dstVid({endVid});
      leftVids_.emplace_back(srcVid);
      rightVids_.emplace_back(std::move(dstVid));
    }
  }
}

folly::Future<Status> SingleShortestPath::shortestPath(size_t rowNum, size_t stepNum) {
  std::vector<folly::Future<Status>> futures;
  futures.reserve(2);
  futures.emplace_back(getNeighbors(rowNum, stepNum, false));
  futures.emplace_back(getNeighbors(rowNum, stepNum, true));
  return folly::collect(futures)
      .via(qctx_->rctx()->runner())
      .thenValue([this, rowNum, stepNum](auto&& resps) {
        for (auto& resp : resps) {
          if (!resp.ok()) {
            return folly::makeFuture<Status>(std::move(resp));
          }
        }
        return handleResponse(rowNum, stepNum);
      });
}

folly::Future<Status> SingleShortestPath::getNeighbors(size_t rowNum,
                                                       size_t stepNum,
                                                       bool reverse) {
  StorageClient* storageClient = qctx_->getStorageClient();
  time::Duration getNbrTime;
  storage::StorageClient::CommonRequestParam param(pathNode_->space(),
                                                   qctx_->rctx()->session()->id(),
                                                   qctx_->plan()->id(),
                                                   qctx_->plan()->isProfileEnabled());
  auto& inputVids = reverse ? rightVids_[rowNum] : leftVids_[rowNum];
  std::vector<Value> vids(inputVids.begin(), inputVids.end());
  inputVids.clear();
  return storageClient
      ->getNeighbors(param,
                     {nebula::kVid},
                     std::move(vids),
                     {},
                     pathNode_->edgeDirection(),
                     nullptr,
                     pathNode_->vertexProps(),
                     reverse ? pathNode_->reverseEdgeProps() : pathNode_->edgeProps(),
                     nullptr,
                     false,
                     false,
                     {},
                     -1,
                     nullptr,
                     nullptr)
      .via(qctx_->rctx()->runner())
      .thenValue([this, rowNum, stepNum, getNbrTime, reverse](auto&& resp) {
        addStats(resp, stepNum, getNbrTime.elapsedInUSec(), reverse);
        return buildPath(rowNum, std::move(resp), reverse);
      });
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
  return doBuildPath(rowNum, iter.get(), reverse);
}

Status SingleShortestPath::doBuildPath(size_t rowNum, GetNeighborsIter* iter, bool reverse) {
  auto iterSize = iter->size();
  auto& visitedVids = reverse ? rightVisitedVids_[rowNum] : leftVisitedVids_[rowNum];
  visitedVids.reserve(visitedVids.size() + iterSize);
  auto& allSteps = reverse ? allRightPaths_[rowNum] : allLeftPaths_[rowNum];
  allSteps.emplace_back();
  auto& currentStep = allSteps.back();

  auto& nextStepVids = reverse ? rightVids_[rowNum] : leftVids_[rowNum];
  nextStepVids.reserve(iterSize);

  QueryExpressionContext ctx(qctx_->ectx());
  for (iter->reset(); iter->valid(); iter->next()) {
    auto edgeVal = iter->getEdge();
    if (UNLIKELY(!edgeVal.isEdge())) {
      continue;
    }
    auto& edge = edgeVal.getEdge();
    auto& dst = edge.dst;
    if (visitedVids.find(dst) != visitedVids.end()) {
      continue;
    }
    visitedVids.emplace(edge.src);
    nextStepVids.emplace(dst);
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
  visitedVids.insert(nextStepVids.begin(), nextStepVids.end());
  return Status::OK();
}

folly::Future<Status> SingleShortestPath::handleResponse(size_t rowNum, size_t stepNum) {
  return folly::makeFuture<Status>(Status::OK())
      .via(qctx_->rctx()->runner())
      .thenValue([this, rowNum, stepNum](auto&& status) {
        UNUSED(status);
        return conjunctPath(rowNum, stepNum);
      })
      .thenValue([this, rowNum, stepNum](auto&& result) {
        if (result || stepNum * 2 >= maxStep_) {
          return folly::makeFuture<Status>(Status::OK());
        }
        auto& leftVids = leftVids_[rowNum];
        auto& rightVids = rightVids_[rowNum];
        if (leftVids.empty() || rightVids.empty()) {
          return folly::makeFuture<Status>(Status::OK());
        }
        return shortestPath(rowNum, stepNum + 1);
      });
}

folly::Future<bool> SingleShortestPath::conjunctPath(size_t rowNum, size_t stepNum) {
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
    return folly::makeFuture<bool>(true);
  }
  if (stepNum * 2 > maxStep_) {
    return folly::makeFuture<bool>(false);
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
    return folly::makeFuture<bool>(false);
  }
  return buildEvenPath(rowNum, meetVids);
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

folly::Future<bool> SingleShortestPath::buildEvenPath(size_t rowNum,
                                                      const std::vector<Value>& meetVids) {
  auto future = getMeetVidsProps(meetVids);
  return future.via(qctx_->rctx()->runner()).thenValue([this, rowNum](auto&& vertices) {
    if (vertices.empty()) {
      return false;
    }
    for (auto& meetVertex : vertices) {
      if (!meetVertex.isVertex()) {
        continue;
      }
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
  });
}

std::vector<Row> SingleShortestPath::createLeftPath(size_t rowNum, const Value& meetVid) {
  auto& allSteps = allLeftPaths_[rowNum];
  auto& lastSteps = allSteps.back();
  auto findMeetVid = lastSteps.find(meetVid);
  std::vector<Row> leftPaths(findMeetVid->second);
  for (auto stepIter = allSteps.rbegin() + 1; stepIter != allSteps.rend(); ++stepIter) {
    std::vector<Row> temp;
    for (auto& leftPath : leftPaths) {
      Value id = leftPath.values.front().getVertex().vid;
      auto findId = stepIter->find(id);
      for (auto& step : findId->second) {
        auto newPath = leftPath;
        newPath.values.insert(newPath.values.begin(), step.values.begin(), step.values.end());
        temp.emplace_back(std::move(newPath));
      }
    }
    leftPaths.swap(temp);
  }
  std::vector<Row> result;
  result.reserve(leftPaths.size());
  for (auto& path : leftPaths) {
    Row row;
    auto src = path.values.front();
    path.values.erase(path.values.begin());
    row.emplace_back(std::move(src));
    row.emplace_back(std::move(path));
    result.emplace_back(std::move(row));
  }
  return result;
}

std::vector<Row> SingleShortestPath::createRightPath(size_t rowNum,
                                                     const Value& meetVid,
                                                     bool oddStep) {
  auto& allSteps = allRightPaths_[rowNum];
  std::vector<Row> rightPaths;
  auto& lastSteps = allSteps.back();
  if (oddStep) {
    for (auto& steps : lastSteps) {
      bool flag = false;
      for (auto& step : steps.second) {
        auto& vertex = step.values.front();
        if (vertex.getVertex().vid == meetVid) {
          rightPaths.emplace_back(Row({vertex}));
          flag = true;
          break;
        }
      }
      if (flag) {
        break;
      }
    }
  } else {
    auto findMeetVid = lastSteps.find(meetVid);
    rightPaths = findMeetVid->second;
  }
  for (auto stepIter = allSteps.rbegin() + 1; stepIter != allSteps.rend() - 1; ++stepIter) {
    std::vector<Row> temp;
    for (auto& rightPath : rightPaths) {
      Value id = rightPath.values.front().getVertex().vid;
      auto findId = stepIter->find(id);
      for (auto& step : findId->second) {
        auto newPath = rightPath;
        newPath.values.insert(newPath.values.begin(), step.values.begin(), step.values.end());
        temp.emplace_back(std::move(newPath));
      }
    }
    rightPaths.swap(temp);
  }
  for (auto& path : rightPaths) {
    std::reverse(path.values.begin(), path.values.end());
  }
  return rightPaths;
}

}  // namespace graph
}  // namespace nebula
