// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/ShortestPathBase.h"

#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"
#include "sys/sysinfo.h"

using nebula::storage::StorageClient;

DEFINE_uint32(num_path_thread, 0, "number of concurrent threads when do shortest path");
namespace nebula {
namespace graph {

folly::Future<Status> ShortestPathBase::shortestPath(size_t rowNum, size_t stepNum) {
  std::vector<folly::Future<Status>> futures;
  futures.emplace_back(getNeighbors(rowNum, false));
  futures.emplace_back(getNeighbors(rowNum, true));
  return folly::collect(futures).via(runner()).thenValue([this, rowNum, stepNum](auto&& resps) {
    for (auto& resp : resps) {
      if (!resp.ok()) {
        return folly::makeFuture<Status>(std::move(resp));
      }
    }
    return handleResponse(rowNum, stepNum);
  });
}

folly::Future<Status> ShortestPathBase::getNeighbors(size_t rowNum, bool reverse) {
  StorageClient* storageClient = qctx_->getStorageClient();
  time::Duration getNbrTime;
  storage::StorageClient::CommonRequestParam param(pathNode_->space(),
                                                   qctx()->rctx()->session()->id(),
                                                   qctx()->plan()->id(),
                                                   qctx()->plan()->isProfileEnabled());
  auto& inputRows = reverse ? rightVids_[rowNum].rows : leftVids_[rowNum].rows;
  return storageClient
      ->getNeighbors(param,
                     {nebula::kVid},
                     std::move(inputRows),
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
                     nullptr)
      .via(runner())
      .ensure([this, getNbrTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("total_rpc_time", folly::sformat("{}(us)", getNbrTime.elapsedInUSec()));
      })
      .thenValue([this, rowNum, reverse](auto&& resp) {
        SCOPED_TIMER(&execTime_);
        addStats(resp, otherStats_);
        return buildPath(rowNum, std::move(resp), reverse);
      });
}

folly::Future<Status> ShortestPathBase::handleResponse(size_t rowNum, size_t stepNum) {
  if (conjunctPath(rowNum, stepNum)) {
    return Status::OK();
  }
  auto& leftVids = leftVids_[rowNum].rows;
  auto& rightVids = rightVids_[rowNum].rows;
  if (stepNum * 2 >= maxStep_ || leftVids.empty() || rightVids.empty()) {
    return Status::OK();
  }
  return shortestPath(rowNum, ++stepNum);
}

bool ShortestPathBase::conjunctPath(size_t rowNum, size_t stepNum) {
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

Status ShortestPathBase::buildPath(size_t rowNum, RpcResponse&& resps, bool reverse) {
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

Status ShortestPathBase::buildMultiPairPath(size_t rowNum, GetNeighborsIter* iter, bool reverse) {
  size_t iterSize = iter->size();
  auto historyVidMap = reverse ? allRightVidMap_[rowNum] : allLeftVidMap_[rowNum];
  historyVidMap.reserve(historyVidMap.size() + iterSize);
  auto& halfPath = reverse ? allRightPaths_[rowNum] : allLeftPaths_[rowNum];
  halfPath.emplace_back();
  auto& currentStep = halfPath.back();

  DataSet nextStepVids;
  nextStepVids.colNames = {nebula::kVid};

  if (step_ == 1) {
    for (; iter->valid(); iter->next()) {
      auto edgeVal = iter->getEdge();
      if (UNLIKELY(!edgeVal.isEdge())) {
        continue;
      }
      auto& edge = edgeVal.getEdge();
      auto& src = edge.src;
      auto& dst = edge.dst;
      if (src == dst) {
        continue;
      }
      auto findStartVidsFromDst = historyVidMap.find(dst);
      if (findStartVidsFromDst == historyVidMap.end()) {
        std::unordered_set<Value> startVid({src});
        historyVidMap.emplace(dst, std::move(startVid));
      } else {
        findStartVidsFromDst->second.emplace(src);
      }
      auto vertex = iter->getVertex();
      Row step;
      step.emplace_back(std::move(vertex));
      step.emplace_back(std::move(edge));
      auto findDst = currentStep.find(dst);
      if (findDst == currentStep.end()) {
        nextStepVids.rows.emplace_back(Row({dst}));
        std::vector<Row> steps({std::move(step)});
        currentStep.emplace(std::move(dst), std::move(steps));
      } else {
        findDst->second.emplace_back(std::move(step));
      }
    }
  } else {
    VidMap currentVidMap;
    currentVidMap.reserve(iterSize);
    for (; iter->valid(); iter->next()) {
      auto edgeVal = iter->getEdge();
      if (UNLIKELY(!edgeVal.isEdge())) {
        continue;
      }
      auto& edge = edgeVal.getEdge();
      auto& src = edge.src;
      auto& dst = edge.dst;
      if (src == dst) {
        continue;
      }
      // get start vids from edge's src
      auto findHistoryStartVidsFromSrc = historyVidMap.find(src);
      if (findHistoryStartVidsFromSrc == historyVidMap.end()) {
        DLOG(ERROR) << "can't access here";
        continue;
      }
      auto startVidsFromSrc = findHistoryStartVidsFromSrc->second;
      // get start vids from edge's dst
      auto findHistoryStartVidsFromDst = historyVidMap.find(dst);
      if (findHistoryStartVidsFromDst != historyVidMap.end()) {
        // dst in history
        auto& startVidsFromDst = findHistoryStartVidsFromDst->second;
        if (startVidsFromDst == startVidsFromSrc) {
          // loop: a->b->c->a or a->b->c->b
          continue;
        }
      }
      currentVidMap[dst].insert(startVidsFromSrc.begin(), startVidsFromSrc.end());

      auto vertex = iter->getVertex();
      Row step;
      step.emplace_back(std::move(vertex));
      step.emplace_back(std::move(edge));
      auto findDst = currentStep.find(dst);
      if (findDst == currentStep.end()) {
        nextStepVids.rows.emplace_back(Row({dst}));
        std::vector<Row> steps({std::move(step)});
        currentStep.emplace(std::move(dst), std::move(steps));
      } else {
        findDst->second.emplace_back(std::move(step));
      }
    }
    // update historyVidMap
    for (auto& iter : currentVidMap) {
      historyVidMap[iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                       std::make_move_iterator(iter.second.end()));
    }
  }
  // set nextVid
  if (reverse) {
    rightVids_[rowNum].rows.swap(nextStepVids);
  } else {
    leftVids_[rowNum].rows.swap(nextStepVids);
  }
  return Status::OK();
}

Status ShortestPathBase::buildSinglePairPath(size_t rowNum, GetNeighborsIter* iter, bool reverse) {
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

void ShortestPathBase::buildOddPath(size_t rowNum, const std::vector<Value>& meetVids) {
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

bool ShortestPathBase::buildEvenPath(size_t rowNum, const std::vector<Value>& meetVids) {
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

std::vector<Row> ShortestPathBase::createLeftPath(size_t rowNum, const Value& meetVid) {
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

std::vector<Row> ShortestPathBase::createRightPath(size_t rowNum,
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

folly::Future<Status> ShortestPathBase::getMeetVidsProps(const std::vector<Value>& meetVids,
                                                         std::vector<Value>& meetVertices) {
  SCOPED_TIMER(&execTime_);
  nebula::DataSet vertices({kVid});
  vertices.rows.reserve(meetVids.size());
  for (auto& vid : meetVids) {
    vertices.emplace_back(Row({vid}));
  }

  time::Duration getPropsTime;
  StorageClient* storageClient = qctx()->getStorageClient();
  StorageClient::CommonRequestParam param(pathNode_->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());
  return DCHECK_NOTNULL(storageClient)
      ->getProps(param,
                 std::move(vertices),
                 pathNode_->vertexProps(),
                 nullptr,
                 nullptr,
                 false,
                 {},
                 -1,
                 nullptr)
      .via(runner())
      .ensure([this, getPropsTime]() {
        SCOPED_TIMER(&execTime_);
        otherStats_.emplace("get_prop_total_rpc",
                            folly::sformat("{}(us)", getPropsTime.elapsedInUSec()));
      })
      .thenValue([this, &meetVertices](PropRpcResponse&& resp) {
        SCOPED_TIMER(&execTime_);
        addStats(resp, otherStats_);
        return handlePropResp(std::move(resp), meetVertices);
      });
}

Status ShortestPathBase::handlePropResp(PropRpcResponse&& resps, std::vector<Value>& vertices) {
  auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
  NG_RETURN_IF_ERROR(result);
  nebula::DataSet v;
  for (auto& resp : resps.responses()) {
    if (resp.props_ref().has_value()) {
      if (UNLIKELY(!v.append(std::move(*resp.props_ref())))) {
        // it's impossible according to the interface
        LOG(WARNING) << "Heterogeneous props dataset";
      }
    } else {
      LOG(WARNING) << "GetProp partial success";
    }
  }
  auto val = std::make_shared<Value>(std::move(v));
  auto iter = std::make_unique<PropIter>(val);
  vertices.reserve(iter->size());
  for (; iter->valid(); iter->next()) {
    vertices.emplace_back(iter->getVertex());
  }
  return Status::OK();
}
}  // namespace graph
}  // namespace nebula
