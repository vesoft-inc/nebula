// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/BatchShortestPath.h"

#include <climits>

#include "graph/service/GraphFlags.h"
#include "sys/sysinfo.h"

using nebula::storage::StorageClient;
DECLARE_uint32(num_path_thread);
namespace nebula {
namespace graph {
size_t BatchShortestPath::splitTask(const std::unordered_set<Value>& startVids,
                                    const std::unordered_set<Value>& endVids) {
  size_t threadNum = FLAGS_num_path_thread;
  size_t startVidsSize = startVids.size();
  size_t endVidsSize = endVids.size();
  size_t maxSlices = startVidsSize < threadNum ? startVidsSize : threadNum;
  int minValue = INT_MAX;
  size_t startSlices = 1;
  size_t endSlices = 1;
  for (size_t i = maxSlices; i >= 1; --i) {
    auto j = threadNum / i;
    auto val = std::abs(static_cast<int>(startVidsSize / i) - static_cast<int>(endVidsSize / j));
    if (val < minValue) {
      minValue = val;
      startSlices = i;
      endSlices = j;
    }
  }
  VLOG(1) << "jmq : startVidsSize " << startVidsSize << " endVidsSize " << endVidsSize
          << " threadNum " << threadNum << " startSlices " << startSlices << "  endSlices "
          << endSlices;

  // split tasks
  {
    auto batchSize = startVidsSize / startSlices;
    size_t i = 0;
    size_t slices = 1;
    size_t count = 1;
    std::vector<StartVid> tmp;
    tmp.reserve(batchSize);
    for (auto& startVid : startVids) {
      tmp.emplace_back(startVid);
      if ((++i == batchSize && slices != startSlices) || count == startVidsSize) {
        batchStartVids_.emplace_back(std::move(tmp));
        tmp.reserve(batchSize);
        i = 0;
        ++slices;
      }
      ++count;
    }
  }
  {
    auto batchSize = endVidsSize / endSlices;
    size_t i = 0;
    size_t slices = 1;
    size_t count = 1;
    std::vector<EndVid> tmp;
    tmp.reserve(batchSize);
    for (auto& endVid : endVids) {
      tmp.emplace_back(endVid);
      if ((++i == batchSize && slices != endSlices) || count == endVidsSize) {
        batchEndVids_.emplace_back(std::move(tmp));
        tmp.reserve(batchSize);
        i = 0;
        ++slices;
      }
      ++count;
    }
  }
  VLOG(1) << "jmq : rowSize " << batchStartVids_.size() * batchEndVids_.size();
  VLOG(1) << "jmq : check startSlices * endSlices " << startSlices * endSlices;
  return startSlices * endSlices;
}

folly::Future<Status> BatchShortestPath::execute(const std::unordered_set<Value>& startVids,
                                                 const std::unordered_set<Value>& endVids,
                                                 DataSet* result) {
  size_t rowSize = init(startVids, endVids);
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

size_t BatchShortestPath::init(const std::unordered_set<Value>& startVids,
                               const std::unordered_set<Value>& endVids) {
  size_t rowSize = splitTask(startVids, endVids);

  leftVids_.reserve(rowSize);
  rightVids_.reserve(rowSize);
  allLeftVidMap_.reserve(rowSize);
  allRightVidMap_.reserve(rowSize);

  allLeftPaths_.resize(rowSize);
  allRightPaths_.reserve(rowSize);
  terminationMaps_.reserve(rowSize);
  resultDs_.resize(rowSize);

  for (auto& _startVids : batchStartVids_) {
    DataSet startDs;
    VidMap leftVidMap;
    for (auto& startVid : _startVids) {
      std::unordered_set<Value> dummy({startVid});
      leftVidMap.emplace(startVid, std::move(dummy));
      startDs.rows.emplace_back(Row({startVid}));
    }
    for (auto& _endVids : batchEndVids_) {
      VidMap rightVidMap;
      DataSet endDs;
      std::unordered_map<Value, std::vector<Row>> steps;
      for (auto& endVid : _endVids) {
        std::unordered_set<Value> tmp({endVid});
        rightVidMap.emplace(endVid, std::move(tmp));
        endDs.rows.emplace_back(Row({endVid}));
        std::vector<Row> dummy;
        steps.emplace(endVid, std::move(dummy));
      }
      // set VidMap
      allLeftVidMap_.emplace_back(leftVidMap);
      allRightVidMap_.emplace_back(std::move(rightVidMap));

      // set originRightpath
      HalfPath originRightPath({std::move(steps)});
      allRightPaths_.emplace_back(std::move(originRightPath));

      // set vid for getNeightbor
      leftVids_.emplace_back(startDs);
      rightVids_.emplace_back(std::move(endDs));

      // set terminateMap
      std::unordered_multimap<Value, std::pair<Value, bool>> terminationMap;
      for (auto& _startVid : _startVids) {
        for (auto& _endVid : _endVids) {
          if (_startVid != _endVid) {
            terminationMap.emplace(_startVid, std::make_pair(_endVid, true));
          }
        }
      }
      terminationMaps_.emplace_back(std::move(terminationMap));
    }
  }
  return rowSize;
}

folly::Future<Status> BatchShortestPath::shortestPath(size_t rowNum, size_t stepNum) {
  std::vector<folly::Future<Status>> futures;
  futures.emplace_back(getNeighbors(rowNum, false));
  futures.emplace_back(getNeighbors(rowNum, true));
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

folly::Future<Status> BatchShortestPath::getNeighbors(size_t rowNum, bool reverse) {
  StorageClient* storageClient = qctx_->getStorageClient();
  time::Duration getNbrTime;
  storage::StorageClient::CommonRequestParam param(pathNode_->space(),
                                                   qctx_->rctx()->session()->id(),
                                                   qctx_->plan()->id(),
                                                   qctx_->plan()->isProfileEnabled());
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
      .via(qctx_->rctx()->runner())
      .ensure([this, getNbrTime]() {
        stats_->emplace("total_rpc_time", folly::sformat("{}(us)", getNbrTime.elapsedInUSec()));
      })
      .thenValue([this, rowNum, reverse](auto&& resp) {
        addStats(resp, stats_);
        return buildPath(rowNum, std::move(resp), reverse);
      });
}

Status BatchShortestPath::buildPath(size_t rowNum, RpcResponse&& resps, bool reverse) {
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
  VLOG(1) << "jmq GetNeightbor : rowNum " << rowNum << " reverse " << reverse << " result " << list;
  auto listVal = std::make_shared<Value>(std::move(list));
  auto iter = std::make_unique<GetNeighborsIter>(listVal);
  return doBuildPath(rowNum, iter.get(), reverse);
}

Status BatchShortestPath::doBuildPath(size_t rowNum, GetNeighborsIter* iter, bool reverse) {
  size_t iterSize = iter->size();
  auto& historyVidMap = reverse ? allRightVidMap_[rowNum] : allLeftVidMap_[rowNum];
  historyVidMap.reserve(historyVidMap.size() + iterSize);
  auto& halfPath = reverse ? allRightPaths_[rowNum] : allLeftPaths_[rowNum];
  halfPath.emplace_back();
  auto& currentStep = halfPath.back();

  std::vector<Row> nextStepVids;
  nextStepVids.reserve(iterSize);

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
      // first step
      auto findCurrentStartVidsFromDst = currentVidMap.find(dst);
      if (findCurrentStartVidsFromDst == currentVidMap.end()) {
        std::unordered_set<Value> startVid({src});
        currentVidMap.emplace(dst, std::move(startVid));
      } else {
        findCurrentStartVidsFromDst->second.emplace(src);
      }
    } else {
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
    }

    auto vertex = iter->getVertex();
    Row step;
    step.emplace_back(std::move(vertex));
    step.emplace_back(std::move(edge));
    auto findDst = currentStep.find(dst);
    if (findDst == currentStep.end()) {
      nextStepVids.emplace_back(Row({dst}));
      std::vector<Row> steps({std::move(step)});
      currentStep.emplace(std::move(dst), std::move(steps));
    } else {
      findDst->second.emplace_back(std::move(step));
    }
  }
  // update historyVidMap
  for (auto& vidIter : currentVidMap) {
    historyVidMap[vidIter.first].insert(std::make_move_iterator(vidIter.second.begin()),
                                        std::make_move_iterator(vidIter.second.end()));
  }
  // set nextVid
  if (reverse) {
    rightVids_[rowNum].rows.swap(nextStepVids);
  } else {
    leftVids_[rowNum].rows.swap(nextStepVids);
  }
  return Status::OK();
}

folly::Future<Status> BatchShortestPath::handleResponse(size_t rowNum, size_t stepNum) {
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

bool BatchShortestPath::checkMeetVid(const Value& meetVid, size_t rowNum) {
  auto& terminationMap = terminationMaps_[rowNum];
  auto& leftVidMap = allLeftVidMap_[rowNum];
  auto& rightVidMap = allRightVidMap_[rowNum];
  auto& startVids = leftVidMap[meetVid];
  auto& endVids = rightVidMap[meetVid];
  for (const auto& startVid : startVids) {
    auto range = terminationMap.equal_range(startVid);
    if (range.first == range.second) {
      continue;
    }
    for (const auto& endVid : endVids) {
      for (auto found = range.first; found != range.second; ++found) {
        if (found->second.first == endVid) {
          return true;
        }
      }
    }
  }
  return false;
}

bool BatchShortestPath::conjunctPath(size_t rowNum, size_t stepNum) {
  const auto& leftStep = allLeftPaths_[rowNum].back();
  const auto& prevRightStep = allRightPaths_[rowNum][stepNum - 1];
  std::vector<Value> meetVids;
  for (const auto& step : leftStep) {
    if (prevRightStep.find(step.first) != prevRightStep.end()) {
      if (checkMeetVid(step.first, rowNum)) {
        meetVids.push_back(step.first);
      }
    }
  }
  if (!meetVids.empty() && buildOddPath(rowNum, meetVids)) {
    return true;
  }
  if (stepNum * 2 >= maxStep_) {
    return false;
  }
  std::vector<Value> dummy;
  meetVids.swap(dummy);
  const auto& rightStep = allRightPaths_[rowNum].back();
  for (const auto& step : leftStep) {
    if (rightStep.find(step.first) != rightStep.end()) {
      if (checkMeetVid(step.first, rowNum)) {
        meetVids.push_back(step.first);
      }
    }
  }
  if (meetVids.empty()) {
    return false;
  }
  return buildEvenPath(rowNum, meetVids);
}

bool BatchShortestPath::updateTermination(size_t rowNum) {
  auto& terminationMap = terminationMaps_[rowNum];
  for (auto iter = terminationMap.begin(); iter != terminationMap.end();) {
    if (!iter->second.second) {
      iter = terminationMap.erase(iter);
    } else {
      ++iter;
    }
  }
  if (terminationMap.empty()) {
    return true;
  }
  return false;
}

bool BatchShortestPath::buildOddPath(size_t rowNum, const std::vector<Value>& meetVids) {
  auto& terminationMap = terminationMaps_[rowNum];
  for (auto& meetVid : meetVids) {
    auto leftPaths = createLeftPath(rowNum, meetVid);
    auto rightPaths = createRightPath(rowNum, meetVid, true);
    for (auto& leftPath : leftPaths) {
      auto range = terminationMap.equal_range(leftPath.values.front().getVertex().vid);
      if (range.first == range.second) {
        continue;
      }
      for (auto& rightPath : rightPaths) {
        for (auto found = range.first; found != range.second; found++) {
          if (found->second.first == rightPath.values.back().getVertex().vid) {
            Row path = leftPath;
            auto& steps = path.values.back().mutableList().values;
            steps.insert(steps.end(), rightPath.values.begin(), rightPath.values.end() - 1);
            path.emplace_back(rightPath.values.back());
            resultDs_[rowNum].rows.emplace_back(std::move(path));

            found->second.second = false;
          }
        }
      }
    }
  }
  return updateTermination(rowNum);
}

bool BatchShortestPath::buildEvenPath(size_t rowNum, const std::vector<Value>& meetVids) {
  auto& terminationMap = terminationMaps_[rowNum];
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
      auto range = terminationMap.equal_range(leftPath.values.front().getVertex().vid);
      if (range.first == range.second) {
        continue;
      }
      for (auto& rightPath : rightPaths) {
        for (auto found = range.first; found != range.second; found++) {
          if (found->second.first == rightPath.values.back().getVertex().vid) {
            Row path = leftPath;
            auto& steps = path.values.back().mutableList().values;
            steps.insert(steps.end(), rightPath.values.begin(), rightPath.values.end() - 1);
            path.emplace_back(rightPath.values.back());
            resultDs_[rowNum].rows.emplace_back(std::move(path));

            found->second.second = false;
          }
        }
      }
    }
  }
  return updateTermination(rowNum);
}

}  // namespace graph
}  // namespace nebula
