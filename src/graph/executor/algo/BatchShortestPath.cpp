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
  allLeftPathMaps_.reserve(rowSize);
  allRightPathMaps_.reserve(rowSize);
  currentLeftPathMaps_.resize(rowSize);
  currentRightPathMaps_.resize(rowSize);
  preRightPathMaps_.reserve(rowSize);

  terminationMaps_.reserve(rowSize);
  resultDs_.resize(rowSize);

  for (auto& _startVids : batchStartVids_) {
    DataSet startDs;
    PathMap leftPathMap;
    for (auto& startVid : _startVids) {
      startDs.rows.emplace_back(Row({startVid}));
      std::vector<CustomPath> dummy;
      leftPathMap[startVid].emplace(startVid, std::move(dummy));
    }
    for (auto& _endVids : batchEndVids_) {
      DataSet endDs;
      PathMap preRightPathMap;
      PathMap rightPathMap;
      for (auto& endVid : _endVids) {
        endDs.rows.emplace_back(Row({endVid}));
        std::vector<CustomPath> dummy;
        rightPathMap[endVid].emplace(endVid, dummy);
        preRightPathMap[endVid].emplace(endVid, std::move(dummy));
      }

      // set originRightpath
      allLeftPathMaps_.emplace_back(leftPathMap);
      preRightPathMaps_.emplace_back(std::move(preRightPathMap));
      allRightPathMaps_.emplace_back(std::move(rightPathMap));

      // set vid for getNeightbor
      leftVids_.emplace_back(startDs);
      rightVids_.emplace_back(std::move(endDs));

      // set terminateMap
      std::unordered_multimap<StartVid, std::pair<EndVid, bool>> terminationMap;
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
  auto& historyPathMap = reverse ? allRightPathMaps_[rowNum] : allLeftPathMaps_[rowNum];
  auto& currentPathMap = reverse ? currentRightPathMaps_[rowNum] : currentLeftPathMaps_[rowNum];

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
    auto vertex = iter->getVertex();
    CustomPath customPath;
    customPath.emplace_back(std::move(vertex));
    customPath.emplace_back(std::move(edge));

    auto findSrcFromHistory = historyPathMap.find(src);
    if (findSrcFromHistory == historyPathMap.end()) {
      // first step
      auto findDstFromCurrent = currentPathMap.find(dst);
      if (findDstFromCurrent == currentPathMap.end()) {
        std::vector<CustomPath> tmp({std::move(customPath)});
        currentPathMap[dst].emplace(src, std::move(tmp));
      } else {
        auto findSrc = findDstFromCurrent->second.find(src);
        if (findSrc == findDstFromCurrent->second.end()) {
          std::vector<CustomPath> tmp({std::move(customPath)});
          findDstFromCurrent->second.emplace(src, std::move(tmp));
        } else {
          // same <src, dst>, different edge type or rank
          findSrc->second.emplace_back(std::move(customPath));
        }
      }
    } else {
      // not first step
      auto& srcPathMap = findSrcFromHistory->second;
      auto findDstFromHistory = historyPathMap.find(dst);
      if (findDstFromHistory == historyPathMap.end()) {
        // dst not in history
        auto findDstFromCurrent = currentPathMap.find(dst);
        if (findDstFromCurrent == currentPathMap.end()) {
          // dst not in current, new edge
          for (const auto& srcPath : srcPathMap) {
            currentPathMap[dst].emplace(srcPath.first, createPaths(srcPath.second, customPath));
          }
        } else {
          // dst in current
          for (const auto& srcPath : srcPathMap) {
            auto newPaths = createPaths(srcPath.second, curstomPath);
            auto findSrc = findDstFromCurrent->second.find(srcPath.first);
            if (findSrc == findDstFromCurrent->second.end()) {
              findDstFromCurrent->second.emplace(srcPath.first, std::move(newPaths));
            } else {
              findSrc->second.insert(findSrc->second.begin(),
                                     std::make_move_iterator(newPaths.begin()),
                                     std::make_move_iterator(newPaths.end()));
            }
          }
        }
      } else {
        // dst in history
        auto& dstPathMap = findDstFromHistory->second;
        for (const auto& srcPath : srcPathMap) {
          if (dstPathMap.find(srcPath.frist) != dstPathMap.end()) {
            // loop : a->b->c->a or a->b->c->b
            // filter out path that with duplicate vertex or have already been found before
            continue;
          }
          auto findDstFromCurrent = currentPathMap.find(dst);
          if (findDstFromCurrent == currentPathMap.end()) {
            currentPathMap[dst].emplace(srcPath.first, createPaths(srcPath.second, customPath));
          } else {
            auto newPaths = createPaths(srcPath.second, customPath);
            auto findSrc = findDstFromCurrent->second.find(srcPath.first);
            if (findSrc == findDstFromCurrent->second.end()) {
              findDstFromCurrent->second.emplace(srcPath.first, std::move(customPath));
            } else {
              findSrc->second.insert(findSrc->second.begin(),
                                     std::make_move_iterator(newPaths.begin()),
                                     std::make_move_iterator(newPaths.end()));
            }
          }
        }
      }
    }
  }
  // set nextVid
  setNextStepVid(currentPathMap, rowNum, reverse);
  return Status::OK();
}

folly::Future<Status> BatchShortestPath::handleResponse(size_t rowNum, size_t stepNum) {
  if (conjunctPath(rowNum, stepNum, true)) {
    return Status::OK();
  }
  if (stepNum * 2 < maxStep_ && conjunctPath(rowNum, stepNum, false)) {
    return Status::OK();
  }
  auto& leftVids = leftVids_[rowNum].rows;
  auto& rightVids = rightVids_[rowNum].rows;
  if (stepNum * 2 >= maxStep_ || leftVids.empty() || rightVids.empty()) {
    return Status::OK();
  }
  // update allPathMap
  auto& leftPathMap = currentLeftPathMaps_[rowNum];
  auto& rightPathMap = currentRightPathMaps_[rowNum];
  preRightPathMaps_[rowNum] = rightPathMap;
  for (auto& iter : leftPathMap) {
    allLeftPathMaps_[rowNum][iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                                std::make_move_iterator(iter.second.end()));
  }
  for (auto& iter : rightPathMap) {
    allRightPathMaps_[rowNum][iter.first].insert(std::make_move_iterator(iter.second.begin()),
                                                 std::make_move_iterator(iter.second.end()));
  }
  leftPathMap.clear();
  rightPathMap.clear();
  return shortestPath(rowNum, ++stepNum);
}

bool BatchShortestPath::conjunctPath(size_t rowNum, size_t stepNum, bool oddStep) {
  auto& leftPathMaps = currentLeftPathMaps_[rowNum];
  auto& rightPathMaps = oddStep ? preRightPathMaps_[rowNum] : currentRightPathMaps_[rowNum];
  auto& terminationMap = terminationMaps_[rowNum];

  std::vector<Value> meetVids;
  meetVids.reserve(leftPathMaps.size());
  for (const auto& leftPathMap : leftPathMaps) {
    auto findCommonVid = rightPathMaps.find(leftPathMap.first);
    if (findCommonVid != rightPathMaps.end()) {
      meetVids.emplace_back(findCommonVid->first);
    }
  }
  if (meetVids.empty()) {
    return false;
  }
  std::unordered_map<Value, Value> verticesMap;
  std::vector<Value> meetVertices;
  meetVertices.reserve(meetVids.size());
  verticesMap.reserve(meetVids.size());
  if (oddStep) {
    for (auto& meetVid : meetVids) {
      for (auto& dstPaths : currentRightPathMaps_[rowNum]) {
        bool flag = false;
        for (auto& srcPaths : dstPaths) {
          for (auto& path : srcPaths.second) {
            auto& vertex = path.values[path.size() - 2];
            auto& vid = vertex.getVertex().vid;
            if (vid == meetVid) {
              verticesMap[vid] = vertex;
              flag = true;
              break;
            }
          }
          if (flag) {
            break;
          }
        }
        if (flag) {
          break;
        }
      }
    }
  } else {
    auto status = getMeetVidsProps(meetVids, meetVertices).get();
    if (!status.ok() || meetVertices.empty()) {
      return false;
    }
    for (auto& vertex : meetVertices) {
      verticesMap[vertex.getVertex().vid] = std::move(vertex);
    }
  }

  for (const auto& leftPathMap : leftPathMaps) {
    auto findCommonVid = rightPathMaps.find(leftPathMap.first);
    if (findCommonVid == rightPathMaps.end()) {
      continue;
    }
    auto findCommonVertex = verticesMap.find(findCommonVid->first);
    if (findCommonVertex == verticesMap.end()) {
      continue;
    }
    auto& rightPaths = findCommonVid->second;
    for (const auto& srcPaths : leftPathMap.second) {
      auto range = terminationMap.equal_range(srcPaths.first);
      if (range.first == range.second) {
        continue;
      }
      for (const auto& dstPaths : rightPaths) {
        for (auto found = range.first; found != range.second; ++found) {
          if (found->second.first == dstPaths.first) {
            doConjunctPath(srcPaths.second, dstPaths.second, findCommonVertex->second, rowNum);
            found->second.second = false;
          }
        }
      }
    }
  }
  // update terminationMap
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

void BatchShortestPath::doConjunctPath(const std::vector<CustomPath>& leftPaths,
                                       const std::vector<CustomPath>& rightPaths,
                                       const Value& commonVertex,
                                       size_t rowNum) {
  auto& resultDs = resultDs_[rowNum];
  for (const auto& leftPath : leftPaths) {
    for (const auto& rightPath : rightPaths) {
      auto forwardPath = leftPath;
      auto backwardPath = rightPath;
      auto src = forwardPath.values.front();
      forwardPath.values.erase(forwardPath.values.begin());
      forwardPath.values.emplace_back(commonVertex);
      std::reverse(backwardPath.values.begin(), backwardPath.values.end());
      forwardPath.values.insert(forwardPath.values.end(),
                                std::make_move_iterator(backwardPath.begin()),
                                std::make_move_iterator(backwardPath.end()));
      auto dst = forwardPath.back();
      forwardPath.pop_back();
      Row row;
      row.emplace_back(std::move(src));
      row.emplace_back(std::move(forwardPath));
      row.emplace_back(std::move(dst));
      resultDs.rows.emplace_back(std::move(row));
    }
  }
}

// [a, a->b, b, b->c ] insert [c, c->d]  result is [a, a->b, b, b->c, c, c->d]
std::vector<CustomPath> BatchShortestPath::createPaths(const std::vector<CustomPath>& paths,
                                                       const CustomPath& path) {
  std::vector<CustomPath> newPaths;
  newPaths.reserve(paths.size());
  for (const auto& p : paths) {
    CustomPath customPath = p;
    customPath.values.insert(customPath.end(), path.values.begin(), path.values.end());
    newPaths.emplace_back(std::move(path));
  }
  return newPaths;
}

void BatchShortestPath::setNextStepVid(const PathMap& paths, size_t rowNum, bool reverse) {
  std::vector<Row> nexStepVids;
  nextStepVids.reserve(paths.size());
  for (const auto& path : paths) {
    Row row;
    row.values.emplace_back(path.first);
    nextStepVids.emplace_back(std::move(row));
  }
  if (reverse) {
    rightVids_[rowNum].rows.swap(nextStepVids);
  } else {
    leftVids_[rowNum].rows.swap(nextStepVids);
  }
}

}  // namespace graph
}  // namespace nebula
