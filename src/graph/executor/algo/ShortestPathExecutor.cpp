// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/ShortestPathExecutor.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"

using nebula::storage::StorageClient;
namespace nebula {
namespace graph {
folly::Future<Status> ShortestPathExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  single_ = pathNode_->single();
  range_ = {pathNode_->stepRange()->min(), pathNode_->stepRange()->max()};
  auto& colNames = pathNode_->colNames();
  auto rowSize = buildRequestDataSet();
  std::vector<folly::Future<Status>> futures;
  for (size_t rowNum = 0; rowNum < rowSize; ++rowNum) {
    resultDs_[rowNum].colNames = colNames;
    futures.emplace_back(shortestPath(rowNum, 0));
  }
  return folly::collect(futures).via(runner()).thenValue([this, &colNames](auto&& resps) {
    for (auto& resp : resps) {
      NG_RETURN_IF_ERROR(resp);
    }
    // append dataset
    DataSet result;
    result.colNames = colNames;
    for (auto& ds : resultDs_) {
      result.append(std::move(ds));
    }
    return finish(ResultBuilder().value(Value(std::move(result))).build());
  });
}

size_t ShortestPathExecutor::buildRequestDataSet() {
  auto iter = ectx_->getResult(pathNode_->inputVar()).iter();
  auto rowSize = iter->size();
  leftVids_.reserve(rowSize);
  rightVids_.reserve(rowSize);
  leftVisitedVids_.resize(rowSize);
  rightVisitedVids_.resize(rowSize);
  allLeftSteps_.resize(rowSize);
  allRightSteps_.reserve(rowSize);
  resultDs_.resize(rowSize);

  const auto& vidType = *(qctx()->rctx()->session()->space().spaceDesc.vid_type_ref());
  std::unordered_set<std::pair<Value, Value>> uniqueSet;
  uniqueSet.reserve(rowSize);
  for (; iter->valid(); iter->next()) {
    auto start = iter->getColumn(0);
    auto end = iter->getColumn(1);
    if (!SchemaUtil::isValidVid(start, vidType) || !SchemaUtil::isValidVid(end, vidType)) {
      LOG(ERROR) << "Mismatched vid type. start type : " << start.type()
                 << ", end type: " << end.type()
                 << ", space vid type: " << SchemaUtil::typeToString(vidType);
      continue;
    }
    if (start == end) {
      // continue or return error
      continue;
    }
    if (uniqueSet.emplace(std::pair<Value, Value>{start, end}).second) {
      // set origin rightStep_;
      std::unordered_map<Value, std::vector<Row>> steps;
      std::vector<Row> dummy;
      steps.emplace(end, std::move(dummy));
      std::vector<std::unordered_map<Value, std::vector<Row>>> originRightStep({std::move(steps)});
      allRightSteps_.emplace_back(std::move(originRightStep));

      DataSet startDs, endDs;
      startDs.rows.emplace_back(Row({std::move(start)}));
      endDs.rows.emplace_back(Row({std::move(end)}));
      leftVids_.emplace_back(std::move(startDs));
      rightVids_.emplace_back(std::move(endDs));
    }
  }
  return rowSize;
}

folly::Future<Status> ShortestPathExecutor::shortestPath(size_t rowNum, size_t stepNum) {
  VLOG(1) << "current rowNum is : " << rowNum << " step is :" << stepNum;
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

folly::Future<Status> ShortestPathExecutor::getNeighbors(size_t rowNum, bool reverse) {
  if (reverse) {
    VLOG(1) << "reverse GetNeightbor input : " << rightVids_[rowNum].toString();
  } else {
    VLOG(1) << "GetNeightbor input : " << leftVids_[rowNum].toString();
  }
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
      .ensure([this, rowNum, getNbrTime]() {
        SCOPED_TIMER(&execTime_);
        // otherStats_.emplace("rowNum", rowNum);
        otherStats_.emplace("total_rpc_time", folly::sformat("{}(us)", getNbrTime.elapsedInUSec()));
      })
      .thenValue([this, rowNum, reverse](auto&& resp) {
        SCOPED_TIMER(&execTime_);
        addStats(resp, otherStats_);
        return buildPath(rowNum, std::move(resp), reverse);
      });
}

folly::Future<Status> ShortestPathExecutor::handleResponse(size_t rowNum, size_t stepNum) {
  if (conjunctPath(rowNum, stepNum)) {
    return Status::OK();
  }
  stepNum++;
  if (stepNum * 2 >= range_.second) {
    return Status::OK();
  }
  return shortestPath(rowNum, stepNum);
}

bool ShortestPathExecutor::conjunctPath(size_t rowNum, size_t stepNum) {
  const auto& leftStep = allLeftSteps_[rowNum].back();
  const auto& prevRightStep = allRightSteps_[rowNum][stepNum];
  std::vector<Value> meetVids;
  for (const auto& step : leftStep) {
    if (prevRightStep.find(step.first) != prevRightStep.end()) {
      meetVids.push_back(step.first);
    }
  }
  if (!meetVids.empty()) {
    buildOddPath(rowNum, meetVids);
    return true;
  }
  if (stepNum * 2 >= range_.second) {
    return false;
  }

  const auto& rightStep = allRightSteps_[rowNum].back();
  for (const auto& step : leftStep) {
    if (rightStep.find(step.first) != rightStep.end()) {
      meetVids.push_back(step.first);
    }
  }
  if (meetVids.empty()) {
    return false;
  }
  return buildEvenPath(rowNum, meetVids);
}

Status ShortestPathExecutor::buildPath(size_t rowNum, RpcResponse&& resps, bool reverse) {
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
  if (reverse) {
    VLOG(1) << "reverse GetNeightbor output: " << list.toString().c_str();
  } else {
    VLOG(1) << "GetNeightbor output: " << list.toString().c_str();
  }
  auto listVal = std::make_shared<Value>(std::move(list));
  auto iter = std::make_unique<GetNeighborsIter>(listVal);
  return doBuildPath(rowNum, iter.get(), reverse);
}

Status ShortestPathExecutor::doBuildPath(size_t rowNum, GetNeighborsIter* iter, bool reverse) {
  auto iterSize = iter->size();
  auto& visitedVids = reverse ? rightVisitedVids_[rowNum] : leftVisitedVids_[rowNum];
  visitedVids.reserve(visitedVids.size() + iterSize);
  auto& allSteps = reverse ? allRightSteps_[rowNum] : allLeftSteps_[rowNum];
  allSteps.emplace_back();
  auto& currentStep = allSteps.back();

  std::unordered_set<Value> uniqueDst;
  uniqueDst.reserve(iterSize);
  std::vector<Row> nextStepVids;
  nextStepVids.reserve(iterSize);

  QueryExpressionContext ctx(ectx_);
  auto* vFilter = pathNode_->vFilter();
  auto* eFilter = pathNode_->eFilter();
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
    if (vFilter != nullptr) {
      auto& vFilterVal = vFilter->eval(ctx(iter));
      if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
        continue;
      }
    }
    if (eFilter != nullptr) {
      auto& eFilterVal = eFilter->eval(ctx(iter));
      if (!eFilterVal.isBool() || !eFilterVal.getBool()) {
        continue;
      }
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
    VLOG(1) << "reverse next Vid : " << rightVids_[rowNum].toString() << " rowNum" << rowNum;
  } else {
    leftVids_[rowNum].rows.swap(nextStepVids);
    VLOG(1) << "current next Vid : " << leftVids_[rowNum].toString() << " rowNum " << rowNum;
  }
  return Status::OK();
}

void ShortestPathExecutor::buildOddPath(size_t rowNum, const std::vector<Value>& meetVids) {
  for (auto& meetVid : meetVids) {
    auto leftPaths = createLeftPath(rowNum, meetVid);
    auto rightPaths = createRightPath(rowNum, meetVid, true);
    for (auto& leftPath : leftPaths) {
      for (auto& rightPath : rightPaths) {
        Row path = leftPath;
        auto& steps = path.values.back().mutableList().values;
        steps.insert(steps.end(), rightPath.values.begin(), rightPath.values.end());
        resultDs_[rowNum].rows.emplace_back(std::move(path));
      }
    }
  }
}

bool ShortestPathExecutor::buildEvenPath(size_t rowNum, const std::vector<Value>& meetVids) {
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
        steps.insert(steps.end(), rightPath.values.begin(), rightPath.values.end());
        resultDs_[rowNum].rows.emplace_back(std::move(path));
      }
    }
  }
  return true;
}

std::vector<Row> ShortestPathExecutor::createLeftPath(size_t rowNum, const Value& meetVid) {
  auto& allSteps = allLeftSteps_[rowNum];
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

std::vector<Row> ShortestPathExecutor::createRightPath(size_t rowNum,
                                                       const Value& meetVid,
                                                       bool oddStep) {
  auto& allSteps = allRightSteps_[rowNum];
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

folly::Future<Status> ShortestPathExecutor::getMeetVidsProps(const std::vector<Value>& meetVids,
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
  auto* vFilter = pathNode_->vFilter();
  return DCHECK_NOTNULL(storageClient)
      ->getProps(param,
                 std::move(vertices),
                 pathNode_->vertexProps(),
                 nullptr,
                 nullptr,
                 false,
                 {},
                 -1,
                 vFilter)
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

Status ShortestPathExecutor::handlePropResp(PropRpcResponse&& resps, std::vector<Value>& vertices) {
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
