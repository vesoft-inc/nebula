// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.
#include "graph/executor/algo/ShortestPathExecutor.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;

namespace nebula {
namespace graph {

Status ShortestPathExecutor::buildRequestDataSet() {
  auto iter = ectx_->getResult(pathNode_->inputVar()).iter();
  cartesianProduct_.reserve(iter->size());
  const auto& vidType = *(qctx()->rctx()->session()->space().spaceDesc.vid_type_ref());

  std::unordered_set<std::pair<Value, Value>> uniqueSet;
  uniqueSet.reserve(iter->size());
  for (; iter->valid(); iter->next()) {
    auto start = iter->getColumn(0);
    auto end = iter->getColumn(1);
    if (!SchemaUtil::isValidVid(start, vidType) || !SchemaUtil::isValidVid(end, vidType)) {
      return Status::Error("Mismatched vid type, space vid type: %s",
                           SchemaUtil::typeToString(vidType));
    }
    if (start == end) {
      // continue or return error
      continue;
    }
    if (uniqueSet.emplace(std::pair<Value, Value>{start, end}).second) {
      cartesianProduct_.emplace_back(std::move(start), std::move(end));
    }
  }
  if (!cartesianProduct_.empty()) {
    leftVids_.rows.emplace_back(Row({std::move(cartesianProduct_.front().first)}));
    rightVids_.rows.emplace_back(Row({std::move(cartesianProduct_.front().second)}));
  }
  return Status::OK();
}

Status ShortestPathExecutor::handlePropResp(StorageRpcResponse<GetPropResponse>&& resps,
                                            std::vector<Vertex>& vertices) {
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
    vertices.emplace(iter->getVertex());
  }
  return Status::OK();
}

folly::Future<Status> ShortestPathExecutor::getMeetVidsProps(const std::vector<Value>& meetVids,
                                                             std::vector<Vertex>& meetVertices) {
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
      .thenValue([this, &meetVertices](StorageRpcResponse<GetPropResponse>&& resp) {
        SCOPED_TIMER(&execTime_);
        // addStats(resp, otherStats_);
        return handlePropResp(std::move(resp), meetVertices);
      });
}

std::vector<Row> ShortestPathExecutor::createLeftPath(Value& meetVid) {
  std::vector<Row> leftPaths;
  std::vector<Row> interimPaths({meetVid});
  for (auto stepIter = allLeftSteps_.rbegin(); stepIter < allLeftSteps_.rend(); ++stepIter) {
    std::vector<Row> temp;
    for (auto& interimPath : interimPaths) {
      Value id = meetVid;
      if (interimPath.size() != 1) {
        auto& row = interimPath.back();
        id = row.values.front().getVertex().vid;
      }

      auto findStep = stepIter.find(id);
      for (auto step : findStep.second) {
        Row path = interimPath;
        path.emplace_back(step);
        if (stepIter == allLeftSteps_.rend() - 1) {
          leftPaths.emplace_back(std::move(path));
        } else {
          temp.emplace_back(std::move(path));
        }
      }  // step
    }    // interimPath
    if (stepIter != allLeftSteps_.rend() - 1) {
      interimPaths.swap(temp);
    }
  }  // stepIter
  std::vector<Row> result;
  result.reserve(leftPaths.size());
  for (auto& path : leftPaths) {
    Row row, steps;
    auto& fistStep = path.values.back();
    row.emplace_back(firstStep.values.front());
    steps.emplace_back(firstStep.values.back());
    for (auto iter = path.rbegin() + 1; iter != path.rend() - 1; ++iter) {
      steps.values.insert(std::make_move_iterator(iter->values.begin()),
                          std::make_move_iterator(iter->values.end()));
    }
    row.emplace_back(std::move(steps));
    result.emplace_back(std::move(row));
  }
  return result;
}

std::vector<Row> ShortestPathExecutor::createRightPath(Value& meetVid, bool oddStep) {
  std::vector<Row> rightPaths;
  std::vector<Row> interimPaths({meetVid});

  auto stepIter = oddStep ? allRightSteps_.rbegin() + 1 : allRightSteps_.rbegin();
  for (; stepIter < allRightSteps_.rend() - 1; ++stepIter) {
    std::vector<Row> temp;
    for (auto& interimPath : interimPaths) {
      Value id = meetVid;
      if (interimPath.size() != 1) {
        auto& row = interimPath.back();
        id = row.values.front().getVertex().vid;
      }
      auto findStep = stepIter.find(id);
      for (auto step : findStep.second) {
        Row path = interimPath;
        path.emplace_back(step);
        if (stepIter == allRightSteps_.rend() - 2) {
          rightPaths.emplace_back(std::move(path));
        } else {
          temp.emplace_back(std::move(path));
        }
      }  //  step
    }    //  interimPath
    if (stepIter != allRightSteps_.rend() - 2) {
      interimPaths.swap(temp);
    }
  }
  Value meetVertex;
  if (oddStep) {
    auto& lastSteps = allRightSteps_.back();
    for (auto& steps : lastSteps) {
      bool flag = false;
      for (auto& step : steps->second) {
        auto& vertex = step.values.front();
        if (vertex.getVertex().vid == meetVid) {
          meetVertex = vertex;
          flag = true;
          break;
        }
      }
      if (flag) {
        break;
      }
    }
  }
  std::vector<Row> result;
  result.reserve(rightPaths.size());
  for (auto& path : rightPaths) {
    Row row;
    if (oddStep) {
      row.emplace_back(meetVertex);
    }
    path.values.erase(path.values.begin());
    for (auto& edge : path.values) {
      row.values.insert(std::make_move_iterator(edge.values.rbegin()),
                        std::make_move_iterator(edge.values.rend()));
    }
    result.emplace_back(std::move(row));
  }
  return result;
}

void ShortestPathExecutor::buildOddPath(const std::vector<Value>& meetVids) {
  for (auto& meetVid : meetVids) {
    auto leftPaths = createLeftPath(meetVid);
    auto rightPaths = createRightPath(meetVid);
    for (auto& leftPath : leftPaths) {
      for (auto& rightPath : rightPaths) {
        Row path = leftPath;
        auto& steps = path.values.back();
        steps.insert(rightPath.begin(), rightPath.end());
        resultDs_.rows.emplace_back(std::move(path));
      }
    }
  }
}

bool ShortestPathExecutor::buildEvenPath(const std::vector<Value>& meetVids) {
  std::vector<Vertex> meetVertices;
  auto status = getMeetVidsProps(meetVids, meetVertices).get();
  if (!status.ok() || meetVertices.empty()) {
    return false;
  }
  for (auto& meetVertex : meetVertices) {
    auto leftPaths = createLeftPath(meetVertex.vid);
    auto rightPaths = createRightPath(meetVertx.vid);
    for (auto& leftPath : leftPaths) {
      for (auto& rightPath : rightPaths) {
        Row path = leftPath;
        auto& steps = path.values.back();
        steps.emplace_back(meetVertex);
        steps.insert(rightPath.begin(), rightPath.end());
        resultDs_.rows.emplace_back(std::move(path));
      }
    }
  }
  return true;
}

bool ShortestPathExecutor::conjunctPath() {
  const auto& leftStep = allLeftSteps_.back();
  const auto& prevRightStep = allRightSteps_[step_ - 1];
  std::vector<Value> meetVids;
  for (const auto& step : leftStep) {
    if (prevRightStep.find(step.first) != preRightStep.end()) {
      meetVids.push_back(step.first);
    }
  }
  if (!meetVids.empty()) {
    buildOddPath(meetVids);
    return true;
  }

  const auto& rightStep = allRightSteps_.back();
  for (const auto& step : leftStep) {
    if (rightStep.find(step.first) != rightStep.end()) {
      meetVids.push_back(step.first);
    }
  }
  if (meetVids.empty()) {
    return false;
  }
  return buildEvenPath(meetVids);
}

Status ShortestPathExecutor::doBuildPath(GetNeighborsIter* iter, bool reverse) {
  auto iterSize = iter->size();
  auto& visitedVids = reverse ? rightVisitedVids_ : leftVisitedVids_;
  visitedVids.reserve(visitedVids.size() + iterSize);
  auto& allSteps = reverse ? allRightSteps_ : allLeftSteps_;
  if (reverse&& step_ = 0) {
    for (; iter->valid(); iter->next()) {
      // set oright rightPath_;
    }
  }
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
      std::vector<Row> steps(std::move(step));
      currentStep.emplace(std::move(dst), std::move(steps));
    } else {
      auto& steps = findDst->second;
      steps.emplace_back(std::move(steps));
    }
  }
  visitedVids.insert(std::make_move_iterator(uniqueDst.begin()),
                     std::make_move_iterator(uniqueDst.end()));
  if (reverse) {
    rightVids_.rows.swap(nextStepVids);
  } else {
    leftVids_.rows.swap(nextStepVids);
  }
  return Status::OK();
}

Status ShortestPathExecutor::buildPath(RpcResponse& resps, bool reverse) {
  auto result = handleCompleteness(resps, FLAGS_accept_partial_success);
  NG_RETURN_IF_ERROR(result);
  auto& responses = resps.responses();
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
  return doBuildPath(iter.get(), reverse);
}

folly::Future<Status> ShortestPathExecutor::handleResponse(std::vector<RpcResponse>&& resps,
                                                           size_t i) {
  NG_RETURN_IF_ERROR(buildPath(resps.front()));
  NG_RETURN_IF_ERROR(buildPath(resps.back()));
  if (conjunctPath()) {
    if (i < cartesianProduct_.size()) {
      leftVids_.rows.swap({Row({std::move(cartesianProduct_[i + 1].first)})});
      rightVids_.rows.swap({Row({std::move(cartesianProduct_[i + 1].second)})});
    }
    leftVisitedVids_.clear();
    rightVisitedVids_.clear();
    allLeftSteps_.clear();
    allRightSteps_.clear();
    return Status::OK();
  }
  step_++;
  if (step_ > 10) {
    return Status::OK();
  }
  return shortestPath(i);
}

folly::Future<GetNeighborsResponse> ShortestPathExecutor::getNeighbors(bool reverse) {
  StorageClient* storageClient = qctx_->getStorageClient();
  storage::StorageClient::CommonRequestParam param(pathNode_->space(),
                                                   qctx()->rctx()->session()->id(),
                                                   qctx()->plan()->id(),
                                                   qctx()->plan()->isProfileEnabled());
  return storageClient->getNeighbors(
      param,
      {nebula::kVid},
      reverse ? std::move(rightVids_.rows) : std::move(leftVids_.rows),
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
      nullptr);
}

folly::Future<Status> ShortestPathExecutor::shortestPath(size_t i) {
  std::vector<GetNeighborsResponse> futures;
  futures.emplace_back(getNeighbors(false));
  futures.emplace_back(getNeighbors(true));
  return folly::collect(futures).via(runner()).thenValue(
      [](std::vector<RpcResponse>&& resps) { return handleResponse(resps, i); });
}

folly::Future<Status> ShortestPathExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  NG_RETURN_IF_ERROR(buildRequestDataSet());
  std::vector<folly::Future<Status>> futures;
  for (size_t i = 0; i < cartesianProduct_.size(); ++i) {
    futures.emplace_back(shortestPath(i));
  }
  folly::collect(futures).via(runner()).thenValue(
      []() { return finish(ResultBuilder().value(Value(std::move(resultDs_))).build()); });
}

}  // namespace graph
}  // namespace nebula
