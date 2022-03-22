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
  const auto& srcs = pathNode_->srcs();
  QueryExpressionContext ctx(ectx_);

  std::unordered_set<std::pair<Value, Value>> uniqueSet;
  uniqueSet.reserve(iter->size());
  for (; iter->valid(); iter->next()) {
    auto start = srcs[0]->eval(ctx(iter));
    auto end = srcs[1]->eval(ctx(iter));
    if (!SchemaUtil::isValidVid(start, vidType) || !SchemaUtil::isValidVid(end, vidType)) {
      return Status::Error("Mismatched vid type, space vid type: %s",
                           SchemaUtil::typeToString(vidType));
    }
    if (start == end) {
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
          leftPaths.emplace_back(std::move(path))
        } else {
          temp.emplace_back(std::move(path));
        }
      }  //  step
    }    //  interimPath
    if (stepIter != allLeftSteps_.rend() - 1) {
      interimPaths.swap(temp);
    }
  }
  // reverse leftPath
  return leftPaths;
}

std::vector<Row> ShortestPathExecutor::createRightPath(Value& meetVid, bool evenStep) {
  std::vector<Row> rightPaths;
  std::vector<Row> interimPaths({meetVid});

  for (; stepIter < allRightSteps_.rend() - 1; ++stepIter) {
    std::vector<Row> temp;
    for (auto& iterimPath : interimPaths) {
      Value id = meetVid;
      if (iterimPath.size() != 1) {
        auto& row = interimPath.back();
        id = row.values.front().getVertex().vid;
      }
      auto findStep = stepIter.find(id);
      for (auto step : findStep.second) {
        Row path = iterimPath;
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
  // process
  return rightPaths;
}

void ShortestPathExecutor::buildOddPath(const std::vector<Value>& meetVids) {
  for (auto& meetVid : meetVids) {
    // create leftPath
    auto leftPaths = createLeftPath(meetVid);
    auto rightPaths = createRightPath(meetVid);
    for (auto& leftPath : leftPaths) {
      for (auto& rightPath : rightPaths) {
      }
    }
    // result = leftPaths + rightPaths
    // save result to resultDs_
  }
}

bool ShortestPathExecutor::buildEvenPath(const std::vector<Value>& meetVids) {
  // getProps then filter
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
  if (!result.ok()) {
    return result.status();
  }
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
