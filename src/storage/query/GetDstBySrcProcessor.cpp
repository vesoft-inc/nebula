/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/query/GetDstBySrcProcessor.h"

#include "storage/exec/EdgeNode.h"
#include "storage/exec/GetDstBySrcNode.h"

namespace nebula {
namespace storage {

ProcessorCounters kGetDstBySrcCounters;

void GetDstBySrcProcessor::process(const cpp2::GetDstBySrcRequest& req) {
  if (executor_ != nullptr) {
    executor_->add([req, this]() { this->doProcess(req); });
  } else {
    doProcess(req);
  }
}

void GetDstBySrcProcessor::doProcess(const cpp2::GetDstBySrcRequest& req) {
  spaceId_ = req.get_space_id();
  auto retCode = getSpaceVidLen(spaceId_);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    for (auto& p : req.get_parts()) {
      pushResultCode(retCode, p.first);
    }
    onFinished();
    return;
  }
  this->planContext_ = std::make_unique<PlanContext>(
      this->env_, spaceId_, this->spaceVidLen_, this->isIntId_, req.common_ref());

  // check edgetypes exists
  retCode = checkAndBuildContexts(req);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    for (auto& p : req.get_parts()) {
      pushResultCode(retCode, p.first);
    }
    onFinished();
    return;
  }

  if (!FLAGS_query_concurrently) {
    runInSingleThread(req);
  } else {
    runInMultipleThread(req);
  }
}

void GetDstBySrcProcessor::runInSingleThread(const cpp2::GetDstBySrcRequest& req) {
  contexts_.emplace_back(RuntimeContext(planContext_.get()));
  auto plan = buildPlan(&contexts_.front(), &resultDataSet_);
  std::unordered_set<PartitionID> failedParts;
  for (const auto& partEntry : req.get_parts()) {
    auto partId = partEntry.first;
    for (const auto& src : partEntry.second) {
      auto vId = src.getStr();

      if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vId)) {
        LOG(INFO) << "Space " << spaceId_ << ", vertex length invalid, "
                  << " space vid len: " << spaceVidLen_ << ",  vid is " << vId;
        pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_VID, partId);
        onFinished();
        return;
      }

      // the first column of each row would be the vertex id
      auto ret = plan.go(partId, vId);
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        if (failedParts.find(partId) == failedParts.end()) {
          failedParts.emplace(partId);
          handleErrorCode(ret, spaceId_, partId);
        }
      }
    }
  }
  onProcessFinished();
  onFinished();
}

void GetDstBySrcProcessor::runInMultipleThread(const cpp2::GetDstBySrcRequest& req) {
  for (size_t i = 0; i < req.get_parts().size(); i++) {
    nebula::DataSet result = resultDataSet_;
    partResults_.emplace_back(std::move(result));
    contexts_.emplace_back(RuntimeContext(planContext_.get()));
  }
  size_t i = 0;
  std::vector<folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>>> futures;
  for (const auto& [partId, srcIds] : req.get_parts()) {
    futures.emplace_back(runInExecutor(&contexts_[i], &partResults_[i], partId, srcIds));
    i++;
  }

  folly::collectAll(futures).via(executor_).thenTry([this](auto&& t) mutable {
    CHECK(!t.hasException());
    const auto& tries = t.value();
    for (size_t j = 0; j < tries.size(); j++) {
      CHECK(!tries[j].hasException());
      const auto& [code, partId] = tries[j].value();
      if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(code, spaceId_, partId);
      } else {
        resultDataSet_.append(std::move(partResults_[j]));
      }
    }
    this->onProcessFinished();
    this->onFinished();
  });
}

folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>> GetDstBySrcProcessor::runInExecutor(
    RuntimeContext* context,
    nebula::DataSet* result,
    PartitionID partId,
    const std::vector<Value>& srcIds) {
  return folly::via(executor_, [this, context, result, partId, input = std::move(srcIds)]() {
    auto plan = buildPlan(context, result);
    for (const auto& src : input) {
      auto vId = src.getStr();

      if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vId)) {
        LOG(INFO) << "Space " << spaceId_ << ", vertex length invalid, "
                  << " space vid len: " << spaceVidLen_ << ",  vid is " << vId;
        return std::make_pair(nebula::cpp2::ErrorCode::E_INVALID_VID, partId);
      }

      // the first column of each row would be the vertex id
      auto ret = plan.go(partId, vId);
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return std::make_pair(ret, partId);
      }
    }
    return std::make_pair(nebula::cpp2::ErrorCode::SUCCEEDED, partId);
  });
}

StoragePlan<VertexID> GetDstBySrcProcessor::buildPlan(RuntimeContext* context,
                                                      nebula::DataSet* result) {
  /*
    The StoragePlan looks like this:
        +------------------+
        |  GetDstBySrcNode |
        +--------+---------+
                 |
        +--------+---------+
        |     EdgeNodes    |
        +--------+---------+
  */
  StoragePlan<VertexID> plan;
  std::vector<SingleEdgeNode*> edges;
  for (const auto& ec : edgeContext_.propContexts_) {
    auto edge = std::make_unique<SingleEdgeNode>(context, &edgeContext_, ec.first, &ec.second);
    edges.emplace_back(edge.get());
    plan.addNode(std::move(edge));
  }

  auto output = std::make_unique<GetDstBySrcNode>(context, edges, &edgeContext_, result);
  for (auto* edge : edges) {
    output->addDependency(edge);
  }
  plan.addNode(std::move(output));
  return plan;
}

nebula::cpp2::ErrorCode GetDstBySrcProcessor::checkAndBuildContexts(
    const cpp2::GetDstBySrcRequest& req) {
  resultDataSet_.colNames.emplace_back("_dst");
  auto code = getSpaceEdgeSchema();
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return code;
  }
  code = buildEdgeContext(req);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return code;
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode GetDstBySrcProcessor::buildEdgeContext(
    const cpp2::GetDstBySrcRequest& req) {
  // Offset is not used in this processor, just set to a dummy value
  static constexpr size_t kDstOffsetInRespDataSet = 0;
  edgeContext_.offset_ = kDstOffsetInRespDataSet;
  const auto& edgeTypes = req.get_edge_types();
  for (const auto& edgeType : edgeTypes) {
    auto iter = edgeContext_.schemas_.find(std::abs(edgeType));
    if (iter == edgeContext_.schemas_.end()) {
      VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
      return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
    }
    CHECK(!iter->second.empty());
    auto edgeName = this->env_->schemaMan_->toEdgeName(spaceId_, std::abs(edgeType));
    if (!edgeName.ok()) {
      VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
      return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
    }
    std::vector<PropContext> ctxs;
    // Only need to return dst of the edge
    addReturnPropContext(ctxs, kDst, nullptr);
    edgeContext_.propContexts_.emplace_back(edgeType, std::move(ctxs));
    edgeContext_.indexMap_.emplace(edgeType, kDstOffsetInRespDataSet);
    edgeContext_.edgeNames_.emplace(edgeType, std::move(edgeName).value());
  }
  buildEdgeTTLInfo();
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

void GetDstBySrcProcessor::onProcessFinished() {
  // dedup the dsts before we return
  std::unordered_set<const Row*> unique;
  for (const auto& val : resultDataSet_.rows) {
    unique.emplace(&val);
  }
  std::vector<Row> deduped;
  for (const auto& row : unique) {
    deduped.emplace_back(*row);
  }
  resultDataSet_.rows = std::move(deduped);
  resp_.dsts_ref() = std::move(resultDataSet_);
}

}  // namespace storage
}  // namespace nebula
