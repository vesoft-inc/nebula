/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/query/ScanEdgeProcessor.h"

#include "common/utils/NebulaKeyUtils.h"
#include "storage/StorageFlags.h"
#include "storage/exec/QueryUtils.h"

namespace nebula {
namespace storage {

ProcessorCounters kScanEdgeCounters;

void ScanEdgeProcessor::process(const cpp2::ScanEdgeRequest& req) {
  if (executor_ != nullptr) {
    executor_->add([req, this]() { this->doProcess(req); });
  } else {
    doProcess(req);
  }
}

void ScanEdgeProcessor::doProcess(const cpp2::ScanEdgeRequest& req) {
  spaceId_ = req.get_space_id();
  enableReadFollower_ = req.get_enable_read_from_follower();
  // Negative means no limit
  limit_ = req.get_limit() < 0 ? std::numeric_limits<int64_t>::max() : req.get_limit();

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

nebula::cpp2::ErrorCode ScanEdgeProcessor::checkAndBuildContexts(const cpp2::ScanEdgeRequest& req) {
  auto ret = getSpaceEdgeSchema();
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return ret;
  }

  std::vector<cpp2::EdgeProp> returnProps = *req.return_columns_ref();
  ret = handleEdgeProps(returnProps);
  buildEdgeColName(returnProps);
  ret = buildFilter(req, [](const cpp2::ScanEdgeRequest& r) -> const std::string* {
    if (r.filter_ref().has_value()) {
      return r.get_filter();
    } else {
      return nullptr;
    }
  });
  return ret;
}

void ScanEdgeProcessor::buildEdgeColName(const std::vector<cpp2::EdgeProp>& edgeProps) {
  for (const auto& edgeProp : edgeProps) {
    auto edgeType = edgeProp.get_type();
    auto edgeName = edgeContext_.edgeNames_[edgeType];
    for (const auto& prop : *edgeProp.props_ref()) {
      resultDataSet_.colNames.emplace_back(edgeName + "." + prop);
    }
  }
}

void ScanEdgeProcessor::onProcessFinished() {
  resp_.props_ref() = std::move(resultDataSet_);
  resp_.cursors_ref() = std::move(cursors_);
}

StoragePlan<Cursor> ScanEdgeProcessor::buildPlan(
    RuntimeContext* context,
    nebula::DataSet* result,
    std::unordered_map<PartitionID, cpp2::ScanCursor>* cursors,
    StorageExpressionContext* expCtx) {
  StoragePlan<Cursor> plan;
  std::vector<std::unique_ptr<FetchEdgeNode>> edges;
  for (const auto& ec : edgeContext_.propContexts_) {
    edges.emplace_back(
        std::make_unique<FetchEdgeNode>(context, &edgeContext_, ec.first, &ec.second));
  }
  auto output = std::make_unique<ScanEdgePropNode>(
      context, std::move(edges), enableReadFollower_, limit_, cursors, result, expCtx, filter_);

  plan.addNode(std::move(output));
  return plan;
}

folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>> ScanEdgeProcessor::runInExecutor(
    RuntimeContext* context,
    nebula::DataSet* result,
    std::unordered_map<PartitionID, cpp2::ScanCursor>* cursors,
    PartitionID partId,
    Cursor cursor,
    StorageExpressionContext* expCtx) {
  return folly::via(executor_,
                    [this, context, result, cursors, partId, input = std::move(cursor), expCtx]() {
                      auto plan = buildPlan(context, result, cursors, expCtx);

                      auto ret = plan.go(partId, input);
                      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                        return std::make_pair(ret, partId);
                      }
                      return std::make_pair(nebula::cpp2::ErrorCode::SUCCEEDED, partId);
                    });
}

void ScanEdgeProcessor::runInSingleThread(const cpp2::ScanEdgeRequest& req) {
  contexts_.emplace_back(RuntimeContext(planContext_.get()));
  expCtxs_.emplace_back(StorageExpressionContext(spaceVidLen_, isIntId_));
  std::unordered_set<PartitionID> failedParts;
  auto plan = buildPlan(&contexts_.front(), &resultDataSet_, &cursors_, &expCtxs_.front());
  for (const auto& partEntry : req.get_parts()) {
    auto partId = partEntry.first;
    auto cursor = partEntry.second;

    auto ret = plan.go(
        partId, cursor.next_cursor_ref().has_value() ? cursor.next_cursor_ref().value() : "");
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED &&
        failedParts.find(partId) == failedParts.end()) {
      failedParts.emplace(partId);
      handleErrorCode(ret, spaceId_, partId);
    }
  }
  onProcessFinished();
  onFinished();
}

void ScanEdgeProcessor::runInMultipleThread(const cpp2::ScanEdgeRequest& req) {
  cursorsOfPart_.resize(req.get_parts().size());
  for (size_t i = 0; i < req.get_parts().size(); i++) {
    nebula::DataSet result = resultDataSet_;
    results_.emplace_back(std::move(result));
    contexts_.emplace_back(RuntimeContext(planContext_.get()));
    expCtxs_.emplace_back(StorageExpressionContext(spaceVidLen_, isIntId_));
  }
  size_t i = 0;
  std::vector<folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>>> futures;
  for (const auto& [partId, cursor] : req.get_parts()) {
    futures.emplace_back(
        runInExecutor(&contexts_[i],
                      &results_[i],
                      &cursorsOfPart_[i],
                      partId,
                      cursor.next_cursor_ref().has_value() ? cursor.next_cursor_ref().value() : "",
                      &expCtxs_[i]));
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
        resultDataSet_.append(std::move(results_[j]));
        cursors_.merge(std::move(cursorsOfPart_[j]));
      }
    }
    this->onProcessFinished();
    this->onFinished();
  });
}

}  // namespace storage
}  // namespace nebula
