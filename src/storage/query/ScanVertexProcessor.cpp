/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/query/ScanVertexProcessor.h"

#include <limits>

#include "common/utils/NebulaKeyUtils.h"
#include "storage/StorageFlags.h"
#include "storage/exec/QueryUtils.h"

namespace nebula {
namespace storage {

ProcessorCounters kScanVertexCounters;

void ScanVertexProcessor::process(const cpp2::ScanVertexRequest& req) {
  if (executor_ != nullptr) {
    executor_->add([req, this]() { this->doProcess(req); });
  } else {
    doProcess(req);
  }
}

void ScanVertexProcessor::doProcess(const cpp2::ScanVertexRequest& req) {
  spaceId_ = req.get_space_id();
  // negative limit number means no limit
  limit_ = req.get_limit() < 0 ? std::numeric_limits<int64_t>::max() : req.get_limit();
  enableReadFollower_ = req.get_enable_read_from_follower();

  auto retCode = getSpaceVidLen(spaceId_);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    for (const auto& p : req.get_parts()) {
      pushResultCode(retCode, p.first);
    }
    onFinished();
    return;
  }

  this->planContext_ = std::make_unique<PlanContext>(
      this->env_, spaceId_, this->spaceVidLen_, this->isIntId_, req.common_ref());

  retCode = checkAndBuildContexts(req);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    for (const auto& p : req.get_parts()) {
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

nebula::cpp2::ErrorCode ScanVertexProcessor::checkAndBuildContexts(
    const cpp2::ScanVertexRequest& req) {
  auto ret = getSpaceVertexSchema();
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return ret;
  }

  std::vector<cpp2::VertexProp> returnProps = *req.return_columns_ref();
  ret = handleVertexProps(returnProps);
  buildTagColName(returnProps);
  ret = buildFilter(req, [](const cpp2::ScanVertexRequest& r, bool onlyTag) -> const std::string* {
    UNUSED(onlyTag);
    if (r.filter_ref().has_value()) {
      return r.get_filter();
    } else {
      return nullptr;
    }
  });
  buildTagTTLInfo();
  return ret;
}

void ScanVertexProcessor::buildTagColName(const std::vector<cpp2::VertexProp>& tagProps) {
  resultDataSet_.colNames.emplace_back(kVid);
  for (const auto& tagProp : tagProps) {
    auto tagId = tagProp.get_tag();
    auto tagName = tagContext_.tagNames_[tagId];
    for (const auto& prop : *tagProp.props_ref()) {
      resultDataSet_.colNames.emplace_back(tagName + "." + prop);
    }
  }
}

void ScanVertexProcessor::onProcessFinished() {
  resp_.props_ref() = std::move(resultDataSet_);
  resp_.cursors_ref() = std::move(cursors_);
}

StoragePlan<Cursor> ScanVertexProcessor::buildPlan(
    RuntimeContext* context,
    nebula::DataSet* result,
    std::unordered_map<PartitionID, cpp2::ScanCursor>* cursors,
    StorageExpressionContext* expCtx) {
  StoragePlan<Cursor> plan;
  std::vector<std::unique_ptr<TagNode>> tags;
  for (const auto& tc : tagContext_.propContexts_) {
    tags.emplace_back(std::make_unique<TagNode>(context, &tagContext_, tc.first, &tc.second));
  }
  auto output =
      std::make_unique<ScanVertexPropNode>(context,
                                           std::move(tags),
                                           enableReadFollower_,
                                           limit_,
                                           cursors,
                                           result,
                                           expCtx,
                                           filter_ == nullptr ? nullptr : filter_->clone());

  plan.addNode(std::move(output));
  return plan;
}

folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>> ScanVertexProcessor::runInExecutor(
    RuntimeContext* context,
    nebula::DataSet* result,
    std::unordered_map<PartitionID, cpp2::ScanCursor>* cursorsOfPart,
    PartitionID partId,
    Cursor cursor,
    StorageExpressionContext* expCtx) {
  return folly::via(
      executor_,
      [this, context, result, cursorsOfPart, partId, input = std::move(cursor), expCtx]() {
        auto plan = buildPlan(context, result, cursorsOfPart, expCtx);

        auto ret = plan.go(partId, input);
        if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
          return std::make_pair(ret, partId);
        }
        return std::make_pair(nebula::cpp2::ErrorCode::SUCCEEDED, partId);
      });
}

void ScanVertexProcessor::runInSingleThread(const cpp2::ScanVertexRequest& req) {
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

void ScanVertexProcessor::runInMultipleThread(const cpp2::ScanVertexRequest& req) {
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
    size_t sum = 0;
    for (size_t j = 0; j < tries.size(); j++) {
      CHECK(!tries[j].hasException());
      sum += results_[j].size();
    }
    resultDataSet_.rows.reserve(sum);
    for (size_t j = 0; j < tries.size(); j++) {
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
