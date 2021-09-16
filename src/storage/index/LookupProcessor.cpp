/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/index/LookupProcessor.h"

#include "storage/exec/DeDupNode.h"

namespace nebula {
namespace storage {

ProcessorCounters kLookupCounters;

void LookupProcessor::process(const cpp2::LookupIndexRequest& req) {
  if (executor_ != nullptr) {
    executor_->add([req, this]() { this->doProcess(req); });
  } else {
    doProcess(req);
  }
}

void LookupProcessor::doProcess(const cpp2::LookupIndexRequest& req) {
  auto retCode = requestCheck(req);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    for (auto& p : parts_) {
      pushResultCode(retCode, p);
    }
    onFinished();
    return;
  }

  if (isPagingScan_) {
    runPagingScan();
    return;
  }

  // todo(doodle): specify by each query
  if (!FLAGS_query_concurrently) {
    runInSingleThread();
  } else {
    runInMultipleThread();
  }
}

void LookupProcessor::runInSingleThread() {
  filterItems_.emplace_back(IndexFilterItem());
  auto plan = buildPlan(&filterItems_.front(), &resultDataSet_);
  if (!plan.ok()) {
    for (auto& p : parts_) {
      pushResultCode(nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND, p);
    }
    onFinished();
    return;
  }

  std::unordered_set<PartitionID> failedParts;
  for (const auto& partId : parts_) {
    auto ret = plan.value().go(partId);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      if (failedParts.find(partId) == failedParts.end()) {
        failedParts.emplace(partId);
        handleErrorCode(ret, spaceId_, partId);
      }
    }
  }
  if (FLAGS_profile_storage_detail) {
    profilePlan(plan.value());
  }
  onProcessFinished();
  onFinished();
}

void LookupProcessor::runInMultipleThread() {
  // As for lookup, once requestCheck is done, the info in RunTimeContext won't
  // be changed anymore. So we only use one RunTimeContext, could make it per
  // partition later if necessary.
  for (size_t i = 0; i < parts_.size(); i++) {
    nebula::DataSet result = resultDataSet_;
    partResults_.emplace_back(std::move(result));
    filterItems_.emplace_back(IndexFilterItem());
  }
  size_t i = 0;
  std::vector<folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>>> futures;
  for (const auto& partId : parts_) {
    futures.emplace_back(runInExecutor(&filterItems_[i], &partResults_[i], partId));
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
    // when run each part concurrently, we need to dedup again.
    if (!deDupColPos_.empty()) {
      DeDupNode<IndexID>::dedup(resultDataSet_.rows, deDupColPos_);
    }
    this->onProcessFinished();
    this->onFinished();
  });
}

folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>> LookupProcessor::runInExecutor(
    IndexFilterItem* filterItem, nebula::DataSet* result, PartitionID partId) {
  return folly::via(executor_, [this, filterItem, result, partId]() {
    auto plan = buildPlan(filterItem, result);
    if (!plan.ok()) {
      return std::make_pair(nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND, partId);
    }
    auto ret = plan.value().go(partId);
    if (FLAGS_profile_storage_detail) {
      profilePlan(plan.value());
    }
    return std::make_pair(ret, partId);
  });
}

void LookupProcessor::runPagingScan() {
  std::unordered_set<PartitionID> failedParts;
  for (const auto& ctx : pagingScanContexts_) {
    filterItems_.emplace_back(IndexFilterItem());
    auto plan = buildPagingScanPlan(&filterItems_.front(), &resultDataSet_, ctx);
    if (!plan.ok()) {
      pushResultCode(nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND, ctx.get_part());
      onFinished();
      return;
    }
    auto ret = plan.value().go(ctx.get_part());
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
      if (failedParts.find(ctx.get_part()) == failedParts.end()) {
        failedParts.emplace(ctx.get_part());
        handleErrorCode(ret, spaceId_, ctx.get_part());
      }
    }
    if (FLAGS_profile_storage_detail) {
      profilePlan(plan.value());
    }
  }
  if (!deDupColPos_.empty()) {
    DeDupNode<IndexID>::dedup(resultDataSet_.rows, deDupColPos_);
  }
  onProcessFinished();
  onFinished();
}

void LookupProcessor::onProcessFinished() {
  if (context_->isEdge()) {
    std::transform(resultDataSet_.colNames.begin(),
                   resultDataSet_.colNames.end(),
                   resultDataSet_.colNames.begin(),
                   [this](const auto& col) { return context_->edgeName_ + "." + col; });
  } else {
    std::transform(resultDataSet_.colNames.begin(),
                   resultDataSet_.colNames.end(),
                   resultDataSet_.colNames.begin(),
                   [this](const auto& col) { return context_->tagName_ + "." + col; });
  }
  resp_.set_data(std::move(resultDataSet_));

  // set the offset scan contexts for next index scan.
  if (!nextScanContexts_.empty()) {
    resp_.set_next_scan_contexts(std::move(nextScanContexts_));
  }
}

}  // namespace storage
}  // namespace nebula
