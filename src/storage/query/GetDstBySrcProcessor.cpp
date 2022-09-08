/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/query/GetDstBySrcProcessor.h"

#include <robin_hood.h>

#include "common/thread/GenericThreadPool.h"
#include "storage/exec/EdgeNode.h"
#include "storage/exec/GetDstBySrcNode.h"

DEFINE_uint64(concurrent_dedup_threshold, 50000000, "concurrent dedup threshold");
DEFINE_uint64(max_dedup_threads, 6, "max dedup threads");

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
  auto plan = buildPlan(&contexts_.front(), &flatResult_);
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
    partResults_.emplace_back();
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

    size_t sum = 0;
    for (size_t j = 0; j < tries.size(); j++) {
      const auto& [code, partId] = tries[j].value();
      if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
        sum += partResults_[j].values.size();
      }
    }
    flatResult_.values.reserve(sum);

    for (size_t j = 0; j < tries.size(); j++) {
      DCHECK(!tries[j].hasException());
      const auto& [code, partId] = tries[j].value();
      if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        handleErrorCode(code, spaceId_, partId);
      } else {
        for (auto& v : partResults_[j].values) {
          flatResult_.values.emplace_back(std::move(v));
        }
        std::vector<Value>().swap(partResults_[j].values);
      }
    }

    this->onProcessFinished();
    this->onFinished();
  });
}

folly::Future<std::pair<nebula::cpp2::ErrorCode, PartitionID>> GetDstBySrcProcessor::runInExecutor(
    RuntimeContext* context,
    nebula::List* result,
    PartitionID partId,
    const std::vector<Value>& srcIds) {
  return folly::via(executor_,
                    [this, context, result, partId, input = std::move(srcIds)]() mutable {
                      auto plan = buildPlan(context, result);
                      for (const auto& src : input) {
                        auto& vId = src.getStr();

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
                                                      nebula::List* result) {
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
  static const auto kConcurrentThreshold = FLAGS_concurrent_dedup_threshold;
  static const auto kMaxThreads = FLAGS_max_dedup_threads;
  auto nRows = flatResult_.values.size();
  std::vector<Row> deduped;
  using HashSet = robin_hood::unordered_flat_set<Value, std::hash<Value>>;
  if (nRows < kConcurrentThreshold * 2) {
    HashSet unique;
    unique.reserve(nRows);
    for (const auto& val : flatResult_.values) {
      unique.emplace(val);
    }
    deduped.reserve(unique.size());
    for (const auto& val : unique) {
      deduped.emplace_back(Row({val}));
    }
  } else {
    auto nThreads = nRows / kConcurrentThreshold;
    if (nThreads > kMaxThreads) {
      nThreads = kMaxThreads;
    }
    std::vector<std::pair<size_t, size_t>> ranges;
    auto step = nRows / nThreads;
    auto i = 0UL;
    for (; i < nThreads - 1; i++) {
      ranges.emplace_back(i * step, (i + 1) * step);
    }
    ranges.emplace_back(i * step, nRows);
    nebula::thread::GenericThreadPool pool;
    pool.start(nThreads, "deduper");

    std::vector<HashSet> sets;
    sets.resize(nThreads);
    for (auto& set : sets) {
      set.reserve(step);
    }

    auto cb = [this, &ranges, &sets](size_t idx) {
      auto start = ranges[idx].first;
      auto end = ranges[idx].second;
      for (auto j = start; j < end; j++) {
        sets[idx].emplace(std::move(flatResult_.values[j]));
      }
    };

    std::vector<folly::SemiFuture<folly::Unit>> futures;
    for (i = 0UL; i < nThreads; i++) {
      futures.emplace_back(pool.addTask(cb, i));
    }
    for (auto& future : futures) {
      std::move(future).get();
    }

    for (i = 1UL; i < sets.size(); i++) {
      auto& set = sets[0];
      for (auto& v : sets[i]) {
        set.emplace(std::move(v));
      }
    }

    deduped.reserve(sets[0].size());
    for (auto& v : sets[0]) {
      std::vector<Value> values;
      values.emplace_back(std::move(v));
      deduped.emplace_back(std::move(values));
    }

    pool.stop();
    pool.wait();
  }
  resultDataSet_.rows = std::move(deduped);
  resp_.dsts_ref() = std::move(resultDataSet_);
}

}  // namespace storage
}  // namespace nebula
