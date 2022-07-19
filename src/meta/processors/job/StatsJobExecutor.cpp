/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/StatsJobExecutor.h"

#include "common/utils/MetaKeyUtils.h"
#include "common/utils/Utils.h"
#include "meta/processors/Common.h"

namespace nebula {
namespace meta {

nebula::cpp2::ErrorCode StatsJobExecutor::check() {
  return paras_.empty() ? nebula::cpp2::ErrorCode::SUCCEEDED
                        : nebula::cpp2::ErrorCode::E_INVALID_JOB;
}

nebula::cpp2::ErrorCode StatsJobExecutor::save(const std::string& key, const std::string& val) {
  std::vector<kvstore::KV> data{std::make_pair(key, val)};
  folly::Baton<true, std::atomic> baton;
  auto rc = nebula::cpp2::ErrorCode::SUCCEEDED;
  kvstore_->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        rc = code;
        baton.post();
      });
  baton.wait();
  return rc;
}

nebula::cpp2::ErrorCode StatsJobExecutor::doRemove(const std::string& key) {
  folly::Baton<true, std::atomic> baton;
  auto rc = nebula::cpp2::ErrorCode::SUCCEEDED;
  kvstore_->asyncRemove(kDefaultSpaceId, kDefaultPartId, key, [&](nebula::cpp2::ErrorCode code) {
    rc = code;
    baton.post();
  });
  baton.wait();
  return rc;
}

nebula::cpp2::ErrorCode StatsJobExecutor::prepare() {
  auto spaceRet = spaceExist();
  if (spaceRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Can't find the space, spaceId " << space_;
    return spaceRet;
  }

  // Set the status of the stats job to running
  cpp2::StatsItem statsItem;
  statsItem.status_ref() = cpp2::JobStatus::RUNNING;
  auto statsKey = MetaKeyUtils::statsKey(space_);
  auto statsVal = MetaKeyUtils::statsVal(statsItem);
  return save(statsKey, statsVal);
}

folly::Future<Status> StatsJobExecutor::executeInternal(HostAddr&& address,
                                                        std::vector<PartitionID>&& parts) {
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  adminClient_
      ->addTask(
          cpp2::JobType::STATS, jobId_, taskId_++, space_, std::move(address), {}, std::move(parts))
      .then([pro = std::move(pro)](auto&& t) mutable {
        CHECK(!t.hasException());
        auto status = std::move(t).value();
        if (status.ok()) {
          pro.setValue(Status::OK());
        } else {
          pro.setValue(status.status());
        }
      });
  return f;
}

void showStatsItem(const cpp2::StatsItem& item, const std::string& msg) {
  std::stringstream oss;
  oss << msg << ": ";
  oss << "tag_vertices: ";
  for (auto& it : *item.tag_vertices_ref()) {
    oss << folly::sformat("[{}, {}] ", it.first, it.second);
  }
  oss << ", edges: ";
  for (auto& it : *item.edges_ref()) {
    oss << folly::sformat("[{}, {}] ", it.first, it.second);
  }
  oss << folly::sformat(", space_vertices={}", *item.space_vertices_ref());
  oss << folly::sformat(", space_edges={}", *item.space_edges_ref());
  LOG(INFO) << oss.str();
}

void StatsJobExecutor::addStats(cpp2::StatsItem& lhs, const cpp2::StatsItem& rhs) {
  for (auto& it : *rhs.tag_vertices_ref()) {
    (*lhs.tag_vertices_ref())[it.first] += it.second;
  }

  for (auto& it : *rhs.edges_ref()) {
    (*lhs.edges_ref())[it.first] += it.second;
  }

  *lhs.space_vertices_ref() += *rhs.space_vertices_ref();
  *lhs.space_edges_ref() += *rhs.space_edges_ref();

  (*lhs.positive_part_correlativity_ref())
      .insert((*rhs.positive_part_correlativity_ref()).begin(),  // NOLINT
              (*rhs.positive_part_correlativity_ref()).end());
  (*lhs.negative_part_correlativity_ref())
      .insert((*rhs.negative_part_correlativity_ref()).begin(),  // NOLINT
              (*rhs.negative_part_correlativity_ref()).end());
}

/**
 * @brief caller will guarantee there won't be any conflict read / write.
 */
nebula::cpp2::ErrorCode StatsJobExecutor::saveSpecialTaskStatus(const cpp2::ReportTaskReq& req) {
  if (!req.stats_ref().has_value()) {
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }
  cpp2::StatsItem statsItem;
  auto statsKey = MetaKeyUtils::statsKey(space_);
  auto tempKey = toTempKey(req.get_job_id());
  std::string val;
  auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, tempKey, &val);

  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    if (ret != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      return ret;
    }
    ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, statsKey, &val);
  }

  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return ret;
  }

  statsItem = MetaKeyUtils::parseStatsVal(val);
  addStats(statsItem, *req.stats_ref());
  auto statsVal = MetaKeyUtils::statsVal(statsItem);
  return save(tempKey, statsVal);
}

/**
 * @brief If two stats job run at the same time.
 * This may happens if leader changed. They will write to the same kv data,
 * so separate the partial result by job.
 * @return std::string
 */
std::string StatsJobExecutor::toTempKey(int32_t jobId) {
  std::string key = MetaKeyUtils::statsKey(space_);
  return key.append(reinterpret_cast<const char*>(&jobId), sizeof(int32_t));
}

nebula::cpp2::ErrorCode StatsJobExecutor::finish(bool exeSuccessed) {
  auto statsKey = MetaKeyUtils::statsKey(space_);
  auto tempKey = toTempKey(jobId_);
  std::string val;
  auto ret = kvstore_->get(kDefaultSpaceId, kDefaultPartId, tempKey, &val);
  if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Can't find the stats data, spaceId : " << space_;
    return ret;
  }
  auto statsItem = MetaKeyUtils::parseStatsVal(val);
  if (exeSuccessed) {
    statsItem.status_ref() = cpp2::JobStatus::FINISHED;
  } else {
    statsItem.status_ref() = cpp2::JobStatus::FAILED;
  }
  auto statsVal = MetaKeyUtils::statsVal(statsItem);
  auto retCode = save(statsKey, statsVal);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Sace stats data failed, error " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  return doRemove(tempKey);
}

nebula::cpp2::ErrorCode StatsJobExecutor::stop() {
  auto errOrTargetHost = getTargetHost(space_);
  if (!nebula::ok(errOrTargetHost)) {
    LOG(INFO) << "Get target host failed";
    auto retCode = nebula::error(errOrTargetHost);
    if (retCode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      retCode = nebula::cpp2::ErrorCode::E_NO_HOSTS;
    }
    return retCode;
  }

  auto& hosts = nebula::value(errOrTargetHost);
  std::vector<folly::Future<StatusOr<bool>>> futures;
  for (auto& host : hosts) {
    // Will convert StorageAddr to AdminAddr in AdminClient
    auto future = adminClient_->stopTask(host.first, jobId_, 0);
    futures.emplace_back(std::move(future));
  }

  auto tries = folly::collectAll(std::move(futures)).get();
  if (std::any_of(tries.begin(), tries.end(), [](auto& t) { return t.hasException(); })) {
    LOG(INFO) << "stats job stop() RPC failure.";
    return nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
  }

  for (const auto& t : tries) {
    if (!t.value().ok()) {
      LOG(INFO) << "Stop stats job Failed";
      return nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
