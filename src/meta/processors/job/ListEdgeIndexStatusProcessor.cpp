/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/ListEdgeIndexStatusProcessor.h"

#include "meta/processors/job/JobDescription.h"

namespace nebula {
namespace meta {

void ListEdgeIndexStatusProcessor::process(const cpp2::ListIndexStatusReq& req) {
  auto spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto jobPrefix = MetaKeyUtils::jobPrefix(spaceId);
  auto retCode = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, jobPrefix, &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Loading Job Failed" << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  std::vector<cpp2::JobDesc> jobs;
  for (; iter->valid(); iter->next()) {
    if (MetaKeyUtils::isJobKey(iter->key())) {
      auto optJobRet = JobDescription::makeJobDescription(iter->key(), iter->val());
      if (!nebula::ok(optJobRet)) {
        continue;
      }
      auto optJob = nebula::value(optJobRet);
      if (optJob.getJobType() == cpp2::JobType::REBUILD_EDGE_INDEX) {
        auto jobDesc = optJob.toJobDesc();
        jobs.emplace_back(jobDesc);
      }
    }
  }
  std::sort(jobs.begin(), jobs.end(), [](const auto& a, const auto& b) {
    return a.get_job_id() > b.get_job_id();
  });

  std::unordered_map<std::string, cpp2::JobStatus> tmp;
  for (auto& jobDesc : jobs) {
    auto paras = jobDesc.get_paras();
    std::string edgeIndexJobName;
    if (paras.empty()) {
      edgeIndexJobName = "all_edge_indexes";
    } else {
      edgeIndexJobName = folly::join(",", paras);
    }
    tmp.emplace(edgeIndexJobName, jobDesc.get_status());
  }

  std::vector<cpp2::IndexStatus> statuses;
  for (auto& kv : tmp) {
    cpp2::IndexStatus status;
    status.name_ref() = std::move(kv.first);
    status.status_ref() = apache::thrift::util::enumNameSafe(kv.second);
    statuses.emplace_back(std::move(status));
  }
  resp_.statuses_ref() = std::move(statuses);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
