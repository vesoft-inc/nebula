/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/ListTagIndexStatusProcessor.h"

#include <folly/String.h>                   // for join
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <algorithm>      // for max, sort
#include <memory>         // for allocator, unique_ptr
#include <ostream>        // for operator<<, basic_os...
#include <string>         // for basic_string, string
#include <type_traits>    // for remove_reference<>::...
#include <unordered_map>  // for unordered_map, _Node...
#include <vector>         // for vector

#include "common/base/ErrorOr.h"                 // for ok, value, error
#include "common/base/Logging.h"                 // for Check_GEImpl, DCHECK_GE
#include "common/utils/MetaKeyUtils.h"           // for kDefaultPartId, kDef...
#include "interface/gen-cpp2/common_types.h"     // for ErrorCode, ErrorCode...
#include "kvstore/KVIterator.h"                  // for KVIterator
#include "kvstore/KVStore.h"                     // for KVStore
#include "meta/processors/BaseProcessor.h"       // for BaseProcessor::getSp...
#include "meta/processors/job/JobDescription.h"  // for JobDescription
#include "meta/processors/job/JobUtils.h"        // for JobUtil

namespace nebula {
namespace meta {

void ListTagIndexStatusProcessor::process(const cpp2::ListIndexStatusReq& req) {
  auto curSpaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(curSpaceId);
  std::unique_ptr<kvstore::KVIterator> iter;
  auto retCode = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, JobUtil::jobPrefix(), &iter);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Loading Job Failed" << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  std::vector<cpp2::JobDesc> jobs;
  std::vector<cpp2::IndexStatus> statuses;
  for (; iter->valid(); iter->next()) {
    if (JobDescription::isJobKey(iter->key())) {
      auto optJobRet = JobDescription::makeJobDescription(iter->key(), iter->val());
      if (!nebula::ok(optJobRet)) {
        continue;
      }

      auto optJob = nebula::value(optJobRet);
      auto jobDesc = optJob.toJobDesc();
      if (jobDesc.get_cmd() == cpp2::AdminCmd::REBUILD_TAG_INDEX) {
        auto paras = jobDesc.get_paras();
        DCHECK_GE(paras.size(), 1);
        auto spaceName = paras.back();
        auto ret = getSpaceId(spaceName);
        if (!nebula::ok(ret)) {
          retCode = nebula::error(ret);
          if (retCode == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
            handleErrorCode(retCode);
            onFinished();
            return;
          }
          continue;
        }
        auto spaceId = nebula::value(ret);
        if (spaceId != curSpaceId) {
          continue;
        }

        jobs.emplace_back(jobDesc);
      }
    }
  }
  std::sort(jobs.begin(), jobs.end(), [](const auto& a, const auto& b) {
    return a.get_id() > b.get_id();
  });
  std::unordered_map<std::string, cpp2::JobStatus> tmp;
  for (auto& jobDesc : jobs) {
    auto paras = jobDesc.get_paras();
    if (paras.size() == 1) {
      tmp.emplace(paras[0] + "_all_tag_indexes", jobDesc.get_status());
      continue;
    }
    paras.pop_back();
    tmp.emplace(folly::join(",", paras), jobDesc.get_status());
  }
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
