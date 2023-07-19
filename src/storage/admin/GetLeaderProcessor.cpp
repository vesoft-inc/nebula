/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/GetLeaderProcessor.h"

namespace nebula {
namespace storage {

void GetLeaderProcessor::process(const cpp2::GetLeaderReq& req) {
  UNUSED(req);
  CHECK_NOTNULL(env_->kvstore_);
  std::unordered_map<GraphSpaceID, std::vector<meta::cpp2::LeaderInfo>> allLeaders;
  env_->kvstore_->allLeader(allLeaders);
  std::unordered_map<GraphSpaceID, std::vector<PartitionID>> leaderIds;
  for (auto& spaceLeaders : allLeaders) {
    auto& spaceId = spaceLeaders.first;
    for (auto& partLeader : spaceLeaders.second) {
      leaderIds[spaceId].emplace_back(partLeader.get_part_id());
    }
  }
  resp_.leader_parts_ref() = std::move(leaderIds);
  this->onFinished();
}

void GetLeaderProcessor::onFinished() {
  this->promise_.setValue(std::move(resp_));
  delete this;
}

}  // namespace storage
}  // namespace nebula
