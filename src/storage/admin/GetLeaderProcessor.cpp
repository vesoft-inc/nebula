/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "storage/admin/GetLeaderProcessor.h"

#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <unordered_map>  // for unordered_map, _Node_iter...
#include <utility>        // for move, pair
#include <vector>         // for vector

#include "common/base/Base.h"               // for UNUSED
#include "common/base/Logging.h"            // for CheckNotNull, CHECK_NOTNULL
#include "common/thrift/ThriftTypes.h"      // for PartitionID, GraphSpaceID
#include "interface/gen-cpp2/meta_types.h"  // for LeaderInfo
#include "kvstore/KVStore.h"                // for KVStore
#include "storage/CommonUtils.h"            // for StorageEnv

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
