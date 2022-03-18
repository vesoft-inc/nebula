/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/JobExecutor.h"

#include "common/network/NetworkUtils.h"
#include "common/utils/MetaKeyUtils.h"
#include "common/utils/Utils.h"
#include "interface/gen-cpp2/common_types.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/Common.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/job/CompactJobExecutor.h"
#include "meta/processors/job/DataBalanceJobExecutor.h"
#include "meta/processors/job/FlushJobExecutor.h"
#include "meta/processors/job/LeaderBalanceJobExecutor.h"
#include "meta/processors/job/RebuildEdgeJobExecutor.h"
#include "meta/processors/job/RebuildFTJobExecutor.h"
#include "meta/processors/job/RebuildTagJobExecutor.h"
#include "meta/processors/job/StatsJobExecutor.h"
#include "meta/processors/job/StorageJobExecutor.h"
#include "meta/processors/job/TaskDescription.h"
#include "meta/processors/job/ZoneBalanceJobExecutor.h"

DECLARE_int32(heartbeat_interval_secs);
DECLARE_uint32(expired_time_factor);

namespace nebula {
namespace meta {

ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> JobExecutor::getSpaceIdFromName(
    const std::string& spaceName) {
  auto indexKey = MetaKeyUtils::indexSpaceKey(spaceName);
  std::string val;
  auto retCode = kvstore_->get(kDefaultSpaceId, kDefaultPartId, indexKey, &val);
  if (retCode != nebula::cpp2::ErrorCode::SUCCEEDED) {
    LOG(INFO) << "Get space failed, space name: " << spaceName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    return retCode;
  }
  return *reinterpret_cast<const GraphSpaceID*>(val.c_str());
}

std::unique_ptr<JobExecutor> JobExecutorFactory::createJobExecutor(const JobDescription& jd,
                                                                   kvstore::KVStore* store,
                                                                   AdminClient* client) {
  std::unique_ptr<JobExecutor> ret;
  switch (jd.getJobType()) {
    case cpp2::JobType::COMPACT:
      ret.reset(new CompactJobExecutor(jd.getJobId(), store, client, jd.getParas()));
      break;
    case cpp2::JobType::DATA_BALANCE:
      ret.reset(new DataBalanceJobExecutor(jd, store, client, jd.getParas()));
      break;
    case cpp2::JobType::ZONE_BALANCE:
      ret.reset(new ZoneBalanceJobExecutor(jd, store, client, jd.getParas()));
      break;
    case cpp2::JobType::LEADER_BALANCE:
      ret.reset(new LeaderBalanceJobExecutor(jd.getJobId(), store, client, jd.getParas()));
      break;
    case cpp2::JobType::FLUSH:
      ret.reset(new FlushJobExecutor(jd.getJobId(), store, client, jd.getParas()));
      break;
    case cpp2::JobType::REBUILD_TAG_INDEX:
      ret.reset(new RebuildTagJobExecutor(jd.getJobId(), store, client, jd.getParas()));
      break;
    case cpp2::JobType::REBUILD_EDGE_INDEX:
      ret.reset(new RebuildEdgeJobExecutor(jd.getJobId(), store, client, jd.getParas()));
      break;
    case cpp2::JobType::REBUILD_FULLTEXT_INDEX:
      ret.reset(new RebuildFTJobExecutor(jd.getJobId(), store, client, jd.getParas()));
      break;
    case cpp2::JobType::STATS:
      ret.reset(new StatsJobExecutor(jd.getJobId(), store, client, jd.getParas()));
      break;
    default:
      break;
  }
  return ret;
}

}  // namespace meta
}  // namespace nebula
