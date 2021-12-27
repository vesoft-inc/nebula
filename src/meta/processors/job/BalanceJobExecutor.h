/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_BALANCEJOBEXECUTOR_H_
#define META_BALANCEJOBEXECUTOR_H_

#include "meta/processors/job/BalancePlan.h"
#include "meta/processors/job/BalanceTask.h"
#include "meta/processors/job/MetaJobExecutor.h"
#include "meta/processors/job/SimpleConcurrentJobExecutor.h"

namespace nebula {
namespace meta {
using HostParts = std::unordered_map<HostAddr, std::vector<PartitionID>>;
using LeaderBalancePlan = std::vector<std::tuple<GraphSpaceID, PartitionID, HostAddr, HostAddr>>;

struct Host {
  explicit Host(const HostAddr& ha) : host_(ha) {}
  Host() = default;

  HostAddr host_;
  std::set<PartitionID> parts_;
};

struct Zone {
  Zone() = default;
  explicit Zone(const std::string name) : zoneName_(name) {}
  bool hasHost(const HostAddr& ha) {
    return hosts_.find(ha) != hosts_.end();
  }
  int32_t calPartNum();
  bool partExist(PartitionID partId);

  std::string zoneName_;
  std::map<HostAddr, Host> hosts_;
  int32_t partNum_;
};

struct SpaceInfo {
  nebula::cpp2::ErrorCode loadInfo(GraphSpaceID spaceId, kvstore::KVStore* kvstore);
  bool hasHost(const HostAddr& ha);

  std::string name_;
  GraphSpaceID spaceId_;
  int32_t replica_;
  // zone_name -> zone
  std::map<std::string, Zone> zones_;
};

class BalanceJobExecutor : public MetaJobExecutor {
 public:
  BalanceJobExecutor(JobID jobId,
                     kvstore::KVStore* kvstore,
                     AdminClient* adminClient,
                     const std::vector<std::string>& params);

  bool check() override;

  nebula::cpp2::ErrorCode prepare() override;

  nebula::cpp2::ErrorCode stop() override;

  nebula::cpp2::ErrorCode finish(bool ret = true) override;

  nebula::cpp2::ErrorCode recovery() override;

 protected:
  nebula::cpp2::ErrorCode save(const std::string& k, const std::string& v);

  virtual Status buildBalancePlan() {
    return Status::OK();
  }

 protected:
  std::unique_ptr<BalancePlan> plan_;
  std::unique_ptr<folly::Executor> executor_;
  SpaceInfo spaceInfo_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_BALANCEJOBEXECUTOR_H_
