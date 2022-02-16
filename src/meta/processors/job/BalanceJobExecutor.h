/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_BALANCEJOBEXECUTOR_H_
#define META_BALANCEJOBEXECUTOR_H_

#include <folly/Executor.h>  // for Executor
#include <stdint.h>          // for int32_t

#include <map>            // for map, operator!=
#include <memory>         // for unique_ptr
#include <set>            // for set
#include <string>         // for string
#include <tuple>          // for tuple
#include <unordered_map>  // for unordered_map
#include <vector>         // for vector

#include "common/base/Status.h"               // for Status
#include "common/datatypes/HostAddr.h"        // for HostAddr, hash
#include "common/thrift/ThriftTypes.h"        // for PartitionID, GraphS...
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "meta/processors/job/BalancePlan.h"  // for BalancePlan
#include "meta/processors/job/BalanceTask.h"
#include "meta/processors/job/MetaJobExecutor.h"  // for MetaJobExecutor
#include "meta/processors/job/SimpleConcurrentJobExecutor.h"

namespace nebula {
namespace meta {
class AdminClient;
class BalanceTask;
}  // namespace meta

namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {
class AdminClient;
class BalanceTask;

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

  /**
   * @brief Check if this zone contains the host
   *
   * @param ha
   * @return
   */
  bool hasHost(const HostAddr& ha) {
    return hosts_.find(ha) != hosts_.end();
  }

  /**
   * @brief Get part number in the zone
   *
   * @return
   */
  int32_t calPartNum();

  /**
   * @brief Check if the part exists in the zone
   *
   * @param partId
   * @return
   */
  bool partExist(PartitionID partId);

  std::string zoneName_;
  std::map<HostAddr, Host> hosts_;
  int32_t partNum_;
};

struct SpaceInfo {
  /**
   * @brief Load hosts and zones info of this space from kvStore
   *
   * @param spaceId
   * @param kvstore
   * @return
   */
  nebula::cpp2::ErrorCode loadInfo(GraphSpaceID spaceId, kvstore::KVStore* kvstore);

  /**
   * @brief Check if this space contains the host
   *
   * @param ha
   * @return
   */
  bool hasHost(const HostAddr& ha);

  std::string name_;
  GraphSpaceID spaceId_;
  int32_t replica_;
  // zone_name -> zone
  std::map<std::string, Zone> zones_;
};

/**
 * @brief The base class for balance
 */
class BalanceJobExecutor : public MetaJobExecutor {
 public:
  BalanceJobExecutor(JobID jobId,
                     kvstore::KVStore* kvstore,
                     AdminClient* adminClient,
                     const std::vector<std::string>& params);
  /**
   * @brief Check if paras are legal
   *
   * @return
   */
  bool check() override;

  /**
   * @brief See implementation in child class
   *
   * @return
   */
  nebula::cpp2::ErrorCode prepare() override;

  /**
   * @brief Stop this job
   *
   * @return
   */
  nebula::cpp2::ErrorCode stop() override;

  /**
   * @brief Finish this job
   *
   * @param ret True means succeed, false means failed
   * @return
   */
  nebula::cpp2::ErrorCode finish(bool ret = true) override;

  /**
   * @brief Read balance plan and balance tasks from kvStore
   *
   * @return
   */
  nebula::cpp2::ErrorCode recovery() override;

 protected:
  /**
   * @brief Save one kv pair into kvStore
   *
   * @param k key
   * @param v value
   * @return
   */
  nebula::cpp2::ErrorCode save(const std::string& k, const std::string& v);

  /**
   * @brief See implementation in child class
   *
   * @return
   */
  virtual Status buildBalancePlan() {
    return Status::OK();
  }

  void insertOneTask(const BalanceTask& task,
                     std::map<PartitionID, std::vector<BalanceTask>>* existTasks);

 protected:
  std::unique_ptr<BalancePlan> plan_;
  std::unique_ptr<folly::Executor> executor_;
  SpaceInfo spaceInfo_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_BALANCEJOBEXECUTOR_H_
