/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_LISTHOSTSPROCESSOR_H_
#define META_LISTHOSTSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Used for command `show hosts`, it have two actions:
 *        1. If role is not provided, it will show all storaged hosts with part and leader
 *           distribution.
 *        2. If role is provided, it only show all hosts' status with given role.
 */
class ListHostsProcessor : public BaseProcessor<cpp2::ListHostsResp> {
 public:
  static ListHostsProcessor* instance(kvstore::KVStore* kvstore) {
    return new ListHostsProcessor(kvstore);
  }

  void process(const cpp2::ListHostsReq& req);

 private:
  explicit ListHostsProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ListHostsResp>(kvstore) {}

  /**
   * @brief Get all hosts with status online/offline, gitInfoSHA for the specific HostRole
   *
   * @param type graph/meta/storage
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode allHostsWithStatus(cpp2::HostRole type);

  /**
   * @brief Fill the hostItems_ with leader distribution
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode fillLeaders();

  /**
   * @brief Fill the hostItems_ with parts distribution
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode fillAllParts();

  /**
   * @brief Now(2020-04-29), assume all metad have same gitInfoSHA
   *        this will change if some day
   *        meta.thrift support interface like getHostStatus()
   *        which return a bunch of host information
   *        it's not necessary add this interface only for gitInfoSHA
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode allMetaHostsStatus();

  /**
   * @brief Get map of spaceId -> spaceName for all spaces
   *
   * @return nebula::cpp2::ErrorCode
   */
  nebula::cpp2::ErrorCode getSpaceIdNameMap();

  std::unordered_map<std::string, std::vector<PartitionID>> getLeaderPartsWithSpaceName(
      const LeaderParts& leaderParts);

  /**
   * @brief Remove host that long time at OFFLINE status
   *
   * @param removeHostsKey hosts' key to remove
   */
  void removeExpiredHosts(std::vector<std::string>&& removeHostsKey);

  /**
   * @brief Remove leaders which do not exist
   *
   * @param removeLeadersKey
   */
  void removeInvalidLeaders(std::vector<std::string>&& removeLeadersKey);

 private:
  std::vector<GraphSpaceID> spaceIds_;
  std::unordered_map<GraphSpaceID, std::string> spaceIdNameMap_;
  std::vector<cpp2::HostItem> hostItems_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTHOSTSPROCESSOR_H_
