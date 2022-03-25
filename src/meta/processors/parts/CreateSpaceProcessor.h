/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_CREATESPACEPROCESSOR_H_
#define META_CREATESPACEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

using Hosts = std::vector<HostAddr>;

/**
 * @brief Create a space:
 *        1. Validate all the given space parameters.
 *        2. Pick a group of hosts for each partition according to the hosts loading.
 *
 */
class CreateSpaceProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static CreateSpaceProcessor* instance(kvstore::KVStore* kvstore) {
    return new CreateSpaceProcessor(kvstore);
  }

  void process(const cpp2::CreateSpaceReq& req);

 private:
  explicit CreateSpaceProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  /**
   * @brief Pick one least load host from each given zone, which is used to lay one partition
   *        replica.
   *        Note that the two pick* functions are for only one partition instead of all partitions.
   *
   * @param zones
   * @param zoneHosts
   * @return StatusOr<Hosts>
   */
  ErrorOr<nebula::cpp2::ErrorCode, Hosts> pickHostsWithZone(
      const std::vector<std::string>& zones,
      const std::unordered_map<std::string, Hosts>& zoneHosts);

  /**
   * @brief Get replica factor count of zones for an partition according to current loading.
   *        We calculate loading only by partition's count now.
   *
   * @param replicaFactor space replicate factor
   * @return StatusOr<std::vector<std::string>>
   */
  StatusOr<std::vector<std::string>> pickLightLoadZones(int32_t replicaFactor);

 private:
  std::unordered_map<std::string, int32_t> zoneLoading_;
  std::unordered_map<HostAddr, int32_t> hostLoading_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATESPACEPROCESSOR_H_
