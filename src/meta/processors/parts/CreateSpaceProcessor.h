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
   * @brief Get the host with the least load in the zone
   *
   * @param zones
   * @param zoneHosts
   * @return StatusOr<Hosts>
   */
  StatusOr<Hosts> pickHostsWithZone(const std::vector<std::string>& zones,
                                    const std::unordered_map<std::string, Hosts>& zoneHosts);

  /**
   * @brief Get the zones with the least load
   *
   * @param replicaFactor
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
