/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPHOSTSPROCESSOR_H
#define META_DROPHOSTSPROCESSOR_H

#include "kvstore/LogEncoder.h"
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Drop hosts from this cluster
 * The hosts should not hold any parts
 * It will remove the hosts from zones they belong to,
 * and detach the machine from cluster
 */
class DropHostsProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropHostsProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropHostsProcessor(kvstore);
  }

  void process(const cpp2::DropHostsReq& req);

 private:
  explicit DropHostsProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  /**
   * @brief check all spaces to find the zone, and remove it from the space
   *
   * @param zoneName
   * @param holder
   * @return
   */
  nebula::cpp2::ErrorCode checkRelatedSpaceAndCollect(
      const std::string& zoneName, std::map<GraphSpaceID, meta::cpp2::SpaceDesc>* spaceMap);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPHOSTSPROCESSOR_H
