/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DIVIDEZONEPROCESSOR_H
#define META_DIVIDEZONEPROCESSOR_H

#include "kvstore/LogEncoder.h"
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Divide an existing zone to several zones
 */
class DivideZoneProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DivideZoneProcessor* instance(kvstore::KVStore* kvstore) {
    return new DivideZoneProcessor(kvstore);
  }

  void process(const cpp2::DivideZoneReq& req);

 private:
  explicit DivideZoneProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  /**
   * @brief remove the originalZoneName and add the zoneNames in batchHolder
   *
   * @param batchHolder
   * @param originalZoneName
   * @param zoneNames
   * @return
   */
  nebula::cpp2::ErrorCode updateSpacesZone(kvstore::BatchHolder* batchHolder,
                                           const std::string& originalZoneName,
                                           const std::vector<std::string>& zoneNames);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DIVIDEZONEPROCESSOR_H
