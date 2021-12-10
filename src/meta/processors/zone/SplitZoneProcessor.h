/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_SPLITZONEPROCESSOR_H
#define META_SPLITZONEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class SplitZoneProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static SplitZoneProcessor* instance(kvstore::KVStore* kvstore) {
    return new SplitZoneProcessor(kvstore);
  }

  void process(const cpp2::SplitZoneReq& req);

 private:
  explicit SplitZoneProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  nebula::cpp2::ErrorCode updateSpacesZone(std::vector<kvstore::KV>& data,
                                           const std::string& originalZoneName,
                                           const std::string& oneZoneName,
                                           const std::string& anotherZoneName);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SPLITZONEPROCESSOR_H
