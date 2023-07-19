/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETZONEPROCESSOR_H
#define META_GETZONEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Get a zone and its hosts' info
 */
class GetZoneProcessor : public BaseProcessor<cpp2::GetZoneResp> {
 public:
  static GetZoneProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetZoneProcessor(kvstore);
  }

  void process(const cpp2::GetZoneReq& req);

 private:
  explicit GetZoneProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetZoneResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_GETZONEPROCESSOR_H
