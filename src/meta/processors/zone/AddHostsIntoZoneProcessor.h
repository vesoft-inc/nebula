/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_ADDHOSTSINTOZONEPROCESSOR_H
#define META_ADDHOSTSINTOZONEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Add hosts into a given zone which is existing or a new one
 * The hosts should be unregistered hosts
 */
class AddHostsIntoZoneProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static AddHostsIntoZoneProcessor* instance(kvstore::KVStore* kvstore) {
    return new AddHostsIntoZoneProcessor(kvstore);
  }

  void process(const cpp2::AddHostsIntoZoneReq& req);

 private:
  explicit AddHostsIntoZoneProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_ADDHOSTSINTOZONEPROCESSOR_H
