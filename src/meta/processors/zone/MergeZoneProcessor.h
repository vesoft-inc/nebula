/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_MERGEZONEPROCESSOR_H
#define META_MERGEZONEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class MergeZoneProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static MergeZoneProcessor* instance(kvstore::KVStore* kvstore) {
    return new MergeZoneProcessor(kvstore);
  }

  void process(const cpp2::MergeZoneReq& req);

 private:
  explicit MergeZoneProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_MERGEZONEPROCESSOR_H
