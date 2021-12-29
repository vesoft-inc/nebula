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

class DropHostsProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropHostsProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropHostsProcessor(kvstore);
  }

  void process(const cpp2::DropHostsReq& req);

 private:
  explicit DropHostsProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  nebula::cpp2::ErrorCode checkRelatedSpaceAndCollect(const std::string& zoneName,
                                                      kvstore::BatchHolder* holder);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPHOSTSPROCESSOR_H
