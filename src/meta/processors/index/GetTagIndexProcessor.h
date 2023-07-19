/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETTAGINDEXPROCESSOR_H
#define META_GETTAGINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Get tag index item from meta kv store, including two steps:
 *        1. Get index id by space id and index name.
 *        2. Get tag index item by space id and index id.
 *
 */
class GetTagIndexProcessor : public BaseProcessor<cpp2::GetTagIndexResp> {
 public:
  static GetTagIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetTagIndexProcessor(kvstore);
  }

  void process(const cpp2::GetTagIndexReq& req);

 private:
  explicit GetTagIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetTagIndexResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETTAGINDEXPROCESSOR_H
