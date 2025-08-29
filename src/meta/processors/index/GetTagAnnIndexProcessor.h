/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETTAGANNINDEXPROCESSOR_H
#define META_GETTAGANNINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Get tag ann index item from meta kv store, including two steps:
 *        1. Get index id by space id and index name.
 *        2. Get tag ann index item by space id and index id.
 *
 */
class GetTagAnnIndexProcessor : public BaseProcessor<cpp2::GetTagAnnIndexResp> {
 public:
  static GetTagAnnIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetTagAnnIndexProcessor(kvstore);
  }

  void process(const cpp2::GetTagIndexReq& req);

 private:
  explicit GetTagAnnIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetTagAnnIndexResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETTAGANNINDEXPROCESSOR_H
