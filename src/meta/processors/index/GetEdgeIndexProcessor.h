/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETEDGEINDEXPROCESSOR_H
#define META_GETEDGEINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Get edge index item from meta kv store, including two steps:
 *        1. Get index id by space id and index name.
 *        2. Get edge index item by space id and index id.
 *
 */
class GetEdgeIndexProcessor : public BaseProcessor<cpp2::GetEdgeIndexResp> {
 public:
  static GetEdgeIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetEdgeIndexProcessor(kvstore);
  }

  void process(const cpp2::GetEdgeIndexReq& req);

 private:
  explicit GetEdgeIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetEdgeIndexResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETEDGEINDEXPROCESSOR_H
