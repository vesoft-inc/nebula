/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_GETEDGEANNINDEXPROCESSOR_H
#define META_GETEDGEANNINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Get edge ann index item from meta kv store, including two steps:
 *        1. Get index id by space id and index name.
 *        2. Get edge ann index item by space id and index id.
 *
 */
class GetEdgeAnnIndexProcessor : public BaseProcessor<cpp2::GetEdgeAnnIndexResp> {
 public:
  static GetEdgeAnnIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new GetEdgeAnnIndexProcessor(kvstore);
  }

  void process(const cpp2::GetEdgeIndexReq& req);

 private:
  explicit GetEdgeAnnIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::GetEdgeAnnIndexResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETEDGEINDEXPROCESSOR_H
