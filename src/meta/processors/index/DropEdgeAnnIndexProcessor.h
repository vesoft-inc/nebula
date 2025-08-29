/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPEDGEANNINDEXPROCESSOR_H
#define META_DROPEDGEANNINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Drop the edge ann index. It will drop the index name and index id key for given space,
 *        but will not handle the index data. The index data in storaged will be removed when
 *        do compact with custom compaction filter.
 *
 */
class DropEdgeAnnIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropEdgeAnnIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropEdgeAnnIndexProcessor(kvstore);
  }

  void process(const cpp2::DropEdgeIndexReq& req);

 private:
  explicit DropEdgeAnnIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPEDGEANNINDEXPROCESSOR_H
