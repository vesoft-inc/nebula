/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPEDGEINDEXPROCESSOR_H
#define META_DROPEDGEINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Drop the edge index, it will drop the index name and index id key for given space.
 *        The index data in storaged will be remove when do compact with
 *        custom compaction filter.
 *
 */
class DropEdgeIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropEdgeIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropEdgeIndexProcessor(kvstore);
  }

  void process(const cpp2::DropEdgeIndexReq& req);

 private:
  explicit DropEdgeIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPEDGEINDEXPROCESSOR_H
