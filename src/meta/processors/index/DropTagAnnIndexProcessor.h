/* Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPTAGANNINDEXPROCESSOR_H
#define META_DROPTAGANNINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Drop the tag ann index, it will drop the index name and index id key for given space.
 *        It will not handle the existing index data.
 *        The index data in storaged will be removed when do compact with
 *        custom compaction filter.
 *
 */
class DropTagAnnIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropTagAnnIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropTagAnnIndexProcessor(kvstore);
  }

  void process(const cpp2::DropTagIndexReq& req);

 private:
  explicit DropTagAnnIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPTAGANNINDEXPROCESSOR_H
