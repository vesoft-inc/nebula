/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPTAGINDEXPROCESSOR_H
#define META_DROPTAGINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Drop the tag index, it will drop the index name and index id key for given space.
 *        It will not handle the existing index data.
 *        The index data in storaged will be removed when do compact with
 *        custom compaction filter.
 *
 */
class DropTagIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropTagIndexProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropTagIndexProcessor(kvstore);
  }

  void process(const cpp2::DropTagIndexReq& req);

 private:
  explicit DropTagIndexProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPTAGINDEXPROCESSOR_H
