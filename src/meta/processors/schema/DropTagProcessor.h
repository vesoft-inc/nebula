/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPTAGPROCESSOR_H
#define META_DROPTAGPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * @brief Drop tag schema for all versions. The indexes on it should be dropped before drop the
 *        edge.
 *
 */
class DropTagProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropTagProcessor* instance(kvstore::KVStore* kvstore) {
    return new DropTagProcessor(kvstore);
  }

  void process(const cpp2::DropTagReq& req);

 private:
  explicit DropTagProcessor(kvstore::KVStore* kvstore) : BaseProcessor<cpp2::ExecResp>(kvstore) {}

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> getTagKeys(GraphSpaceID id,
                                                                        TagID tagId);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPTAGPROCESSOR_H
