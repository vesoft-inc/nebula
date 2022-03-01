/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef META_PROCESSORS_PARTS_CREATESPACEASPROCESSOR_H
#define META_PROCESSORS_PARTS_CREATESPACEASPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

using Hosts = std::vector<HostAddr>;

/**
 * @brief Create a new space, copying schema and partition topology from an existing space,
 * including:
 *        - space properties
 *        - partition hosts distribution
 *        - tags
 *        - edges
 *        - indexes
 *
 */
class CreateSpaceAsProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static CreateSpaceAsProcessor* instance(kvstore::KVStore* kvstore) {
    return new CreateSpaceAsProcessor(kvstore);
  }

  void process(const cpp2::CreateSpaceAsReq& req);

 protected:
  ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> makeNewSpaceData(
      GraphSpaceID oldSpaceId, GraphSpaceID newSpaceId, const std::string& spaceName);

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> makeNewTags(GraphSpaceID oldSpaceId,
                                                                         GraphSpaceID newSpaceId);

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> makeNewEdges(GraphSpaceID oldSpaceId,
                                                                          GraphSpaceID newSpaceId);

  ErrorOr<nebula::cpp2::ErrorCode, std::vector<kvstore::KV>> makeNewIndexes(
      GraphSpaceID oldSpaceId, GraphSpaceID newSpaceId);

  nebula::cpp2::ErrorCode rc_{nebula::cpp2::ErrorCode::SUCCEEDED};

 private:
  explicit CreateSpaceAsProcessor(kvstore::KVStore* kvstore)
      : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif
