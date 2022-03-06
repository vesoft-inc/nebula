/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_CLEARSPACEPROCESSOR_H_
#define META_CLEARSPACEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

/**
 * @brief Clear space data and index data, but keep space schema and index schema.
 */
class ClearSpaceProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static ClearSpaceProcessor* instance(kvstore::KVStore* kvstore, AdminClient* adminClient) {
    return new ClearSpaceProcessor(kvstore, adminClient);
  }

  void process(const cpp2::ClearSpaceReq& req);

 private:
  explicit ClearSpaceProcessor(kvstore::KVStore* kvstore, AdminClient* adminClient)
      : BaseProcessor<cpp2::ExecResp>(kvstore), adminClient_(adminClient) {}

  /**
   * @brief Get all parts' distribution information.
   *
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::unordered_map<PartitionID,
   *         std::vector<HostAddr>>> map for part id -> peer hosts.
   */
  ErrorOr<nebula::cpp2::ErrorCode, std::unordered_map<PartitionID, std::vector<HostAddr>>>
  getAllParts(GraphSpaceID spaceId);

 private:
  AdminClient* adminClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CLEARSPACEPROCESSOR_H_
