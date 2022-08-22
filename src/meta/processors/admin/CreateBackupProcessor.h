/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_CREATEBACKUPPROCESSOR_H_
#define META_CREATEBACKUPPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

/**
 * @brief Create backup files in each mtead and storaged services' local.
 *
 */
class CreateBackupProcessor : public BaseProcessor<cpp2::CreateBackupResp> {
 public:
  static CreateBackupProcessor* instance(kvstore::KVStore* kvstore,
                                         AdminClient* client,
                                         ClusterID clusterId = 0) {
    return new CreateBackupProcessor(kvstore, client, clusterId);
  }

  void process(const cpp2::CreateBackupReq& req);

 private:
  CreateBackupProcessor(kvstore::KVStore* kvstore, AdminClient* client, ClusterID clusterId)
      : BaseProcessor<cpp2::CreateBackupResp>(kvstore), client_(client), clusterId_(clusterId) {}

  nebula::cpp2::ErrorCode cancelWriteBlocking();

  ErrorOr<nebula::cpp2::ErrorCode, std::unordered_set<GraphSpaceID>> spaceNameToId(
      const std::vector<std::string>* backupSpaces);

 private:
  AdminClient* client_;

  ClusterID clusterId_{0};
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATEBackupPROCESSOR_H_
