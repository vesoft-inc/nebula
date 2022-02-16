/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_CREATEBACKUPPROCESSOR_H_
#define META_CREATEBACKUPPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promi...
#include <gtest/gtest_prod.h>

#include <string>         // for string
#include <unordered_set>  // for unordered_set
#include <utility>        // for move
#include <vector>         // for vector

#include "common/base/ErrorOr.h"              // for ErrorOr
#include "common/thrift/ThriftTypes.h"        // for GraphSpaceID
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode
#include "interface/gen-cpp2/meta_types.h"    // for CreateBackupResp, Creat...
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {
class AdminClient;
}  // namespace meta

namespace kvstore {
class KVStore;

class KVStore;
}  // namespace kvstore

namespace meta {
class AdminClient;

/**
 * @brief Create backup files in each mtead and storaged services' local.
 *
 */
class CreateBackupProcessor : public BaseProcessor<cpp2::CreateBackupResp> {
 public:
  static CreateBackupProcessor* instance(kvstore::KVStore* kvstore, AdminClient* client) {
    return new CreateBackupProcessor(kvstore, client);
  }

  void process(const cpp2::CreateBackupReq& req);

 private:
  CreateBackupProcessor(kvstore::KVStore* kvstore, AdminClient* client)
      : BaseProcessor<cpp2::CreateBackupResp>(kvstore), client_(client) {}

  nebula::cpp2::ErrorCode cancelWriteBlocking();

  ErrorOr<nebula::cpp2::ErrorCode, std::unordered_set<GraphSpaceID>> spaceNameToId(
      const std::vector<std::string>* backupSpaces);

 private:
  AdminClient* client_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATEBackupPROCESSOR_H_
