/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_DROPSNAPSHOTPROCESSOR_H_
#define META_DROPSNAPSHOTPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Promise.h>  // for PromiseException::Promise...
#include <gtest/gtest_prod.h>

#include <utility>  // for move

#include "interface/gen-cpp2/meta_types.h"  // for ExecResp, DropSnapshotReq...
#include "meta/processors/BaseProcessor.h"  // for BaseProcessor
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
 * @brief Drop snapshot for all spaces. It could drop snapshots
 *        created by CreateBackupProcessor or CreateCheckpointProcessor.
 *
 */
class DropSnapshotProcessor : public BaseProcessor<cpp2::ExecResp> {
 public:
  static DropSnapshotProcessor* instance(kvstore::KVStore* kvstore, AdminClient* client) {
    return new DropSnapshotProcessor(kvstore, client);
  }

  void process(const cpp2::DropSnapshotReq& req);

 private:
  DropSnapshotProcessor(kvstore::KVStore* kvstore, AdminClient* client)
      : BaseProcessor<cpp2::ExecResp>(kvstore), client_(client) {}

 private:
  AdminClient* client_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPSNAPSHOTPROCESSOR_H_
