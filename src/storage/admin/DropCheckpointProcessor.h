/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_DROPCHECKPOINTPROCESSOR_H_
#define STORAGE_ADMIN_DROPCHECKPOINTPROCESSOR_H_

#include "common/base/Base.h"
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {
class DropCheckpointProcessor {
 public:
  /**
   * @brief Construct new instance of DropCheckpoint.
   *
   * @param env Related environment variables for storage.
   * @return DropCheckpointProcessor* DropCheckpointProcessor instance.
   */
  static DropCheckpointProcessor* instance(StorageEnv* env) {
    return new DropCheckpointProcessor(env);
  }

  /**
   * @brief Entry point for dropping checkpoint.
   *
   * @param req Request for dropping checkpoint.
   */
  void process(const cpp2::DropCPRequest& req);

  folly::Future<cpp2::DropCPResp> getFuture() {
    return promise_.getFuture();
  }

 private:
  explicit DropCheckpointProcessor(StorageEnv* env) : env_(env) {}

  void onFinished();

  StorageEnv* env_{nullptr};
  folly::Promise<cpp2::DropCPResp> promise_;
  cpp2::DropCPResp resp_;
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_DROPCHECKPOINTPROCESSOR_H_
