/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_STOPADMINTASKPROCESSOR_H_
#define STORAGE_ADMIN_STOPADMINTASKPROCESSOR_H_

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class StopAdminTaskProcessor {
 public:
  /**
   * @brief Construct StopAdminTaskProcessor
   *
   * @param env Related environment variables for storage.
   * @return StopAdminTaskProcessor* StopAdminTaskProcessor instance.
   */
  static StopAdminTaskProcessor* instance(StorageEnv* env) {
    return new StopAdminTaskProcessor(env);
  }

  /**
   * @brief Entry point of stopping admin task.
   *
   * @param req Reuqest for stopping admin task.
   */
  void process(const cpp2::StopTaskRequest& req);

  folly::Future<cpp2::StopTaskResp> getFuture() {
    return promise_.getFuture();
  }

 private:
  explicit StopAdminTaskProcessor(StorageEnv* env) : env_(env) {
    UNUSED(env_);
  }

  void onFinished();

  StorageEnv* env_{nullptr};
  folly::Promise<cpp2::StopTaskResp> promise_;
  cpp2::StopTaskResp resp_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_STOPADMINTASKPROCESSOR_H_
