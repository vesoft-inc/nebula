/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_ADMINTASKPROCESSOR_H_
#define STORAGE_ADMIN_ADMINTASKPROCESSOR_H_

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class AdminTaskProcessor {
 public:
  /**
   * @brief Construct new instance of the admin task processor.
   *
   * @param env Related environment variables for storage.
   * @return AdminTaskProcessor* AdminTaskProcessor instance.
   */
  static AdminTaskProcessor* instance(StorageEnv* env) {
    return new AdminTaskProcessor(env);
  }

  /**
   * @brief Entry point for admin task logic.
   *
   * @param req Request for admin task.
   */
  void process(const cpp2::AddTaskRequest& req);

  folly::Future<cpp2::AddTaskResp> getFuture() {
    return promise_.getFuture();
  }

 private:
  explicit AdminTaskProcessor(StorageEnv* env) : env_(env) {}

  void onFinished();

  StorageEnv* env_{nullptr};
  folly::Promise<cpp2::AddTaskResp> promise_;
  cpp2::AddTaskResp resp_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_ADMINTASKPROCESSOR_H_
