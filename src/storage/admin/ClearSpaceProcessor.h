/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_CLEARSPACEPROCESSOR_H_
#define STORAGE_ADMIN_CLEARSPACEPROCESSOR_H_

#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

/**
 * @brief Processor class to clear space.
 */
class ClearSpaceProcessor {
 public:
  /**
   * @brief Construct a new instance of ClearSpaceProcessor.
   *
   * @param env Related environment variables for storage.
   * @return ClearSpaceProcessor* ClearSpaceProcessor instance.
   */
  static ClearSpaceProcessor* instance(StorageEnv* env) {
    return new ClearSpaceProcessor(env);
  }

  /**
   * @brief Entry point to clear space.
   *
   * @param req Request for clearing space.
   */
  void process(const cpp2::ClearSpaceReq& req);

  folly::Future<cpp2::ClearSpaceResp> getFuture() {
    return promise_.getFuture();
  }

 private:
  explicit ClearSpaceProcessor(StorageEnv* env) : env_(env) {}

  void onFinished();

  StorageEnv* env_{nullptr};  // Related environment variables for storage.
  folly::Promise<cpp2::ClearSpaceResp> promise_;
  cpp2::ClearSpaceResp resp_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_CLEARSPACEPROCESSOR_H_
