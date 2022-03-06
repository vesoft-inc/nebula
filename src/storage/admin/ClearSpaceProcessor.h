/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_CLEARSPACEPROCESSOR_H_
#define STORAGE_ADMIN_CLEARSPACEPROCESSOR_H_

#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

class ClearSpaceProcessor {
 public:
  static ClearSpaceProcessor* instance(StorageEnv* env) {
    return new ClearSpaceProcessor(env);
  }

  void process(const cpp2::ClearSpaceReq& req);

  folly::Future<cpp2::ClearSpaceResp> getFuture() {
    return promise_.getFuture();
  }

 private:
  explicit ClearSpaceProcessor(StorageEnv* env) : env_(env) {}

  void onFinished();

  StorageEnv* env_{nullptr};
  folly::Promise<cpp2::ClearSpaceResp> promise_;
  cpp2::ClearSpaceResp resp_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_CLEARSPACEPROCESSOR_H_
