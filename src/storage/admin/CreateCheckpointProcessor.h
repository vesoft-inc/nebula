/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_CREATECHECKPOINTPROCESSOR_H_
#define STORAGE_ADMIN_CREATECHECKPOINTPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Future.h>   // for SemiFuture::releaseDef...
#include <folly/futures/Future.h>   // for Future
#include <folly/futures/Future.h>   // for SemiFuture::releaseDef...
#include <folly/futures/Future.h>   // for Future
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for Promise, PromiseExcept...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for Promise, PromiseExcept...

#include "common/base/Base.h"
#include "interface/gen-cpp2/storage_types.h"  // for CreateCPResp, CreateCP...
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {
class StorageEnv;

class StorageEnv;

class CreateCheckpointProcessor {
 public:
  static CreateCheckpointProcessor* instance(StorageEnv* env) {
    return new CreateCheckpointProcessor(env);
  }

  void process(const cpp2::CreateCPRequest& req);

  folly::Future<cpp2::CreateCPResp> getFuture() {
    return promise_.getFuture();
  }

 private:
  explicit CreateCheckpointProcessor(StorageEnv* env) : env_(env) {}

  void onFinished();

  StorageEnv* env_{nullptr};
  folly::Promise<cpp2::CreateCPResp> promise_;
  cpp2::CreateCPResp resp_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_CREATECHECKPOINTPROCESSOR_H_
