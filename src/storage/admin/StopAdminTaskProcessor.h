/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_STOPADMINTASKPROCESSOR_H_
#define STORAGE_ADMIN_STOPADMINTASKPROCESSOR_H_

#include <folly/Try.h>              // for Try::~Try<T>
#include <folly/futures/Future.h>   // for SemiFuture::releaseDef...
#include <folly/futures/Future.h>   // for Future
#include <folly/futures/Future.h>   // for SemiFuture::releaseDef...
#include <folly/futures/Future.h>   // for Future
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for Promise, PromiseExcept...
#include <folly/futures/Promise.h>  // for Promise::Promise<T>
#include <folly/futures/Promise.h>  // for Promise, PromiseExcept...

#include "common/base/Base.h"  // for UNUSED
#include "common/thrift/ThriftTypes.h"
#include "interface/gen-cpp2/storage_types.h"  // for StopTaskResp, StopTask...
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {
class StorageEnv;

class StorageEnv;

class StopAdminTaskProcessor {
 public:
  static StopAdminTaskProcessor* instance(StorageEnv* env) {
    return new StopAdminTaskProcessor(env);
  }

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
