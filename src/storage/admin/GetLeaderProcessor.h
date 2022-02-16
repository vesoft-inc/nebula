/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_GETLEADERPROCESSOR_H_
#define STORAGE_ADMIN_GETLEADERPROCESSOR_H_

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
#include "interface/gen-cpp2/storage_types.h"  // for GetLeaderPartsResp
#include "kvstore/NebulaStore.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {
class StorageEnv;

class StorageEnv;

class GetLeaderProcessor {
 public:
  static GetLeaderProcessor* instance(StorageEnv* env) {
    return new GetLeaderProcessor(env);
  }

  void process(const cpp2::GetLeaderReq& req);

  folly::Future<cpp2::GetLeaderPartsResp> getFuture() {
    return promise_.getFuture();
  }

 private:
  explicit GetLeaderProcessor(StorageEnv* env) : env_(env) {}

  void onFinished();

  StorageEnv* env_{nullptr};
  folly::Promise<cpp2::GetLeaderPartsResp> promise_;
  cpp2::GetLeaderPartsResp resp_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_ADMIN_GETLEADERPROCESSOR_H_
