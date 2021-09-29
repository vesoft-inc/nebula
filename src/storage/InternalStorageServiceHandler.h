/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_INTERNALSTORAGESERVICEHANDLER_H_
#define STORAGE_INTERNALSTORAGESERVICEHANDLER_H_

#include <folly/executors/IOThreadPoolExecutor.h>

#include "common/base/Base.h"
#include "interface/gen-cpp2/InternalStorageService.h"
#include "storage/CommonUtils.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class StorageEnv;

class InternalStorageServiceHandler final : public cpp2::InternalStorageServiceSvIf {
 public:
  explicit InternalStorageServiceHandler(StorageEnv* env);

  folly::Future<cpp2::ExecResponse> future_chainAddEdges(const cpp2::ChainAddEdgesRequest& p_req);

  folly::Future<cpp2::UpdateResponse> future_chainUpdateEdge(
      const cpp2::ChainUpdateEdgeRequest& p_req);

 private:
  StorageEnv* env_{nullptr};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_INTERNALSTORAGESERVICEHANDLER_H_
