/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_GENERALSTORAGESERVICEHANDLER_H_
#define STORAGE_GENERALSTORAGESERVICEHANDLER_H_

#include "common/base/Base.h"
#include "common/interface/gen-cpp2/GeneralStorageService.h"

namespace nebula {
namespace storage {

class StorageEnv;

class GeneralStorageServiceHandler final : public cpp2::GeneralStorageServiceSvIf {
public:
    explicit GeneralStorageServiceHandler(StorageEnv* env);

    folly::Future<cpp2::ExecResponse>
    future_put(const cpp2::KVPutRequest& req) override;

    folly::Future<cpp2::KVGetResponse>
    future_get(const cpp2::KVGetRequest& req) override;

    folly::Future<cpp2::ExecResponse>
    future_remove(const cpp2::KVRemoveRequest& req) override;

private:
    StorageEnv*             env_{nullptr};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_GENERALSTORAGESERVICEHANDLER_H_
