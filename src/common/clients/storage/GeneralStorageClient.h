/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_CLIENTS_STORAGE_GENERALSTORAGECLIENT_H_
#define COMMON_CLIENTS_STORAGE_GENERALSTORAGECLIENT_H_

#include "common/base/Base.h"
#include <gtest/gtest_prod.h>
#include "common/interface/gen-cpp2/GeneralStorageServiceAsyncClient.h"
#include "common/datatypes/KeyValue.h"
#include "common/clients/storage/StorageClientBase.h"
#include "common/clients/meta/MetaClient.h"

namespace nebula {
namespace storage {

/**
 * A wrapper class for GeneralStorageServiceAsyncClient thrift API
 *
 * The class is NOT reentrant
 */
class GeneralStorageClient
        : public StorageClientBase<cpp2::GeneralStorageServiceAsyncClient> {
    using Parent = StorageClientBase<cpp2::GeneralStorageServiceAsyncClient>;

public:
    GeneralStorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                         meta::MetaClient* metaClient)
        : Parent(ioThreadPool, metaClient) {}
    virtual ~GeneralStorageClient() {}

    folly::SemiFuture<StorageRpcResponse<cpp2::KVGetResponse>> get(
        GraphSpaceID space,
        std::vector<std::string>&& keys,
        bool returnPartly = false,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> put(
        GraphSpaceID space,
        std::vector<KeyValue> kvs,
        folly::EventBase* evb = nullptr);

    folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> remove(
        GraphSpaceID space,
        std::vector<std::string> keys,
        folly::EventBase* evb = nullptr);
};

}  // namespace storage
}  // namespace nebula
#endif  // COMMON_CLIENTS_STORAGE_GENERALSTORAGECLIENT_H_
