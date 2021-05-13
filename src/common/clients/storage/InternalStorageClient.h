/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_CLIENTS_STORAGE_INTERNALSTORAGEClient_H_
#define COMMON_CLIENTS_STORAGE_INTERNALSTORAGEClient_H_

#include <gtest/gtest_prod.h>
#include "common/base/Base.h"
#include "common/base/ErrorOr.h"
#include "common/clients/storage/StorageClientBase.h"
#include "common/interface/gen-cpp2/InternalStorageServiceAsyncClient.h"

namespace nebula {
namespace storage {

typedef ErrorOr<nebula::cpp2::ErrorCode, std::string> ErrOrVal;

/**
 * A wrapper class for InternalStorageServiceAsyncClient thrift API
 *
 * The class is NOT reentrant
 */
class InternalStorageClient : public StorageClientBase<cpp2::InternalStorageServiceAsyncClient> {
    using Parent = StorageClientBase<cpp2::InternalStorageServiceAsyncClient>;

public:
    InternalStorageClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                          meta::MetaClient* metaClient)
        : Parent(ioThreadPool, metaClient) {}
    virtual ~InternalStorageClient() = default;

    folly::SemiFuture<nebula::cpp2::ErrorCode>
    forwardTransaction(int64_t txnId,
                       GraphSpaceID spaceId,
                       PartitionID partId,
                       std::string&& data,
                       folly::EventBase* evb = nullptr);

    folly::SemiFuture<ErrOrVal> getValue(size_t vIdLen,
                                         GraphSpaceID spaceId,
                                         folly::StringPiece key,
                                         folly::EventBase* evb = nullptr);

protected:
    StatusOr<HostAddr> getFuzzyLeader(GraphSpaceID spaceId, PartitionID partId) const;

    void forwardTransactionImpl(int64_t txnId,
                                GraphSpaceID spaceId,
                                PartitionID partId,
                                std::string&& data,
                                folly::Promise<nebula::cpp2::ErrorCode> p,
                                folly::EventBase* evb);

    void getValueImpl(GraphSpaceID spaceId,
                      PartitionID partId,
                      std::string&& key,
                      folly::Promise<ErrOrVal> p,
                      folly::EventBase* evb = nullptr);
};

}   // namespace storage
}   // namespace nebula

#endif   // COMMON_CLIENTS_STORAGE_INTERNALSTORAGEClient_H_
