/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_STORAGE_CLIENT_STORAGECLIENTIMPL_H_
#define NEBULA_STORAGE_CLIENT_STORAGECLIENTIMPL_H_

#include <utility>

#include "tools/storage-client/src/NebulaStorageClient.h"
#include "tools/storage-client/src/lib/NebulaStorageClientQueryImpl.h"

namespace nebula {

class NebulaStorageClientImpl {
public:
    explicit NebulaStorageClientImpl(std::string  metaAddr)
        : metaAddr_(std::move(metaAddr)) {
    }
    static ResultCode initArgs(int argc, char** argv);

    static ResultCode setLogging(const std::string& path);

    ResultCode init(const std::string& spaceName, int32_t ioHandlers);

    ResultCode lookup(const LookupContext& input, ResultSet& result);

    ResultCode getNeighbors(const NeighborsContext& input, ResultSet& result);

    ResultCode getVertexProps(const VertexPropsContext& input, ResultSet& result);

private:
    ResultCode initMetaClient(int32_t ioHandlers);

    ResultCode initStorageClient();

private:
    std::string                                   metaAddr_;
    GraphSpaceID                                  spaceId_{-1};
    std::shared_ptr<folly::IOThreadPoolExecutor>  ioExecutor_{nullptr};
    std::unique_ptr<meta::MetaClient>             metaClient_{nullptr};
    std::unique_ptr<storage::StorageClient>       storageClient_{nullptr};
    std::unique_ptr<NebulaStorageClientQueryImpl> queryHandler_{nullptr};
};

}   // namespace nebula


#endif  // NEBULA_STORAGE_CLIENT_STORAGECLIENTIMPL_H_
