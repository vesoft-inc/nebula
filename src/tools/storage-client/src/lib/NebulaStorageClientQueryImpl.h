/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef NEBULA_STORAGE_CLIENT_QUERY_IMPL_H_
#define NEBULA_STORAGE_CLIENT_QUERY_IMPL_H_

#include "tools/storage-client/src/lib/NebulaStorageClientBase.h"

namespace nebula {
class NebulaStorageClientQueryImpl : public NebulaStorageClientBase {
public:
    NebulaStorageClientQueryImpl(storage::StorageClient *storage,
                                 meta::MetaClient *metaClient,
                                 folly::IOThreadPoolExecutor *ioExecutor,
                                 GraphSpaceID spaceId)
        : NebulaStorageClientBase(storage, metaClient, ioExecutor, spaceId) {}

    ResultCode lookup(const LookupContext& context);

    ResultCode getNeighbors(const NeighborsContext& context);

    ResultCode getVertexProps(const VertexPropsContext& context);

    ResultSet resultSet();

protected:
    ResultCode verifyLookupContext(const LookupContext& context);

    ResultCode verifyNeighborsContext(const NeighborsContext& context);

    ResultCode verifyVertexPropsContext(const VertexPropsContext& context);

private:
    ResultCode verifyLookupReturnCols(const std::vector<std::string>& cols,
                                      const meta::SchemaProviderIf *schema,
                                      bool isEdge);

    ResultCode verifyLookupFilter(const std::string& filter);

    ResultCode lookupFilterCheck(const Expression *expr);

    ResultCode verifyEdgeTypes(const std::vector<std::string>& edges);

    ResultCode verifyReturns(const std::vector<PropDef>& props);

    ResultCode verifyNeighborsFilter(const std::string& filter);

    void clearQueryEnv();

private:
    ResultSet                                   resultSet_;
    IndexID                                     indexId_;
    std::vector<EdgeType>                       edgeTypes_;
    std::vector<storage::cpp2::PropDef>         props_;
    std::string                                 filter_;
};

}   // namespace nebula

#endif   // NEBULA_STORAGE_CLIENT_QUERY_IMPL_H_
