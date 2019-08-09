/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CLUSTERMANAGER_H_
#define META_CLUSTERMANAGER_H_

#include "base/Base.h"
#include "base/ThriftTypes.h"
#include "kvstore/Common.h"

namespace nebula {

namespace kvstore {
class KVStore;
}

namespace meta {

using nebula::ClusterID;
using nebula::kvstore::KVStore;
using nebula::kvstore::ResultCode;

/**
 * This class manages clusterId used for meta server and storage server.
 * */
class ClusterManager {
public:
    ClusterManager(const std::string& clusterHosts,
                   const std::string& clusterIdPath,
                   ClusterID clusterId = 0)
            : clusterHosts_(clusterHosts)
            , clusterIdPath_(clusterIdPath)
            , clusterId_(clusterId) {}

    virtual ~ClusterManager() = default;

    ResultCode kvPut(KVStore* kvstore, std::string& strClusterId);

    bool loadOrCreateCluId(nebula::kvstore::KVStore* kvstore);

    bool loadClusterId();

    bool dumpClusterId();

    ClusterID getClusterId() {
        return clusterId_;
    }

    void setClusterId(ClusterID clusterId) {
        clusterId_ = clusterId;
        clusterIdIsSet_ = true;
    }

    bool isClusterIdSet() {
        return clusterIdIsSet_;
    }

    bool clusterIdDumped() {
        return clusterIdDumped_;
    }

private:
    void createClusterId();

private:
    bool clusterIdIsSet_{false};
    bool clusterIdDumped_{false};
    std::string clusterHosts_;
    std::string clusterIdPath_;
    ClusterID clusterId_{0};
    static const char* kClusterIdKey;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_CLUSTERMANAGER_H_

