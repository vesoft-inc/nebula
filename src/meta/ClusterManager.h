/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CLUSTERMANAGER_H_
#define META_CLUSTERMANAGER_H_

#include "base/Base.h"
#include "base/ThriftTypes.h"

namespace nebula {

namespace kvstore {
class KVStore;
}  // namespace kvstore

namespace meta {

using nebula::ClusterID;

/**
 * This class manages clusterId used for meta server and storage server.
 * */

class ClusterManager {
public:
    ClusterManager(const std::string& clusterHosts,
                   const std::string& clusterIdPath)
            : clusterHosts_(clusterHosts)
            , clusterIdPath_(clusterIdPath) {}

    virtual ~ClusterManager() = default;

    bool loadOrCreateCluId(nebula::kvstore::KVStore* kvstore);

    bool loadClusterId();

    bool dumpClusterId();

    ClusterID getClusterId() {
        return clusterId_;
    }

    void setClusterId(ClusterID clusterId) {
        clusterId_ = clusterId;
        clusterIsSet_ = true;
    }

    bool isClusterIdSet() {
        return clusterIsSet_;
    }

private:
    void createClusterId();

private:
    bool clusterIsSet_{false};
    ClusterID clusterId_{0};
    std::string clusterHosts_;
    std::string clusterIdPath_;
    static const char* kClusterIdKey;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_CLUSTERMANAGER_H_

