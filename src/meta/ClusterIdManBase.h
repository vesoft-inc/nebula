/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CLUSTERIDMANBASE_H_
#define META_CLUSTERIDMANBASE_H_

#include "base/Base.h"
#include "fs/FileUtils.h"


namespace nebula {
namespace meta {

/**
 * This class manages clusterId used for meta server and storage server.
 * */
class ClusterIdManBase {
public:
    ClusterIdManBase() = delete;

    static ClusterID create(const std::string& metaAddrs) {
        std::hash<std::string> hash_fn;
        auto clusterId = hash_fn(metaAddrs);
        uint64_t mask = 0x7FFFFFFFFFFFFFFF;
        clusterId &= mask;
        LOG(INFO) << "Create ClusterId " << clusterId;
        return clusterId;
    }
};

}  // namespace meta
}  // namespace nebula
#endif  // META_CLUSTERIDMANBASE_H_

