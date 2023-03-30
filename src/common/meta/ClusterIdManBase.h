/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_META_CLUSTERIDMANBASE_H_
#define COMMON_META_CLUSTERIDMANBASE_H_

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "common/thrift/ThriftTypes.h"

namespace nebula {
namespace meta {

/**
 * This class manages clusterId used for meta server and storage server.
 * */
class ClusterIdManBase {
 public:
  ClusterIdManBase() = delete;

  static ClusterID create(const std::string& metaAddrs) {
    // Generate random salt
    std::random_device rd;
    std::mt19937 gen(rd());
    std::string randomBytes(16, ' ');
    std::generate_n(randomBytes.begin(), 16, std::ref(gen));

    // Concatenate salt with input string
    std::string salted_input = metaAddrs + randomBytes;

    std::hash<std::string> hash_fn;
    auto clusterId = hash_fn(salted_input);
    uint64_t mask = 0x7FFFFFFFFFFFFFFF;
    clusterId &= mask;
    LOG(INFO) << "Create ClusterId " << clusterId;
    return clusterId;
  }
};

}  // namespace meta
}  // namespace nebula
#endif  // COMMON_META_CLUSTERIDMANBASE_H_
