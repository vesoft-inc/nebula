/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_CLUSTERIDMAN_H_
#define META_CLUSTERIDMAN_H_

#include <folly/synchronization/Baton.h>

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "kvstore/Common.h"
#include "kvstore/KVStore.h"
#include "meta/processors/Common.h"

namespace nebula {
namespace meta {

/**
 * This class manages clusterId used for meta server and storage server.
 * */
class ClusterIdMan {
 public:
  /**
   * @brief Create a ClusterID by hash function
   *
   * @param metaAddrs
   * @return
   */
  static ClusterID create(const std::string& metaAddrs) {
    // Generate random salt
    std::random_device rd;
    std::mt19937 gen(rd());
    std::string randomBytes(16, ' ');
    std::generate_n(randomBytes.begin(), 16, std::ref(gen));

    // Concatenate salt with input string
    std::string saltedInput = metaAddrs + randomBytes;

    std::hash<std::string> hash_fn;
    auto clusterId = hash_fn(saltedInput);
    uint64_t mask = 0x7FFFFFFFFFFFFFFF;
    clusterId &= mask;
    LOG(INFO) << "Create ClusterId " << clusterId;
    return clusterId;
  }

  /**
   * @brief Write the cluster id in a file
   *
   * @param clusterId
   * @param filename
   * @return
   */
  static bool persistInFile(ClusterID clusterId, const std::string& filename) {
    auto dirname = fs::FileUtils::dirname(filename.c_str());
    if (!fs::FileUtils::makeDir(dirname)) {
      LOG(INFO) << "Failed mkdir " << dirname;
      return false;
    }
    if (fs::FileUtils::remove(filename.c_str())) {
      LOG(INFO) << "Remove the existed file " << filename;
    }
    int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
      LOG(INFO) << "Open file error, file " << filename << ", error " << strerror(errno);
      return false;
    }
    int bytes = ::write(fd, reinterpret_cast<const char*>(&clusterId), sizeof(ClusterID));
    if (bytes != sizeof(clusterId)) {
      LOG(INFO) << "Write clusterId failed!";
      ::close(fd);
      return false;
    }
    LOG(INFO) << "Persist clusterId " << clusterId << " succeeded!";
    ::close(fd);
    return true;
  }

  /**
   * @brief Get cluster id from a file
   *
   * @param filename
   * @return
   */
  static ClusterID getClusterIdFromFile(const std::string& filename) {
    LOG(INFO) << "Try to open " << filename;
    int fd = ::open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
      LOG(INFO) << "Open file failed, error " << strerror(errno);
      return 0;
    }
    ClusterID clusterId = 0;
    int len = ::read(fd, reinterpret_cast<char*>(&clusterId), sizeof(ClusterID));
    if (len != sizeof(ClusterID)) {
      LOG(INFO) << "Get clusterId failed!";
      ::close(fd);
      return 0;
    }
    LOG(INFO) << "Get clusterId: " << clusterId;
    ::close(fd);
    return clusterId;
  }

  /**
   * @brief Get cluster id from kvStore
   *
   * @param kv
   * @param key
   * @return
   */
  static ClusterID getClusterIdFromKV(kvstore::KVStore* kv, const std::string& key) {
    CHECK_NOTNULL(kv);
    std::string value;
    auto code = kv->get(0, 0, key, &value, true);
    if (code == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      LOG(INFO) << "There is no clusterId existed in kvstore!";
      return 0;
    } else if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
      if (value.size() != sizeof(ClusterID)) {
        LOG(INFO) << "Bad clusterId " << value;
        return 0;
      }
      return *reinterpret_cast<const ClusterID*>(value.data());
    } else {
      LOG(INFO) << "Error in kvstore, err " << static_cast<int32_t>(code);
      return 0;
    }
  }

  /**
   * @brief Save cluster id into kvStore
   *
   * @param kv
   * @param key
   * @param clusterId
   * @return
   */
  static bool persistInKV(kvstore::KVStore* kv, const std::string& key, ClusterID clusterId) {
    CHECK_NOTNULL(kv);
    std::vector<kvstore::KV> data;
    data.emplace_back(key, std::string(reinterpret_cast<char*>(&clusterId), sizeof(ClusterID)));
    bool ret = true;
    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiPut(0, 0, std::move(data), [&](nebula::cpp2::ErrorCode code) {
      if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(INFO) << "Put failed, error " << static_cast<int32_t>(code);
        ret = false;
      } else {
        LOG(INFO) << "Put key " << key << ", val " << clusterId;
      }
      baton.post();
    });
    baton.wait();
    return ret;
  }
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CLUSTERIDMAN_H_
