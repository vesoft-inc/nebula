/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_DISKMANAGER_H_
#define KVSTORE_DISKMANAGER_H_

#include <gtest/gtest_prod.h>

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/thread/GenericWorker.h"
#include "common/thrift/ThriftTypes.h"
#include "folly/synchronization/Rcu.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace kvstore {

using PartDiskMap = std::unordered_map<std::string, std::set<PartitionID>>;
using SpaceDiskPartsMap =
    std::unordered_map<GraphSpaceID, std::unordered_map<std::string, meta::cpp2::PartitionList>>;

/**
 * @brief Monitor remaining spaces of each disk
 */
class DiskManager {
  FRIEND_TEST(DiskManagerTest, AvailableTest);
  FRIEND_TEST(DiskManagerTest, WalNoSpaceTest);

 public:
  /**
   * @brief Construct a new Disk Manager object
   *
   * @param dataPaths `data_path` in configuration
   * @param bgThread Background thread to refresh remaining spaces of each data path
   */
  DiskManager(const std::vector<std::string>& dataPaths,
              std::shared_ptr<thread::GenericWorker> bgThread = nullptr);

  ~DiskManager();

  /**
   * @brief return canonical data path of given space
   *
   * @param spaceId
   * @return StatusOr<std::vector<std::string>> Canonical path of all which contains the specified
   * space, e.g. {"/DataPath1/nebula/spaceId", "/DataPath2/nebula/spaceId" ... }
   */
  StatusOr<std::vector<std::string>> path(GraphSpaceID spaceId) const;

  /**
   * @brief Canonical path which contains the specified space and part, e.g.
   * "/DataPath/nebula/spaceId". As for one storage instance, at most one path should contain a
   * partition. Note that there isn't a separate dir for a partition (except wal), so we return
   * space dir
   *
   * @param spaceId
   * @param partId
   * @return StatusOr<std::string> data path of given partId if found, else return error status
   */
  StatusOr<std::string> path(GraphSpaceID spaceId, PartitionID partId) const;

  /**
   * @brief Add a partition to a given path, called when add a partition in NebulaStore
   * @pre Path is the space path, so it must end with /nebula/spaceId and path must exists
   *
   * @param spaceId
   * @param partId
   * @param path
   */
  void addPartToPath(GraphSpaceID spaceId, PartitionID partId, const std::string& path);

  /**
   * @brief Remove a partition form a given path, called when remove a partition in NebulaStore
   * @pre Path is the space path, so it must end with /nebula/spaceId and path must exists
   *
   * @param spaceId
   * @param partId
   * @param path
   */
  void removePartFromPath(GraphSpaceID spaceId, PartitionID partId, const std::string& path);

  /**
   * @brief Check if the data path of given partition has enough spaces
   *
   * @param spaceId
   * @param partId
   * @return true Data path remains enough space
   * @return false Data path does not remain enough space
   */
  bool hasEnoughSpace(GraphSpaceID spaceId, PartitionID partId) const;

  /**
   * @brief Get all partitions grouped by data path and spaceId
   *
   * @param diskParts Get all space data path and all partition in the path
   */
  void getDiskParts(SpaceDiskPartsMap& diskParts) const;

 private:
  /**
   * @brief Refresh free bytes of data path periodically
   */
  void refresh();

  struct Paths {
    // canonical path of data_path flag
    std::vector<boost::filesystem::path> dataPaths_;
    // given a space and data path, return all parts in the path
    std::unordered_map<GraphSpaceID, PartDiskMap> partPath_;
    // the index in dataPaths_ for a given space + part
    std::unordered_map<GraphSpaceID, std::unordered_map<PartitionID, size_t>> partIndex_;
  };

 private:
  std::shared_ptr<thread::GenericWorker> bgThread_;

  std::atomic<Paths*> paths_;
  // free space available to a non-privileged process, in bytes
  std::vector<std::atomic_uint64_t> freeBytes_;

  // lock used to protect partPath_ and partIndex_
  std::mutex lock_;
};

}  // namespace kvstore
}  // namespace nebula

#endif
