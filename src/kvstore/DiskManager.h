/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_DISKMANAGER_H_
#define KVSTORE_DISKMANAGER_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/thread/GenericWorker.h"
#include "common/thrift/ThriftTypes.h"
#include <gtest/gtest_prod.h>
#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>

namespace nebula {
namespace kvstore {

using PartDiskMap = std::unordered_map<std::string, std::set<PartitionID>>;

class DiskManager {
    FRIEND_TEST(DiskManagerTest, AvailableTest);
    FRIEND_TEST(DiskManagerTest, WalNoSpaceTest);

public:
    DiskManager(const std::vector<std::string>& dataPaths,
                std::shared_ptr<thread::GenericWorker> bgThread = nullptr);

    // Canonical path of all which contains the specified space,
    // e.g. {"/DataPath1/nebula/spaceId", "/DataPath2/nebula/spaceId" ... }
    StatusOr<std::vector<std::string>> path(GraphSpaceID spaceId);

    // Canonical path which contains the specified space and part, e.g. "/DataPath/nebula/spaceId".
    // As for one storage instance, at most one path should contain a parition.
    // Note that there isn't a separate dir for a parititon (except wal), so we return space dir
    StatusOr<std::string> path(GraphSpaceID spaceId, PartitionID partId);

    // pre-condition: path is the space path, so it must end with /nebula/spaceId and path exists
    void addPartToPath(GraphSpaceID spaceId, PartitionID partId, const std::string& path);

    // pre-condition: path is the space path, so it must end with /nebula/spaceId and path exists
    void removePartFromPath(GraphSpaceID spaceId, PartitionID partId, const std::string& path);

    bool hasEnoughSpace(GraphSpaceID spaceId, PartitionID partId);

    // Given a space, return data path and all partition in the path
    StatusOr<PartDiskMap> partDist(GraphSpaceID spaceId);

private:
    // refresh free bytes of data path periodically
    void refresh();

private:
    std::shared_ptr<thread::GenericWorker> bgThread_;

    // canonical path of data_path flag
    std::vector<boost::filesystem::path> dataPaths_;
    // free space available to a non-privileged process, in bytes
    std::vector<std::atomic_uint64_t> freeBytes_;

    // given a space and data path, return all parts in the path
    std::unordered_map<GraphSpaceID, PartDiskMap> partPath_;

    // the index in dataPaths_ for a given space + part
    std::unordered_map<GraphSpaceID, std::unordered_map<PartitionID, size_t>> partIndex_;

    // lock used to protect partPath_ and partIndex_
    std::mutex lock_;
};

}  // namespace kvstore
}  // namespace nebula

#endif
