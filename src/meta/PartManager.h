/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_PARTMANAGER_H_
#define META_PARTMANAGER_H_

#include "base/Base.h"
#include <folly/RWSpinLock.h>
#include <gtest/gtest.h>

namespace nebula {
namespace meta {

class PartManager {
    FRIEND_TEST(FileBasedPartManager, PartitionAllocation);
public:
    // Retrieve the Partition Manager for the given graph space
    static std::shared_ptr<const PartManager> get(GraphSpaceID space);

    // Return the total number of partitions
    size_t numParts() const;

    // Return the total number of hosts
    size_t numHosts() const;

    // Return all hosts
    const std::vector<HostAddr>& allHosts() const;

    // Calculate the partition id for the given vertex id
    PartitionID partId(int64_t id) const;

    // Figure out which host that the given partition belongs to
    const std::vector<HostAddr>& hostsForPart(PartitionID part) const;

    const std::vector<PartitionID>& partsOnHost(HostAddr host) const;

    // Cluster given ids into the shard they belong to
    // The method returns a map
    //  shard_id => [ids that belong to the shard]
    std::unordered_map<PartitionID, std::vector<int64_t>>
    clusterIdsToParts(std::vector<int64_t>& ids) const;

protected:
    PartManager() = default;

protected:
    // A list of hosts
    std::vector<HostAddr> hosts_;

    // A map from partition => host
    std::unordered_map<PartitionID, std::vector<HostAddr>> partHostMap_;

    // A reversed map from host => partition
    std::unordered_map<HostAddr, std::vector<PartitionID>> hostPartMap_;

protected:
    static folly::RWSpinLock accessLock_;
    static std::unordered_map<GraphSpaceID, std::shared_ptr<const PartManager>> partManagers_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_PARTMANAGER_H_
