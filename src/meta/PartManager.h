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
    friend class AdHocPartManagersBuilder;

public:
    // Retrieve the Partition Manager for the given graph space
    static std::shared_ptr<const PartManager> get(GraphSpaceID space);
    // Iterate through all Graph Spaces
    // The method has to have following signature
    //   void handler(GraphSpaceID space, std::shared_ptr<PartManager> pm)
    template<class GraphSpaceHandler>
    static void forEachGraphSpace(GraphSpaceHandler&& handler) {
        for (auto& pm : partManagers_) {
            handler(pm.first, pm.second);
        }
    }

    // Return the total number of graph spaces
    static size_t  numGraphSpaces() {
        return partManagers_.size();
    }

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
    static std::unordered_map<GraphSpaceID, std::shared_ptr<PartManager>> partManagers_;
};

class AdHocPartManagersBuilder final {
public:
    static std::unordered_map<GraphSpaceID, std::shared_ptr<PartManager>> get();

    static void add(GraphSpaceID spaceId, HostAddr host, std::vector<PartitionID> parts);

private:
    static std::unordered_map<GraphSpaceID, std::shared_ptr<PartManager>> partManagers_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_PARTMANAGER_H_
