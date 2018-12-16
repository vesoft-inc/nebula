/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_HOSTMANAGER_H_
#define META_HOSTMANAGER_H_

#include "base/Base.h"

namespace nebula {
namespace meta {

class HostManager final {
public:
    static const HostManager& get();

    // Get the number of storage hosts for the given space
    size_t numHosts(GraphSpaceID space) const;

    // Get all storage hosts for the given space
    const std::vector<HostAddr>& allHosts(GraphSpaceID space) const;

    // Return one storage host that has the given id
    HostAddr hostForId(GraphSpaceID space, int64_t id, PartitionID& shard) const;

    // Cluster given ids into the host they belong to
    // The method returns a map
    //  host_addr (Any one, in most case, the leader will be chosen)
    //      => (shard -> [ids that belong to the shard])
    std::unordered_map<HostAddr, std::unordered_map<PartitionID, std::vector<int64_t>>>
    clusterIdsToHosts(GraphSpaceID space, std::vector<int64_t>& ids) const;

private:
    HostManager() = default;

private:
};

}  // namespace meta
}  // namespace nebula
#endif  // META_HOSTMANAGER_H_
