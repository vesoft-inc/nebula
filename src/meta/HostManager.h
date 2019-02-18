/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_HOSTMANAGER_H_
#define META_HOSTMANAGER_H_

#include "base/Base.h"
#include "meta/PartManager.h"

namespace nebula {
namespace meta {

class HostManager final {
public:
    static std::shared_ptr<const HostManager> get(GraphSpaceID space);

    // Get the number of storage hosts
    size_t numHosts() const;

    // Get all storage hosts
    const std::vector<HostAddr>& allHosts() const;

    // Return one storage host that has the given id
    HostAddr hostForId(int64_t id, PartitionID& part) const;

    // Cluster given ids into the host they belong to
    // The method returns a map
    //  host_addr (A host, but in most case, the leader will be chosen)
    //      => (partition -> [ids that belong to the shard])
    template<class Container, class GetIdFunc>
    std::unordered_map<HostAddr,
                       std::unordered_map<PartitionID,
                                          std::vector<typename Container::value_type>
                                         >
                      >
    clusterIdsToHosts(Container ids, GetIdFunc f) const {
        std::unordered_map<HostAddr,
                           std::unordered_map<PartitionID,
                                              std::vector<typename Container::value_type>
                                             >
                          > clusters;
        auto sm = PartManager::get(space_);
        if (!sm) {
            LOG(ERROR) << "Cannot find PartManager for the graph space " << space_;
        } else {
            for (auto& id : ids) {
                PartitionID part = sm->partId(f(id));
                auto hosts = sm->hostsForPart(part);
                CHECK_GT(hosts.size(), 0U);
                // TODO We need to use the leader here
                clusters[hosts.front()][part].push_back(std::move(id));
            }
        }

        return clusters;
    }

private:
    explicit HostManager(GraphSpaceID space) : space_(space) {}

private:
    static std::unordered_map<GraphSpaceID, std::shared_ptr<HostManager>> hostManagers_;

    const GraphSpaceID space_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_HOSTMANAGER_H_
