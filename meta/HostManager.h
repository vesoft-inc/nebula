/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_HOSTMANAGER_H_
#define META_HOSTMANAGER_H_

#include "base/Base.h"

namespace vesoft {
namespace meta {

class HostManager final {
public:
    static const HostManager& get();

    // Return the total number of hosts
    int32_t numHosts() const;

    // Get all hosts
    const std::vector<HostAddr>& allHosts() const;

    // Return the host address that has the given id
    HostAddr host(int64_t id) const;

    // Cluster given ids into the host they belong to
    // The method returns a map
    //  host_addr => [ids that belong to the host]
    std::unordered_map<HostAddr, std::vector<int64_t>> clusterIdsToHosts(
        std::vector<int64_t> ids) const;

private:
    HostManager() = default;

private:
    std::vector<HostAddr> hosts_;
};

}  // namespace meta
}  // namespace vesoft
#endif  // META_HOSTMANAGER_H_
