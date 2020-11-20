/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_META_METACOMMON_H_
#define COMMON_META_METACOMMON_H_

#include "common/base/Base.h"
#include "common/thrift/ThriftTypes.h"
#include "common/datatypes/HostAddr.h"
#include "common/interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

struct PartHosts {
    GraphSpaceID spaceId_;
    PartitionID partId_;
    std::vector<HostAddr> hosts_;

    bool operator==(const PartHosts& rhs) const {
        return this->spaceId_ == rhs.spaceId_ && this->partId_ == rhs.partId_ &&
               this->hosts_ == rhs.hosts_;
    }

    bool operator!=(const PartHosts& rhs) const {
        return !(*this == rhs);
    }
};

// ListenerHosts saves the listener type and the peers of the data replica
struct ListenerHosts {
    ListenerHosts(cpp2::ListenerType type, std::vector<HostAddr> peers)
        : type_(std::move(type)), peers_(std::move(peers)) {
    }

    bool operator==(const ListenerHosts& rhs) const {
        return this->type_ == rhs.type_ && this->peers_ == rhs.peers_;
    }

    bool operator<(const ListenerHosts& rhs) const {
        if (this->type_ == rhs.type_) {
            return this->peers_ < rhs.peers_;
        }
        return this->type_ < rhs.type_;
    }

    cpp2::ListenerType      type_;
    // peers is the part peers which would send logs to the listener
    std::vector<HostAddr>   peers_;
};

using PartsMap = std::unordered_map<GraphSpaceID, std::unordered_map<PartitionID, PartHosts>>;
// ListenersMap is used for listener replica to get its peers of data replica
using ListenersMap =
    std::unordered_map<GraphSpaceID, std::unordered_map<PartitionID, std::vector<ListenerHosts>>>;
// RemoteListenerInfo is pair of <listener host, listener type>
using RemoteListenerInfo = std::pair<HostAddr, cpp2::ListenerType>;
// RemoteListeners is used for data replica to check if some part has remote listener
using RemoteListeners =
    std::unordered_map<GraphSpaceID,
                       std::unordered_map<PartitionID, std::vector<RemoteListenerInfo>>>;

inline bool checkSegment(const std::string& segment) {
    static const std::regex pattern("^[0-9a-zA-Z]+$");
    if (!segment.empty() && std::regex_match(segment, pattern)) {
        return true;
    }
    return false;
}

}   // namespace meta
}   // namespace nebula

#endif  // COMMON_META_METACOMMON_H_
