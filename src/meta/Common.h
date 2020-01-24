/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METACOMMON_H_
#define META_METACOMMON_H_

#include "base/Base.h"
#include "thrift/ThriftTypes.h"
#include "datatypes/HostAddr.h"

namespace nebula {
namespace meta {

// reserved property names
constexpr char _ID[]    = "_id";
constexpr char _SRC[]   = "_src";
constexpr char _TYPE[]  = "_type";
constexpr char _RANK[]  = "_rank";
constexpr char _DST[]   = "_dst";


struct PartMeta {
    GraphSpaceID           spaceId_;
    PartitionID            partId_;
    std::vector<HostAddr>  peers_;

    bool operator==(const PartMeta& that) const {
        return this->spaceId_ == that.spaceId_
                    && this->partId_ == that.partId_
                    && this->peers_ == that.peers_;
    }

    bool operator!=(const PartMeta& that) const {
        return !(*this == that);
    }
};

using PartsMap  = std::unordered_map<GraphSpaceID, std::unordered_map<PartitionID, PartMeta>>;


inline bool checkSegment(const std::string& segment) {
    static const std::regex pattern("^[0-9a-zA-Z]+$");
    if (!segment.empty() && std::regex_match(segment, pattern)) {
        return true;
    }
    return false;
}

}  // namespace meta
}  // namespace nebula

#endif  // META_METACOMMON_H_
