/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "storage/PartManager.h"

DEFINE_string(part_man_type, "memory", "memory, meta");

namespace vesoft {
namespace storage {

//static
PartManager* PartManager::instance_;
static std::once_flag initPartManFlag;
PartManager* PartManager::instance() {
    std::call_once(initPartManFlag, [](){
        if (FLAGS_part_man_type == "memory") {
            instance_ = new MemPartManager();
        } else {
            LOG(FATAL) << "Unknown partManager type " << FLAGS_part_man_type;
        }
    });
    return instance_;
}

PartsMap MemPartManager::parts(HostAddr hostAddr) {
    return partsMap_;
}

PartMeta MemPartManager::partMeta(GraphSpaceID spaceId, PartitionID partId) {
    auto it = partsMap_.find(spaceId);
    CHECK(it != partsMap_.end());
    auto partIt = it->second.find(partId);
    CHECK(partIt != it->second.end());
    return partIt->second;
}


}  // namespace storage
}  // namespace vesoft

