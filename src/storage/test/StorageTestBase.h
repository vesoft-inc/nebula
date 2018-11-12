/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "kvstore/include/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/KVStoreImpl.h"

DECLARE_string(part_man_type);

namespace nebula {
namespace storage {

class TestUtils {
public:
    static kvstore::KVStore* initKV(const char* rootPath) {
        FLAGS_part_man_type = "memory";  // Use MemPartManager.
        using namespace kvstore;
        MemPartManager* partMan = reinterpret_cast<MemPartManager*>(PartManager::instance());
        // GraphSpaceID =>  {PartitionIDs}
        // 0 => {0, 1, 2, 3, 4, 5}
        auto& partsMap = partMan->partsMap();
        for (auto partId = 0; partId < 6; partId++) {
            partsMap[0][partId] = PartMeta();
        }
        auto dataPath = folly::stringPrintf("%s/disk1, %s/disk2", rootPath, rootPath);
        std::vector<std::string> paths;
        paths.push_back(folly::stringPrintf("%s/disk1", rootPath));
        paths.push_back(folly::stringPrintf("%s/disk2", rootPath));
        KVStoreImpl* kv = reinterpret_cast<KVStoreImpl*>(KVStore::instance(HostAddr(0, 0),
                                                                       std::move(paths)));
        return kv;
    }
};

}  // namespace storage
}  // namespace nebula

