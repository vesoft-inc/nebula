/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "kvstore/include/KVStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/KVStoreImpl.h"
#include "meta/CreateNodeProcessor.h"

DECLARE_string(part_man_type);

namespace nebula {
namespace meta {

class TestUtils {
public:
    static kvstore::KVStore* initKV(const char* rootPath) {
        FLAGS_part_man_type = "memory";  // Use MemPartManager.
        kvstore::MemPartManager* partMan = static_cast<kvstore::MemPartManager*>(
                                                            kvstore::PartManager::instance());
        // GraphSpaceID =>  {PartitionIDs}
        // 0 => {0}
        auto& partsMap = partMan->partsMap();
        partsMap[0][0] = kvstore::PartMeta();

        std::vector<std::string> paths;
        paths.push_back(folly::stringPrintf("%s/disk1", rootPath));

        kvstore::KVOptions options;
        options.local_ = HostAddr(0, 0);
        options.dataPaths_ = std::move(paths);

        kvstore::KVStoreImpl* kv = static_cast<kvstore::KVStoreImpl*>(
                                        kvstore::KVStore::instance(std::move(options)));
        return kv;
    }

    static void createSomeNodes(kvstore::KVStore* kv, folly::RWSpinLock* lock,
                                std::vector<std::string> nodes = {"/", "/abc", "/abc/d"}) {
        for (auto& node : nodes) {
            auto layer = MetaUtils::layer(node);
            auto* processor = CreateNodeProcessor::instance(kv, lock);
            cpp2::CreateNodeRequest req;
            req.set_path(node);
            req.set_value(folly::stringPrintf("%d_%s", layer, node.c_str()));
            auto f = processor->getFuture();
            processor->process(req);
            auto resp = std::move(f).get();
            EXPECT_EQ(resp.code, cpp2::ErrorCode::SUCCEEDED);
            std::string val;
            CHECK_EQ(kvstore::ResultCode::SUCCESSED,
                     kv->get(0, 0, MetaUtils::metaKey(layer, node.c_str()), &val));
            CHECK_EQ(folly::stringPrintf("%d_%s", layer, node.c_str()), val);
        }
    }
};

}  // namespace meta
}  // namespace nebula

