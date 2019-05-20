/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/hbase/HBaseStore.h"

namespace nebula {
namespace kvstore {

// static
KVStore* KVStore::instance(KVOptions options, const std::string& engineType) {
    if (engineType == "rocksdb") {
        auto* instance = new NebulaStore(std::move(options));
        static_cast<NebulaStore*>(instance)->init();
        return instance;
    } else if (engineType == "hbase") {
        auto* instance = new HBaseStore(std::move(options));
        static_cast<HBaseStore*>(instance)->init();
        return instance;
    } else {
        LOG(FATAL) << "Unknown Engine type " << engineType;
    }
    return nullptr;
}

}  // namespace kvstore
}  // namespace nebula

