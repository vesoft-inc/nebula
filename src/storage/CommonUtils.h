/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_COMMON_H_
#define STORAGE_COMMON_H_

#include "base/Base.h"
#include "base/ConcurrentLRUCache.h"
#include "interface/gen-cpp2/storage_types.h"
#include "codec/RowReader.h"
#include "kvstore/KVStore.h"
#include "meta/SchemaManager.h"
#include "meta/IndexManager.h"
#include "meta/SchemaManager.h"

namespace nebula {
namespace storage {

using VertexCache = ConcurrentLRUCache<std::pair<VertexID, TagID>, std::string>;

struct StorageEnv {
    kvstore::KVStore*                               kvstore_{nullptr};
    meta::SchemaManager*                            schemaMan_{nullptr};
    meta::IndexManager*                             indexMan_{nullptr};
};

class CommonUtils final {
public:
    static bool checkDataExpiredForTTL(const meta::SchemaProviderIf* schema,
                                       RowReader* reader,
                                       const std::string& ttlCol,
                                       int64_t ttlDuration);
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_COMMON_H_
