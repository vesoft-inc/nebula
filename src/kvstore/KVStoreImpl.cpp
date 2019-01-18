/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "kvstore/KVStoreImpl.h"
#include <folly/Likely.h>
#include <algorithm>
#include "network/NetworkUtils.h"
#include "kvstore/RocksdbEngine.h"
#include "meta/PartManager.h"

DEFINE_string(engine_type, "rocksdb", "rocksdb, memory...");
DEFINE_string(part_type, "simple", "simple, consensus...");

/**
 * Check spaceId, partId exists or not.
 * */
#define CHECK_SPACE_AND_PART(spaceId, partId) \
    auto it = kvs_.find(spaceId); \
    if (UNLIKELY(it == kvs_.end())) { \
        return ResultCode::ERR_SPACE_NOT_FOUND; \
    } \
    auto& parts = it->second->parts_; \
    auto partIt = parts.find(partId); \
    if (UNLIKELY(partIt == parts.end())) { \
        return ResultCode::ERR_PART_NOT_FOUND; \
    }
/**
 * Check spaceId, partId and return related storage engine.
 * */
#define CHECK_AND_RETURN_ENGINE(spaceId, partId) \
    StorageEngine* engine = nullptr; \
    do { \
        CHECK_SPACE_AND_PART(spaceId, partId); \
        engine = partIt->second->engine(); \
        CHECK_NOTNULL(engine); \
    } while (false)

namespace nebula {
namespace kvstore {

using meta::PartManager;

// static
KVStore* KVStore::instance(KVOptions options) {
    auto* instance = new KVStoreImpl(options);
    static_cast<KVStoreImpl*>(instance)->init();
    return instance;
}


std::vector<Engine> KVStoreImpl::initEngines(GraphSpaceID spaceId) {
    decltype(kvs_[spaceId]->engines_) engines;
    for (auto& path : options_.dataPaths_) {
        if (FLAGS_engine_type == "rocksdb") {
            engines.emplace_back(
                new RocksdbEngine(spaceId,
                                  folly::stringPrintf("%s/nebula/%d/data",
                                                      path.c_str(), spaceId),
                                  options_.mergeOp_,
                                  options_.cfFactory_),
                path);
        } else {
            LOG(FATAL) << "Unknown engine type " << FLAGS_engine_type;
        }
    }
    return engines;
}


void KVStoreImpl::init() {
    int32_t numSpaces = 0;
    meta::PartManager::forEachGraphSpace(
        [this, &numSpaces] (GraphSpaceID space, std::shared_ptr<const PartManager> pm) {
            ++numSpaces;
            kvs_[space] = std::make_unique<GraphSpaceKV>();
            kvs_[space]->engines_ = initEngines(space);

            size_t engineIdx = 0;
            const size_t numEngines = kvs_[space]->engines_.size();
            for (auto part : pm->partsOnHost(options_.local_)) {
                auto& engine = kvs_[space]->engines_[engineIdx++ % numEngines];
                auto& enginePtr = engine.first;
                auto& path = engine.second;
                if (FLAGS_part_type == "simple") {
                    kvs_[space]->parts_.emplace(
                        part,
                        new SimplePart(space,
                                       part,
                                       folly::stringPrintf("%s/nebula/%d/wals/%d",
                                                           path.c_str(),
                                                           space,
                                                           part),
                                       enginePtr.get()));
                } else {
                    LOG(FATAL) << "Unknown Part type " << FLAGS_part_type;
                }
            }
        });
}


ResultCode KVStoreImpl::get(GraphSpaceID spaceId, PartitionID partId,
                            const std::string& key,
                            std::string* value) {
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->get(key, value);
}


ResultCode KVStoreImpl::range(GraphSpaceID spaceId, PartitionID partId,
                              const std::string& start,
                              const std::string& end,
                              std::unique_ptr<StorageIter>* iter) {
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->range(start, end, iter);
}


ResultCode KVStoreImpl::prefix(GraphSpaceID spaceId, PartitionID partId,
                               const std::string& prefix,
                               std::unique_ptr<StorageIter>* iter) {
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->prefix(prefix, iter);
}


ResultCode KVStoreImpl::asyncMultiPut(GraphSpaceID spaceId, PartitionID partId,
                                      std::vector<KV> keyValues,
                                      KVCallback cb) {
    CHECK_SPACE_AND_PART(spaceId, partId);
    return partIt->second->asyncMultiPut(std::move(keyValues), std::move(cb));
}

}  // namespace kvstore
}  // namespace nebula

