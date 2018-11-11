/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/KVStoreImpl.h"
#include "network/NetworkUtils.h"
#include "storage/RocksdbEngine.h"
#include <algorithm>
#include <folly/Likely.h>

DEFINE_int32(engine_type, 0, "0 => RocksdbEngine");
DEFINE_int32(part_type, 0, "0 => SimplePart, 1 => ConsensusPart");

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

namespace vesoft {
namespace vgraph {
namespace storage {

static std::once_flag initKVFlag;
//static
KVStore* KVStore::instance_;

KVStore* KVStore::instance(HostAddr local, std::vector<std::string> paths) {
    std::call_once(initKVFlag, [&]() {
        KVStore::instance_ = new KVStoreImpl(local, std::move(paths));
        reinterpret_cast<KVStoreImpl*>(KVStore::instance_)->init();
    });
    return KVStore::instance_;
}

std::vector<Engine> KVStoreImpl::initEngines(GraphSpaceID spaceId) {
    decltype(kvs_[spaceId]->engines_) engines;
    for (auto& path : paths_) {
        if (FLAGS_engine_type == 0) {
            engines.emplace_back(
                new RocksdbEngine(spaceId,
                                  folly::stringPrintf("%s/vgraph/%d/data",
                                                      path.c_str(), spaceId)),
                path);
        }
    }
    return engines;
}

void KVStoreImpl::init() {
    auto partsMap = partMan_->parts(local_);
    LOG(INFO) << "Init all parts, total graph space " << partsMap.size();
    std::for_each(partsMap.begin(), partsMap.end(), [this](auto& idPart) {
        auto spaceId = idPart.first;
        auto& spaceParts = idPart.second;

        this->kvs_[spaceId] = std::make_unique<GraphSpaceKV>();
        this->kvs_[spaceId]->engines_ = initEngines(spaceId);

        // Init kvs[spaceId]->parts
        decltype(this->kvs_[spaceId]->parts_) parts;
        int32_t idx = 0;
        std::for_each(spaceParts.begin(), spaceParts.end(), [&](auto& partItem) {
            auto partId = partItem.first;
            auto& engine
                = this->kvs_[spaceId]->engines_[idx++ % this->kvs_[spaceId]->engines_.size()];
            auto& enginePtr = engine.first;
            auto& path = engine.second;
            if (FLAGS_part_type == 0) {
                parts.emplace(partId, new SimplePart(
                                            spaceId,
                                            partId,
                                               folly::stringPrintf("%s/vgraph/%d/wals/%d",
                                                                   path.c_str(), spaceId, partId),
                                              enginePtr.get()));
            }
        });
        this->kvs_[spaceId]->parts_ = std::move(parts);
    });
}

ResultCode KVStoreImpl::get(GraphSpaceID spaceId, PartitionID partId,
                               const std::string& key,
                            std::string& value) {
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->get(key, value);
}


ResultCode KVStoreImpl::range(GraphSpaceID spaceId, PartitionID partId,
                                const std::string& start,
                                const std::string& end,
                              std::unique_ptr<StorageIter>& iter) {
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->range(start, end, iter);
}

ResultCode KVStoreImpl::prefix(GraphSpaceID spaceId, PartitionID partId,
                                 const std::string& prefix,
                                 std::unique_ptr<StorageIter>& iter) {
    CHECK_AND_RETURN_ENGINE(spaceId, partId);
    return engine->prefix(prefix, iter);
}

ResultCode KVStoreImpl::asyncMultiPut(GraphSpaceID spaceId, PartitionID partId,
                                         std::vector<KV> keyValues,
                                       KVCallback cb) {
    CHECK_SPACE_AND_PART(spaceId, partId);
    return partIt->second->asyncMultiPut(std::move(keyValues), cb);
}

}  // namespace storage
}  // namespace vgraph
}  // namespace vesoft

