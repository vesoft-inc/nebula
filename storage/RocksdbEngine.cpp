/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/RocksdbEngine.h"
#include <folly/String.h>

DEFINE_uint32(batch_reserved_bytes, 4 * 1024, "default reserved bytes for one batch operation");

#define CHECK_PART_EXIST(partId) \
    auto it = dbs_.find(partId); \
    if (it == dbs_.end()) { \
        return ResultCode::ERR_SHARD_NOT_FOUND; \
    }
    
namespace vesoft {
namespace vgraph {
namespace storage {

RocksdbEngine::~RocksdbEngine() {
    cleanup();
}

void RocksdbEngine::registerParts(const std::vector<PartitionID>& ids) {
    std::vector<std::string> paths;
    folly::split(",", dataPath_, paths, true);
    for (auto& path : paths) {
        LOG(INFO) << "init rocksdb on " << path;
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::DB* db = nullptr;
        rocksdb::Status status = rocksdb::DB::Open(options, path, &db);
        CHECK(status.ok());
        instances_.push_back(db);
    }
    std::vector<PartitionID> sortedPartIds(ids);
    std::sort(sortedPartIds.begin(), sortedPartIds.end());
    uint32_t index = 0;
    for (auto& id : sortedPartIds) {
        dbs_.emplace(id, instances_[index++ % instances_.size()]);
    }
}

void RocksdbEngine::cleanup() {
    for (auto it = instances_.begin(); it != instances_.end(); it++) {
        delete *it;
    }
    instances_.clear();
    dbs_.clear();
}

ResultCode RocksdbEngine::get(PartitionID partId, 
                              const std::string& key,
                              std::string& value) {
    CHECK_PART_EXIST(partId);
    rocksdb::ReadOptions options;
    rocksdb::Status status = it->second->Get(options, rocksdb::Slice(key), &value);
    if (status.ok()) {
        return ResultCode::SUCCESSED;
    } else if (status.IsNotFound()) {
        return ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ResultCode::ERR_UNKNOWN;
}

ResultCode RocksdbEngine::put(PartitionID partId,
                              std::string key,
                              std::string value) {
    CHECK_PART_EXIST(partId);
    rocksdb::WriteOptions options;
    rocksdb::Status status = it->second->Put(options, rocksdb::Slice(key), rocksdb::Slice(value));
    if (status.ok()) {
        return ResultCode::SUCCESSED;
    }
    return ResultCode::ERR_UNKNOWN;
}

ResultCode RocksdbEngine::multiPut(PartitionID partId, std::vector<KV> keyValues) {
    CHECK_PART_EXIST(partId);
    rocksdb::WriteBatch updates(FLAGS_batch_reserved_bytes);
    for (size_t i = 0; i < keyValues.size(); i++) {
        updates.Put(rocksdb::Slice(keyValues[i].first), rocksdb::Slice(keyValues[i].second));
    }
    rocksdb::WriteOptions options;
    rocksdb::Status status = it->second->Write(options, &updates);
    if (status.ok()) {
        return ResultCode::SUCCESSED;
    }
    return ResultCode::ERR_UNKNOWN;
}

ResultCode RocksdbEngine::range(PartitionID partId,
                                const std::string& start,
                                const std::string& end,
                                std::shared_ptr<StorageIter>& storageIter) {
    CHECK_PART_EXIST(partId);
    rocksdb::ReadOptions options;
    rocksdb::Iterator* iter = it->second->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(start));
    } else {
        return ResultCode::ERR_KEY_NOT_FOUND;
    }
    storageIter.reset(new RocksdbRangeIter(iter, start, end));
    return ResultCode::SUCCESSED;
}

ResultCode RocksdbEngine::prefix(PartitionID partId,
                                 const std::string& prefix,
                                 std::shared_ptr<StorageIter>& storageIter) {
    CHECK_PART_EXIST(partId);
    rocksdb::ReadOptions options;
    rocksdb::Iterator* iter = it->second->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(prefix));
    }
    storageIter.reset(new RocksdbPrefixIter(iter, prefix));

    return ResultCode::SUCCESSED;
}

}  // namespace storage
}  // namespace vgraph
}  // namespace vesoft

