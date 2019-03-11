/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "kvstore/RocksdbEngine.h"
#include <folly/String.h>
#include "fs/FileUtils.h"
#include "kvstore/KVStore.h"
#include "kvstore/RocksdbConfigFlags.h"

DEFINE_uint32(batch_reserved_bytes, 4 * 1024, "default reserved bytes for one batch operation");

namespace nebula {
namespace kvstore {

const char* kSystemParts = "__system__parts__";

RocksdbEngine::RocksdbEngine(GraphSpaceID spaceId, const std::string& dataPath)
    : KVEngine(spaceId)
    , dataPath_(dataPath) {
    LOG(INFO) << "open rocksdb on " << dataPath;
    if (nebula::fs::FileUtils::fileType(dataPath.c_str()) == nebula::fs::FileType::NOTEXIST) {
        nebula::fs::FileUtils::makeDir(dataPath);
    }
    rocksdb::Options options;
    rocksdb::DB* db = nullptr;
    options = RocksdbConfigOptions::getRocksdbOptions(dataPath, false, false);
    rocksdb::Status status = rocksdb::DB::Open(options, dataPath_, &db);
    CHECK(status.ok());
    db_.reset(db);
    partsNum_ = allParts().size();
}

RocksdbEngine::~RocksdbEngine() {
}

ResultCode RocksdbEngine::get(const std::string& key,
                              std::string* value) {
    rocksdb::ReadOptions options;
    rocksdb::Status status = db_->Get(options, rocksdb::Slice(key), value);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else if (status.IsNotFound()) {
        return ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ResultCode::ERR_UNKNOWN;
}

ResultCode RocksdbEngine::put(std::string key,
                              std::string value) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    rocksdb::Status status = db_->Put(options, rocksdb::Slice(key), rocksdb::Slice(value));
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    }
    return ResultCode::ERR_UNKNOWN;
}

ResultCode RocksdbEngine::multiPut(std::vector<KV> keyValues) {
    rocksdb::WriteBatch updates(FLAGS_batch_reserved_bytes);
    for (size_t i = 0; i < keyValues.size(); i++) {
        updates.Put(rocksdb::Slice(keyValues[i].first), rocksdb::Slice(keyValues[i].second));
    }
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    rocksdb::Status status = db_->Write(options, &updates);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    }
    return ResultCode::ERR_UNKNOWN;
}

ResultCode RocksdbEngine::range(const std::string& start,
                                const std::string& end,
                                std::unique_ptr<KVIterator>* storageIter) {
    rocksdb::ReadOptions options;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(start));
    }
    storageIter->reset(new RocksdbRangeIter(iter, start, end));
    return ResultCode::SUCCEEDED;
}

ResultCode RocksdbEngine::prefix(const std::string& prefix,
                                 std::unique_ptr<KVIterator>* storageIter) {
    rocksdb::ReadOptions options;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(prefix));
    }
    storageIter->reset(new RocksdbPrefixIter(iter, prefix));
    return ResultCode::SUCCEEDED;
}

ResultCode RocksdbEngine::remove(const std::string& key) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    auto status = db_->Delete(options, key);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    }
    return ResultCode::ERR_UNKNOWN;
}

ResultCode RocksdbEngine::removeRange(const std::string& start,
                                      const std::string& end) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    auto status = db_->DeleteRange(options, db_->DefaultColumnFamily(), start, end);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    }
    return ResultCode::ERR_UNKNOWN;
}

std::string RocksdbEngine::partKey(PartitionID partId) {
    std::string key;
    static const size_t prefixLen = ::strlen(kSystemParts);
    key.reserve(prefixLen + sizeof(PartitionID));
    key.append(kSystemParts, prefixLen);
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
    return key;
}

void RocksdbEngine::addPart(PartitionID partId) {
    auto ret = put(partKey(partId), "");
    if (ret == ResultCode::SUCCEEDED) {
        partsNum_++;
        CHECK_GE(partsNum_, 0);
    }
}

void RocksdbEngine::removePart(PartitionID partId) {
     rocksdb::WriteOptions options;
     options.disableWAL = FLAGS_rocksdb_disable_wal;
     auto status = db_->Delete(options, partKey(partId));
     if (status.ok()) {
         partsNum_--;
         CHECK_GE(partsNum_, 0);
     }
}

std::vector<PartitionID> RocksdbEngine::allParts() {
    std::unique_ptr<KVIterator> iter;
    static const size_t prefixLen = ::strlen(kSystemParts);
    static const std::string prefixStr(kSystemParts, prefixLen);
    CHECK_EQ(ResultCode::SUCCEEDED, this->prefix(prefixStr, &iter));
    std::vector<PartitionID> parts;
    while (iter->valid()) {
        auto key = iter->key();
        CHECK_EQ(key.size(), prefixLen + sizeof(PartitionID));
        parts.emplace_back(
           *reinterpret_cast<const PartitionID*>(key.data() + key.size() - sizeof(PartitionID)));
        iter->next();
    }
    return parts;
}

int32_t RocksdbEngine::totalPartsNum() {
    return partsNum_;
}

ResultCode RocksdbEngine::ingest(const std::vector<std::string>& files) {
    rocksdb::IngestExternalFileOptions options;
    rocksdb::Status status = db_->IngestExternalFile(files, options);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        return ResultCode::ERR_UNKNOWN;
    }
}

}  // namespace kvstore
}  // namespace nebula

