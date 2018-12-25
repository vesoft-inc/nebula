/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "kvstore/RocksdbEngine.h"
#include <folly/String.h>
#include "fs/FileUtils.h"

DEFINE_uint32(batch_reserved_bytes, 4 * 1024, "default reserved bytes for one batch operation");

namespace nebula {
namespace kvstore {

RocksdbEngine::RocksdbEngine(GraphSpaceID spaceId, const std::string& dataPath)
    : StorageEngine(spaceId)
    , dataPath_(dataPath) {
    LOG(INFO) << "open rocksdb on " << dataPath;
    if (nebula::fs::FileUtils::fileType(dataPath.c_str()) == nebula::fs::FileType::NOTEXIST) {
        nebula::fs::FileUtils::makeDir(dataPath);
    }
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db = nullptr;
    rocksdb::Status status = rocksdb::DB::Open(options, dataPath_, &db);
    CHECK(status.ok());
    db_.reset(db);
}

RocksdbEngine::~RocksdbEngine() {
}

ResultCode RocksdbEngine::get(const std::string& key,
                              std::string& value) {
    rocksdb::ReadOptions options;
    rocksdb::Status status = db_->Get(options, rocksdb::Slice(key), &value);
    if (status.ok()) {
        return ResultCode::SUCCESSED;
    } else if (status.IsNotFound()) {
        return ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ResultCode::ERR_UNKNOWN;
}

ResultCode RocksdbEngine::put(std::string key,
                              std::string value) {
    rocksdb::WriteOptions options;
    rocksdb::Status status = db_->Put(options, rocksdb::Slice(key), rocksdb::Slice(value));
    if (status.ok()) {
        return ResultCode::SUCCESSED;
    }
    return ResultCode::ERR_UNKNOWN;
}

ResultCode RocksdbEngine::multiPut(std::vector<KV> keyValues) {
    rocksdb::WriteBatch updates(FLAGS_batch_reserved_bytes);
    for (size_t i = 0; i < keyValues.size(); i++) {
        updates.Put(rocksdb::Slice(keyValues[i].first), rocksdb::Slice(keyValues[i].second));
    }
    rocksdb::WriteOptions options;
    rocksdb::Status status = db_->Write(options, &updates);
    if (status.ok()) {
        return ResultCode::SUCCESSED;
    }
    return ResultCode::ERR_UNKNOWN;
}

ResultCode RocksdbEngine::range(const std::string& start,
                                const std::string& end,
                                std::unique_ptr<StorageIter>& storageIter) {
    rocksdb::ReadOptions options;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(start));
    }
    storageIter.reset(new RocksdbRangeIter(iter, start, end));
    return ResultCode::SUCCESSED;
}

ResultCode RocksdbEngine::prefix(const std::string& prefix,
                                 std::unique_ptr<StorageIter>& storageIter) {
    rocksdb::ReadOptions options;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(prefix));
    }
    storageIter.reset(new RocksdbPrefixIter(iter, prefix));
    return ResultCode::SUCCESSED;
}

}  // namespace kvstore
}  // namespace nebula

