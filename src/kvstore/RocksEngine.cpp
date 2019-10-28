/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/RocksEngine.h"
#include <folly/String.h>
#include "fs/FileUtils.h"
#include "kvstore/KVStore.h"
#include "kvstore/RocksEngineConfig.h"

namespace nebula {
namespace kvstore {

using fs::FileUtils;
using fs::FileType;

namespace {

/***************************************
 *
 * Implementation of WriteBatch
 *
 **************************************/
class RocksWriteBatch : public WriteBatch {
private:
    rocksdb::WriteBatch batch_;
    rocksdb::DB* db_{nullptr};

public:
    explicit RocksWriteBatch(rocksdb::DB* db) : batch_(FLAGS_rocksdb_batch_size), db_(db) {}

    virtual ~RocksWriteBatch() = default;

    ResultCode put(folly::StringPiece key, folly::StringPiece value) override {
        if (batch_.Put(toSlice(key), toSlice(value)).ok()) {
            return ResultCode::SUCCEEDED;
        } else {
            return ResultCode::ERR_UNKNOWN;
        }
    }

    ResultCode remove(folly::StringPiece key) override {
        if (batch_.Delete(toSlice(key)).ok()) {
            return ResultCode::SUCCEEDED;
        } else {
            return ResultCode::ERR_UNKNOWN;
        }
    }

    ResultCode removePrefix(folly::StringPiece prefix) override {
        rocksdb::Slice pre(prefix.begin(), prefix.size());
        rocksdb::ReadOptions options;
        std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(options));
        iter->Seek(pre);
        while (iter->Valid()) {
            if (iter->key().starts_with(pre)) {
                if (!batch_.Delete(iter->key()).ok()) {
                    return ResultCode::ERR_UNKNOWN;
                }
            } else {
                // Done
                break;
            }
            iter->Next();
        }
        return ResultCode::SUCCEEDED;
    }

    // Remove all keys in the range [start, end)
    ResultCode removeRange(folly::StringPiece start, folly::StringPiece end) override {
        if (batch_.DeleteRange(toSlice(start), toSlice(end)).ok()) {
            return ResultCode::SUCCEEDED;
        } else {
            return ResultCode::ERR_UNKNOWN;
        }
    }

    rocksdb::WriteBatch* data() {
        return &batch_;
    }
};

}  // Anonymous namespace


/***************************************
 *
 * Implementation of WriteBatch
 *
 **************************************/
RocksEngine::RocksEngine(GraphSpaceID spaceId,
                         const std::string& dataPath,
                         std::shared_ptr<rocksdb::MergeOperator> mergeOp,
                         std::shared_ptr<rocksdb::CompactionFilterFactory> cfFactory)
        : KVEngine(spaceId)
        , dataPath_(folly::stringPrintf("%s/nebula/%d", dataPath.c_str(), spaceId)) {
    auto path = folly::stringPrintf("%s/data", dataPath_.c_str());
    if (FileUtils::fileType(path.c_str()) == FileType::NOTEXIST) {
        FileUtils::makeDir(path);
    }
    LOG(INFO) << "open rocksdb on " << path;

    rocksdb::Options options;
    rocksdb::DB* db = nullptr;
    rocksdb::Status status = initRocksdbOptions(options);
    CHECK(status.ok());
    if (mergeOp != nullptr) {
        options.merge_operator = mergeOp;
    }
    if (cfFactory != nullptr) {
        options.compaction_filter_factory = cfFactory;
    }
    status = rocksdb::DB::Open(options, path, &db);
    CHECK(status.ok()) << status.ToString();
    db_.reset(db);
    partsNum_ = allParts().size();
}


std::unique_ptr<WriteBatch> RocksEngine::startBatchWrite() {
    return std::make_unique<RocksWriteBatch>(db_.get());
}


ResultCode RocksEngine::commitBatchWrite(std::unique_ptr<WriteBatch> batch) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    auto* b = static_cast<RocksWriteBatch*>(batch.get());
    rocksdb::Status status = db_->Write(options, b->data());
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    }
    return ResultCode::ERR_UNKNOWN;
}


ResultCode RocksEngine::get(const std::string& key, std::string* value) {
    rocksdb::ReadOptions options;
    rocksdb::Status status = db_->Get(options, rocksdb::Slice(key), value);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else if (status.IsNotFound()) {
        VLOG(3) << "Get: " << key << " Not Found";
        return ResultCode::ERR_KEY_NOT_FOUND;
    } else {
        VLOG(3) << "Get Failed: " << key << " " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}


ResultCode RocksEngine::multiGet(const std::vector<std::string>& keys,
                                 std::vector<std::string>* values) {
    rocksdb::ReadOptions options;
    std::vector<rocksdb::Slice> slices;
    for (size_t index = 0; index < keys.size(); index++) {
        slices.emplace_back(keys[index]);
    }

    std::vector<rocksdb::Status> status = db_->MultiGet(options, slices, values);
    auto code = std::all_of(status.begin(), status.end(),
                            [](rocksdb::Status s) {
                                return s.ok();
                            });
    if (code) {
        return ResultCode::SUCCEEDED;
    } else {
        return ResultCode::ERR_UNKNOWN;
    }
}


ResultCode RocksEngine::range(const std::string& start,
                              const std::string& end,
                              std::unique_ptr<KVIterator>* storageIter) {
    rocksdb::ReadOptions options;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(start));
    }
    storageIter->reset(new RocksRangeIter(iter, start, end));
    return ResultCode::SUCCEEDED;
}


ResultCode RocksEngine::prefix(const std::string& prefix,
                               std::unique_ptr<KVIterator>* storageIter) {
    rocksdb::ReadOptions options;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(prefix));
    }
    storageIter->reset(new RocksPrefixIter(iter, prefix));
    return ResultCode::SUCCEEDED;
}


ResultCode RocksEngine::put(std::string key, std::string value) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    rocksdb::Status status = db_->Put(options, key, value);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        VLOG(3) << "Put Failed: " << key << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}


ResultCode RocksEngine::multiPut(std::vector<KV> keyValues) {
    rocksdb::WriteBatch updates(FLAGS_rocksdb_batch_size);
    for (size_t i = 0; i < keyValues.size(); i++) {
        updates.Put(keyValues[i].first, keyValues[i].second);
    }
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    rocksdb::Status status = db_->Write(options, &updates);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        VLOG(3) << "MultiPut Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}


ResultCode RocksEngine::remove(const std::string& key) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    auto status = db_->Delete(options, key);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        VLOG(3) << "Remove Failed: " << key << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}


ResultCode RocksEngine::multiRemove(std::vector<std::string> keys) {
    rocksdb::WriteBatch deletes(FLAGS_rocksdb_batch_size);
    for (size_t i = 0; i < keys.size(); i++) {
        deletes.Delete(keys[i]);
    }
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    rocksdb::Status status = db_->Write(options, &deletes);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        VLOG(3) << "MultiRemove Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}


ResultCode RocksEngine::removeRange(const std::string& start,
                                    const std::string& end) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    // TODO(sye) Given the RocksDB version we are using,
    // we should avoud using DeleteRange
    auto status = db_->DeleteRange(options, db_->DefaultColumnFamily(), start, end);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        VLOG(3) << "RemoveRange Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}


ResultCode RocksEngine::removePrefix(const std::string& prefix) {
    rocksdb::Slice pre(prefix.data(), prefix.size());
    rocksdb::ReadOptions readOptions;
    rocksdb::WriteBatch batch;
    std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(readOptions));
    iter->Seek(pre);
    while (iter->Valid()) {
        if (iter->key().starts_with(pre)) {
            auto status = batch.Delete(iter->key());
            if (!status.ok()) {
                return ResultCode::ERR_UNKNOWN;
            }
        } else {
            // Done
            break;
        }
        iter->Next();
    }

    rocksdb::WriteOptions writeOptions;
    writeOptions.disableWAL = FLAGS_rocksdb_disable_wal;
    if (db_->Write(writeOptions, &batch).ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        return ResultCode::ERR_UNKNOWN;
    }
}


std::string RocksEngine::partKey(PartitionID partId) {
    return NebulaKeyUtils::systemPartKey(partId);
}


void RocksEngine::addPart(PartitionID partId) {
    auto ret = put(partKey(partId), "");
    if (ret == ResultCode::SUCCEEDED) {
        partsNum_++;
        CHECK_GE(partsNum_, 0);
    }
}


void RocksEngine::removePart(PartitionID partId) {
     rocksdb::WriteOptions options;
     options.disableWAL = FLAGS_rocksdb_disable_wal;
     auto status = db_->Delete(options, partKey(partId));
     if (status.ok()) {
         partsNum_--;
         CHECK_GE(partsNum_, 0);
     }
}


std::vector<PartitionID> RocksEngine::allParts() {
    std::unique_ptr<KVIterator> iter;
    static const std::string prefixStr = NebulaKeyUtils::systemPrefix();
    CHECK_EQ(ResultCode::SUCCEEDED, this->prefix(prefixStr, &iter));

    std::vector<PartitionID> parts;
    while (iter->valid()) {
        auto key = iter->key();
        CHECK_EQ(key.size(), sizeof(PartitionID) + sizeof(NebulaSystemKeyType));
        PartitionID partId = *reinterpret_cast<const PartitionID*>(key.data());
        if (!NebulaKeyUtils::isSystemPart(key)) {
            VLOG(3) << "Skip: " << std::bitset<32>(partId);
            iter->next();
            continue;
        }

        partId = partId >> 8;
        parts.emplace_back(partId);
        iter->next();
    }
    return parts;
}


int32_t RocksEngine::totalPartsNum() {
    return partsNum_;
}


ResultCode RocksEngine::ingest(const std::vector<std::string>& files) {
    rocksdb::IngestExternalFileOptions options;
    rocksdb::Status status = db_->IngestExternalFile(files, options);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "Ingest Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}


ResultCode RocksEngine::setOption(const std::string& configKey,
                                  const std::string& configValue) {
    std::unordered_map<std::string, std::string> configOptions = {
        {configKey, configValue}
    };

    rocksdb::Status status = db_->SetOptions(configOptions);
    if (status.ok()) {
        LOG(INFO) << "SetOption Succeeded: " << configKey << ":" << configValue;
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "SetOption Failed: " << configKey << ":" << configValue;
        return ResultCode::ERR_INVALID_ARGUMENT;
    }
}


ResultCode RocksEngine::setDBOption(const std::string& configKey,
                                    const std::string& configValue) {
    std::unordered_map<std::string, std::string> configOptions = {
        {configKey, configValue}
    };

    rocksdb::Status status = db_->SetDBOptions(configOptions);
    if (status.ok()) {
        LOG(INFO) << "SetDBOption Succeeded: " << configKey << ":" << configValue;
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "SetDBOption Failed: " << configKey << ":" << configValue;
        return ResultCode::ERR_INVALID_ARGUMENT;
    }
}


ResultCode RocksEngine::compact() {
    rocksdb::CompactRangeOptions options;
    rocksdb::Status status = db_->CompactRange(options, nullptr, nullptr);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "CompactAll Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}

ResultCode RocksEngine::flush() {
    rocksdb::FlushOptions options;
    rocksdb::Status status = db_->Flush(options);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "Flush Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}

}  // namespace kvstore
}  // namespace nebula
