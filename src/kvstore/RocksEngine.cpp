/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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

const char* kSystemParts = "__system__parts__";

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
    explicit RocksWriteBatch(rocksdb::DB* db) : db_(db) {}

    virtual ~RocksWriteBatch() = default;

    ResultCode put(folly::StringPiece key, folly::StringPiece value) override {
        if (batch_.Put(toSlice(key), toSlice(value)).ok()) {
            return ResultCode::SUCCEEDED;
        } else {
            return ResultCode::ERR_UNKNOWN;
        }
    }

    ResultCode putFromLog(folly::StringPiece log) override {
        uint32_t len = *(reinterpret_cast<const uint32_t*>(log.begin()));
        rocksdb::Slice key(log.begin() + sizeof(uint32_t), len);
        auto vBegin = log.begin() + 2 * sizeof(uint32_t) + len;
        len = *(reinterpret_cast<const uint32_t*>(log.begin() + sizeof(uint32_t) + len));
        DCHECK_EQ(vBegin + len, log.begin() + log.size());
        rocksdb::Slice value(vBegin, len);

        if (batch_.Put(key, value).ok()) {
            return ResultCode::SUCCEEDED;
        } else {
            return ResultCode::ERR_UNKNOWN;
        }
    }

    ResultCode multiPut(const std::vector<KV>& keyValues) override {
        for (auto kv : keyValues) {
            if (!batch_.Put(kv.first, kv.second).ok()) {
                return ResultCode::ERR_UNKNOWN;
            }
        }
        return ResultCode::SUCCEEDED;
    }

    ResultCode multiPutFromLog(folly::StringPiece log) override {
        uint32_t numKVs = *(reinterpret_cast<const uint32_t*>(log.begin()));
        auto* p = log.begin() + sizeof(uint32_t);
        for (auto i = 0U; i < numKVs; i++) {
            uint32_t len = *(reinterpret_cast<const uint32_t*>(p));
            rocksdb::Slice key(p + sizeof(uint32_t), len);

            auto vBegin = p + 2 * sizeof(uint32_t) + len;
            len = *(reinterpret_cast<const uint32_t*>(p + sizeof(uint32_t) + len));
            DCHECK_LE(vBegin + len, log.begin() + log.size());
            rocksdb::Slice value(vBegin, len);

            if (!batch_.Put(key, value).ok()) {
                return ResultCode::ERR_UNKNOWN;
            }

            p = vBegin + len;
        }

        return ResultCode::SUCCEEDED;
    }

    ResultCode remove(folly::StringPiece key) override {
        if (batch_.Delete(toSlice(key)).ok()) {
            return ResultCode::SUCCEEDED;
        } else {
            return ResultCode::ERR_UNKNOWN;
        }
    }

    ResultCode removeFromLog(folly::StringPiece log) override {
        uint32_t len = *(reinterpret_cast<const uint32_t*>(log.begin()));
        rocksdb::Slice key(log.begin() + sizeof(uint32_t), len);
        DCHECK_EQ(log.begin() + sizeof(uint32_t) + len, log.begin() + log.size());

        if (batch_.Delete(key).ok()) {
            return ResultCode::SUCCEEDED;
        } else {
            return ResultCode::ERR_UNKNOWN;
        }
    }

    ResultCode multiRemove(const std::vector<std::string>& keys) override {
        for (auto k : keys) {
            if (!batch_.Delete(k).ok()) {
                return ResultCode::ERR_UNKNOWN;
            }
        }
        return ResultCode::SUCCEEDED;
    }

    ResultCode multiRemoveFromLog(folly::StringPiece log) override {
        uint32_t numKeys = *(reinterpret_cast<const uint32_t*>(log.begin()));
        auto* p = log.begin() + sizeof(uint32_t);
        for (auto i = 0U; i < numKeys; i++) {
            uint32_t len = *(reinterpret_cast<const uint32_t*>(p));
            rocksdb::Slice key(p + sizeof(uint32_t), len);
            DCHECK_LE(p + sizeof(uint32_t) + len, log.begin() + log.size());

            if (!batch_.Delete(key).ok()) {
                return ResultCode::ERR_UNKNOWN;
            }

            p += sizeof(uint32_t) + len;
        }

        return ResultCode::SUCCEEDED;
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

    ResultCode removePrefixFromLog(folly::StringPiece log) override {
        uint32_t len = *(reinterpret_cast<const uint32_t*>(log.begin()));
        rocksdb::Slice prefix(log.begin() + sizeof(uint32_t), len);
        DCHECK_EQ(log.begin() + sizeof(uint32_t) + len, log.begin() + log.size());

        rocksdb::ReadOptions options;
        std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(options));
        iter->Seek(prefix);
        while (iter->Valid()) {
            if (iter->key().starts_with(prefix)) {
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

    ResultCode removeRangeFromLog(folly::StringPiece log) override {
        uint32_t len = *(reinterpret_cast<const uint32_t*>(log.begin()));
        rocksdb::Slice begin(log.begin() + sizeof(uint32_t), len);
        auto eBegin = log.begin() + 2 * sizeof(uint32_t) + len;
        len = *(reinterpret_cast<const uint32_t*>(log.begin() + sizeof(uint32_t) + len));
        DCHECK_EQ(eBegin + len, log.begin() + log.size());
        rocksdb::Slice end(eBegin, len);

        if (batch_.DeleteRange(begin, end).ok()) {
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
                         const std::string& path,
                         std::shared_ptr<rocksdb::MergeOperator> mergeOp,
                         std::shared_ptr<rocksdb::CompactionFilterFactory> cfFactory)
        : KVEngine(spaceId)
        , dataRoot_(folly::stringPrintf("%s/nebula/%d", path.c_str(), spaceId)) {
    auto dataPath = folly::stringPrintf("%s/data", dataRoot_.c_str());
    LOG(INFO) << "open rocksdb on " << dataPath;
    if (FileUtils::fileType(dataPath.c_str()) == FileType::NOTEXIST) {
        FileUtils::makeDir(dataPath);
    }

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
    status = rocksdb::DB::Open(options, dataPath, &db);
    CHECK(status.ok());
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


ErrorOr<ResultCode, std::string> RocksEngine::get(folly::StringPiece key) {
    rocksdb::ReadOptions options;
    std::string value;
    rocksdb::Status status = db_->Get(options, toSlice(key), &value);
    if (status.ok()) {
        return std::move(value);
    } else if (status.IsNotFound()) {
        LOG(ERROR) << "Get: " << key << " Not Found";
        return ResultCode::ERR_KEY_NOT_FOUND;
    } else {
        LOG(ERROR) << "Get Failed: " << key << " " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}


ErrorOr<ResultCode, std::vector<std::string>>
RocksEngine::multiGet(const std::vector<std::string>& keys) {
    rocksdb::ReadOptions options;
    std::vector<rocksdb::Slice> slices;
    for (auto& k : keys) {
        slices.emplace_back(k);
    }

    std::vector<std::string> values;
    std::vector<rocksdb::Status> status = db_->MultiGet(options, slices, &values);
    auto code = std::all_of(status.begin(),
                            status.end(),
                            [](rocksdb::Status s) {
                                return s.ok();
                            });
    if (code) {
        return std::move(values);
    } else {
        return ResultCode::ERR_UNKNOWN;
    }
}


ErrorOr<ResultCode, std::unique_ptr<KVIterator>> RocksEngine::range(
        folly::StringPiece start,
        folly::StringPiece end) {
    rocksdb::ReadOptions options;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(toSlice(start));
    }
    return std::make_unique<RocksRangeIter>(iter, toSlice(start), toSlice(end));
}


ErrorOr<ResultCode, std::unique_ptr<KVIterator>> RocksEngine::prefix(
        folly::StringPiece prefix) {
    rocksdb::ReadOptions options;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(toSlice(prefix));
    }
    return std::make_unique<RocksPrefixIter>(iter, toSlice(prefix));
}


ResultCode RocksEngine::put(folly::StringPiece key, folly::StringPiece value) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    rocksdb::Status status = db_->Put(options, toSlice(key), toSlice(value));
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "Put Failed: " << key << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}


ResultCode RocksEngine::multiPut(const std::vector<KV>& keyValues) {
    rocksdb::WriteBatch updates(FLAGS_batch_reserved_bytes);
    for (size_t i = 0; i < keyValues.size(); i++) {
        updates.Put(keyValues[i].first, keyValues[i].second);
    }
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    rocksdb::Status status = db_->Write(options, &updates);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "MultiPut Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}


ResultCode RocksEngine::remove(folly::StringPiece key) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    auto status = db_->Delete(options, toSlice(key));
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "Remove Failed: " << key << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}


ResultCode RocksEngine::multiRemove(const std::vector<std::string>& keys) {
    rocksdb::WriteBatch deletes(FLAGS_batch_reserved_bytes);
    for (size_t i = 0; i < keys.size(); i++) {
        deletes.Delete(keys[i]);
    }
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    rocksdb::Status status = db_->Write(options, &deletes);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "MultiRemove Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}


ResultCode RocksEngine::removeRange(folly::StringPiece start,
                                    folly::StringPiece end) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    auto status = db_->DeleteRange(options,
                                   db_->DefaultColumnFamily(),
                                   toSlice(start),
                                   toSlice(end));
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "RemoveRange Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}


ResultCode RocksEngine::removePrefix(folly::StringPiece prefix) {
    rocksdb::Slice pre(prefix.begin(), prefix.size());
    rocksdb::ReadOptions readOptions;
    rocksdb::WriteOptions writeOptions;
    writeOptions.disableWAL = FLAGS_rocksdb_disable_wal;
    std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(readOptions));
    iter->Seek(pre);
    while (iter->Valid()) {
        if (iter->key().starts_with(pre)) {
            auto status = db_->Delete(writeOptions, iter->key());
            if (!status.ok()) {
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


std::string RocksEngine::partKey(PartitionID partId) {
    std::string key;
    static const size_t prefixLen = ::strlen(kSystemParts);
    key.reserve(prefixLen + sizeof(PartitionID));
    key.append(kSystemParts, prefixLen);
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
    return key;
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
    static const size_t prefixLen = ::strlen(kSystemParts);
    static const std::string prefixStr(kSystemParts, prefixLen);
    auto res = this->prefix(prefixStr);
    CHECK(ok(res));

    std::unique_ptr<KVIterator> iter = value(std::move(res));
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
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "SetDBOption Failed: " << configKey << ":" << configValue;
        return ResultCode::ERR_INVALID_ARGUMENT;
    }
}

ResultCode RocksEngine::compactAll() {
    rocksdb::CompactRangeOptions options;
    rocksdb::Status status = db_->CompactRange(options, nullptr, nullptr);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "CompactAll Failed: " << status.ToString();
        return ResultCode::ERR_UNKNOWN;
    }
}

}  // namespace kvstore
}  // namespace nebula

