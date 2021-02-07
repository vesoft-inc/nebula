/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "kvstore/RocksEngine.h"
#include <folly/String.h>
#include <rocksdb/convenience.h>
#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "kvstore/KVStore.h"
#include "kvstore/RocksEngineConfig.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace kvstore {

using fs::FileType;
using fs::FileUtils;

namespace {

/***************************************
 *
 * Implementation of WriteBatch
 *
 **************************************/
class RocksWriteBatch : public WriteBatch {
private:
    rocksdb::WriteBatch batch_;

public:
    RocksWriteBatch() : batch_(FLAGS_rocksdb_batch_size) {}

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

}   // Anonymous namespace

/***************************************
 *
 * Implementation of WriteBatch
 *
 **************************************/
RocksEngine::RocksEngine(GraphSpaceID spaceId,
                         int32_t vIdLen,
                         const std::string& dataPath,
                         std::shared_ptr<rocksdb::MergeOperator> mergeOp,
                         std::shared_ptr<rocksdb::CompactionFilterFactory> cfFactory)
    : KVEngine(spaceId), dataPath_(folly::stringPrintf("%s/nebula/%d", dataPath.c_str(), spaceId)) {
    auto path = folly::stringPrintf("%s/data", dataPath_.c_str());
    if (FileUtils::fileType(path.c_str()) == FileType::NOTEXIST) {
        if (!FileUtils::makeDir(path)) {
            LOG(FATAL) << "makeDir " << path << " failed";
        }
    }

    if (FileUtils::fileType(path.c_str()) != FileType::DIRECTORY) {
        LOG(FATAL) << path << " is not directory";
    }

    rocksdb::Options options;
    rocksdb::DB* db = nullptr;
    rocksdb::Status status = initRocksdbOptions(options, vIdLen);
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
    LOG(INFO) << "open rocksdb on " << path;
}

void RocksEngine::stop() {
    if (db_) {
        // Because we trigger compaction in WebService, we need to stop all background work
        // before we stop HttpServer.
        rocksdb::CancelAllBackgroundWork(db_.get(), true);
    }
}

std::unique_ptr<WriteBatch> RocksEngine::startBatchWrite() {
    return std::make_unique<RocksWriteBatch>();
}

ResultCode RocksEngine::commitBatchWrite(std::unique_ptr<WriteBatch> batch,
                                         bool disableWAL,
                                         bool sync) {
    rocksdb::WriteOptions options;
    options.disableWAL = disableWAL;
    options.sync = sync;
    auto* b = static_cast<RocksWriteBatch*>(batch.get());
    rocksdb::Status status = db_->Write(options, b->data());
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    }
    LOG(ERROR) << "Write into rocksdb failed because of " << status.ToString();
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

std::vector<Status> RocksEngine::multiGet(const std::vector<std::string>& keys,
                                          std::vector<std::string>* values) {
    rocksdb::ReadOptions options;
    std::vector<rocksdb::Slice> slices;
    for (size_t index = 0; index < keys.size(); index++) {
        slices.emplace_back(keys[index]);
    }

    auto status = db_->MultiGet(options, slices, values);
    std::vector<Status> ret;
    std::transform(status.begin(), status.end(), std::back_inserter(ret), [](const auto& s) {
        if (s.ok()) {
            return Status::OK();
        } else if (s.IsNotFound()) {
            return Status::KeyNotFound();
        } else {
            return Status::Error();
        }
    });
    return ret;
}

ResultCode RocksEngine::range(const std::string& start,
                              const std::string& end,
                              std::unique_ptr<KVIterator>* storageIter) {
    rocksdb::ReadOptions options;
    options.total_order_seek = true;
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
    options.prefix_same_as_start = true;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(prefix));
    }
    storageIter->reset(new RocksPrefixIter(iter, prefix));
    return ResultCode::SUCCEEDED;
}

ResultCode RocksEngine::rangeWithPrefix(const std::string& start,
                                        const std::string& prefix,
                                        std::unique_ptr<KVIterator>* storageIter) {
    rocksdb::ReadOptions options;
    options.prefix_same_as_start = true;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(start));
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

ResultCode RocksEngine::removeRange(const std::string& start, const std::string& end) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    auto status = db_->DeleteRange(options, db_->DefaultColumnFamily(), start, end);
    if (status.ok()) {
        return ResultCode::SUCCEEDED;
    } else {
        VLOG(3) << "RemoveRange Failed: " << status.ToString();
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

ResultCode RocksEngine::setOption(const std::string& configKey, const std::string& configValue) {
    std::unordered_map<std::string, std::string> configOptions = {{configKey, configValue}};

    rocksdb::Status status = db_->SetOptions(configOptions);
    if (status.ok()) {
        LOG(INFO) << "SetOption Succeeded: " << configKey << ":" << configValue;
        return ResultCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "SetOption Failed: " << configKey << ":" << configValue;
        return ResultCode::ERR_INVALID_ARGUMENT;
    }
}

ResultCode RocksEngine::setDBOption(const std::string& configKey, const std::string& configValue) {
    std::unordered_map<std::string, std::string> configOptions = {{configKey, configValue}};

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
    options.change_level = FLAGS_rocksdb_compact_change_level;
    options.target_level = FLAGS_rocksdb_compact_target_level;
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

ResultCode RocksEngine::createCheckpoint(const std::string& name) {
    LOG(INFO) << "Begin checkpoint : " << dataPath_;

    /*
     * The default checkpoint directory structure is :
     *   |--FLAGS_data_path
     *   |----nebula
     *   |------space1
     *   |--------data
     *   |--------wal
     *   |--------checkpoints
     *   |----------snapshot1
     *   |------------data
     *   |------------wal
     *   |----------snapshot2
     *   |----------snapshot3
     *
     */

    auto checkpointPath = folly::stringPrintf("%s/checkpoints/%s/data",
                                              dataPath_.c_str(), name.c_str());
    LOG(INFO) << "Target checkpoint path : " << checkpointPath;
    if (fs::FileUtils::exist(checkpointPath) &&
        !fs::FileUtils::remove(checkpointPath.data(), true)) {
        LOG(ERROR) << "Remove exist dir failed of checkpoint : " << checkpointPath;
        return ResultCode::ERR_IO_ERROR;
    }

    auto parent = checkpointPath.substr(0, checkpointPath.rfind('/'));
    if (!FileUtils::exist(parent)) {
        if (!FileUtils::makeDir(parent)) {
            LOG(ERROR) << "Make dir " << parent << " failed";
            return ResultCode::ERR_UNKNOWN;
        }
    }

    rocksdb::Checkpoint* checkpoint;
    rocksdb::Status status = rocksdb::Checkpoint::Create(db_.get(), &checkpoint);
    std::unique_ptr<rocksdb::Checkpoint> cp(checkpoint);
    if (!status.ok()) {
        LOG(ERROR) << "Init checkpoint Failed: " << status.ToString();
        return ResultCode::ERR_CHECKPOINT_ERROR;
    }
    status = cp->CreateCheckpoint(checkpointPath, 0);
    if (!status.ok()) {
        LOG(ERROR) << "Create checkpoint Failed: " << status.ToString();
        return ResultCode::ERR_CHECKPOINT_ERROR;
    }
    return ResultCode::SUCCEEDED;
}

ErrorOr<ResultCode, std::string> RocksEngine::backupTable(
    const std::string& name,
    const std::string& tablePrefix,
    std::function<bool(const folly::StringPiece& key)> filter) {
    auto backupPath = folly::stringPrintf(
        "%s/checkpoints/%s/%s.sst", dataPath_.c_str(), name.c_str(), tablePrefix.c_str());
    VLOG(3) << "Start writing the sst file with table (" << tablePrefix
            << ") to file: " << backupPath;

    auto parent = backupPath.substr(0, backupPath.rfind('/'));
    if (!FileUtils::exist(parent)) {
        if (!FileUtils::makeDir(parent)) {
            LOG(ERROR) << "Make dir " << parent << " failed";
            return ResultCode::ERR_BACKUP_EXISTED;
        }
    }

    rocksdb::Options options;
    rocksdb::SstFileWriter sstFileWriter(rocksdb::EnvOptions(), options);

    std::unique_ptr<KVIterator> iter;
    auto ret = prefix(tablePrefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ResultCode::ERR_BACKUP_EMPTY_TABLE;
    }

    if (!iter->valid()) {
        return ResultCode::ERR_BACKUP_EMPTY_TABLE;
    }

    auto s = sstFileWriter.Open(backupPath);
    if (!s.ok()) {
        LOG(ERROR) << "BackupTable failed, path: " << backupPath << ", error: " << s.ToString();
        return ResultCode::ERR_BACKUP_OPEN_FAILED;
    }

    while (iter->valid()) {
        if (filter && filter(iter->key())) {
            iter->next();
            continue;
        }
        s = sstFileWriter.Put(iter->key().toString(), iter->val().toString());
        if (!s.ok()) {
            LOG(ERROR) << "BackupTable failed, path: " << backupPath << ", error: " << s.ToString();
            sstFileWriter.Finish();
            return ResultCode::ERR_BACKUP_TABLE_FAILED;
        }
        iter->next();
    }

    s = sstFileWriter.Finish();
    if (!s.ok()) {
        LOG(WARNING) << "Failure to insert data when backupTable,  " << backupPath
                     << ", error: " << s.ToString();
        return ResultCode::ERR_BACKUP_EMPTY_TABLE;
    }

    if (sstFileWriter.FileSize() == 0) {
        return ResultCode::ERR_BACKUP_EMPTY_TABLE;
    }

    return backupPath;
}

}   // namespace kvstore
}   // namespace nebula
