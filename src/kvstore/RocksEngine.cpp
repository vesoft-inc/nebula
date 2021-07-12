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

DEFINE_bool(move_files, false,
            "Move the SST files instead of copy when ingest into dataset");

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

    nebula::cpp2::ErrorCode
    put(folly::StringPiece key, folly::StringPiece value) override {
        if (batch_.Put(toSlice(key), toSlice(value)).ok()) {
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        } else {
            return nebula::cpp2::ErrorCode::E_UNKNOWN;
        }
    }

    nebula::cpp2::ErrorCode remove(folly::StringPiece key) override {
        if (batch_.Delete(toSlice(key)).ok()) {
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        } else {
            return nebula::cpp2::ErrorCode::E_UNKNOWN;
        }
    }

    // Remove all keys in the range [start, end)
    nebula::cpp2::ErrorCode
    removeRange(folly::StringPiece start, folly::StringPiece end) override {
        if (batch_.DeleteRange(toSlice(start), toSlice(end)).ok()) {
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        } else {
            return nebula::cpp2::ErrorCode::E_UNKNOWN;
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
                         const std::string& walPath,
                         std::shared_ptr<rocksdb::MergeOperator> mergeOp,
                         std::shared_ptr<rocksdb::CompactionFilterFactory> cfFactory,
                         bool readonly)
    : KVEngine(spaceId)
    , spaceId_(spaceId)
    , dataPath_(folly::stringPrintf("%s/nebula/%d", dataPath.c_str(), spaceId)) {
    // set wal path as dataPath by default
    if (walPath.empty()) {
        walPath_ = folly::stringPrintf("%s/nebula/%d", dataPath.c_str(), spaceId);
    } else {
        walPath_ = folly::stringPrintf("%s/nebula/%d", walPath.c_str(), spaceId);
    }
    auto path = folly::stringPrintf("%s/data", dataPath_.c_str());
    if (FileUtils::fileType(path.c_str()) == FileType::NOTEXIST) {
        if (readonly) {
            LOG(FATAL) << "Path " << path << " not exist";
        } else {
            if (!FileUtils::makeDir(path)) {
                LOG(FATAL) << "makeDir " << path << " failed";
            }
        }
    }

    if (FileUtils::fileType(path.c_str()) != FileType::DIRECTORY) {
        LOG(FATAL) << path << " is not directory";
    }

    openBackupEngine(spaceId);

    rocksdb::Options options;
    rocksdb::DB* db = nullptr;
    rocksdb::Status status = initRocksdbOptions(options, spaceId, vIdLen);
    CHECK(status.ok()) << status.ToString();
    if (mergeOp != nullptr) {
        options.merge_operator = mergeOp;
    }
    if (cfFactory != nullptr) {
        options.compaction_filter_factory = cfFactory;
    }

    if (readonly) {
        status = rocksdb::DB::OpenForReadOnly(options, path, &db);
    } else {
        status = rocksdb::DB::Open(options, path, &db);
    }
    CHECK(status.ok()) << status.ToString();
    db_.reset(db);
    partsNum_ = allParts().size();
    LOG(INFO) << "open rocksdb on " << path;

    backup();
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

nebula::cpp2::ErrorCode
RocksEngine::commitBatchWrite(std::unique_ptr<WriteBatch> batch,
                              bool disableWAL,
                              bool sync,
                              bool wait) {
    rocksdb::WriteOptions options;
    options.disableWAL = disableWAL;
    options.sync = sync;
    options.no_slowdown = !wait;
    auto* b = static_cast<RocksWriteBatch*>(batch.get());
    rocksdb::Status status = db_->Write(options, b->data());
    if (status.ok()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else if (!wait && status.IsIncomplete()) {
        return nebula::cpp2::ErrorCode::E_WRITE_STALLED;
    }
    LOG(ERROR) << "Write into rocksdb failed because of " << status.ToString();
    return nebula::cpp2::ErrorCode::E_UNKNOWN;
}

nebula::cpp2::ErrorCode RocksEngine::get(const std::string& key, std::string* value) {
    rocksdb::ReadOptions options;
    rocksdb::Status status = db_->Get(options, rocksdb::Slice(key), value);
    if (status.ok()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else if (status.IsNotFound()) {
        VLOG(3) << "Get: " << key << " Not Found";
        return nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND;
    } else {
        VLOG(3) << "Get Failed: " << key << " " << status.ToString();
        return nebula::cpp2::ErrorCode::E_UNKNOWN;
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

nebula::cpp2::ErrorCode
RocksEngine::range(const std::string& start,
                   const std::string& end,
                   std::unique_ptr<KVIterator>* storageIter) {
    rocksdb::ReadOptions options;
    options.total_order_seek = true;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(start));
    }
    storageIter->reset(new RocksRangeIter(iter, start, end));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode
RocksEngine::prefix(const std::string& prefix,
                    std::unique_ptr<KVIterator>* storageIter) {
    rocksdb::ReadOptions options;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(prefix));
    }
    storageIter->reset(new RocksPrefixIter(iter, prefix));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode
RocksEngine::rangeWithPrefix(const std::string& start,
                             const std::string& prefix,
                             std::unique_ptr<KVIterator>* storageIter) {
    rocksdb::ReadOptions options;
    rocksdb::Iterator* iter = db_->NewIterator(options);
    if (iter) {
        iter->Seek(rocksdb::Slice(start));
    }
    storageIter->reset(new RocksPrefixIter(iter, prefix));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode
RocksEngine::put(std::string key, std::string value) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    rocksdb::Status status = db_->Put(options, key, value);
    if (status.ok()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
        VLOG(3) << "Put Failed: " << key << status.ToString();
        return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
}

nebula::cpp2::ErrorCode
RocksEngine::multiPut(std::vector<KV> keyValues) {
    rocksdb::WriteBatch updates(FLAGS_rocksdb_batch_size);
    for (size_t i = 0; i < keyValues.size(); i++) {
        updates.Put(keyValues[i].first, keyValues[i].second);
    }
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    rocksdb::Status status = db_->Write(options, &updates);
    if (status.ok()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
        VLOG(3) << "MultiPut Failed: " << status.ToString();
        return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
}

nebula::cpp2::ErrorCode
RocksEngine::remove(const std::string& key) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    auto status = db_->Delete(options, key);
    if (status.ok()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
        VLOG(3) << "Remove Failed: " << key << status.ToString();
        return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
}

nebula::cpp2::ErrorCode
RocksEngine::multiRemove(std::vector<std::string> keys) {
    rocksdb::WriteBatch deletes(FLAGS_rocksdb_batch_size);
    for (size_t i = 0; i < keys.size(); i++) {
        deletes.Delete(keys[i]);
    }
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    rocksdb::Status status = db_->Write(options, &deletes);
    if (status.ok()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
        VLOG(3) << "MultiRemove Failed: " << status.ToString();
        return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
}

nebula::cpp2::ErrorCode
RocksEngine::removeRange(const std::string& start, const std::string& end) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    auto status = db_->DeleteRange(options, db_->DefaultColumnFamily(), start, end);
    if (status.ok()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
        VLOG(3) << "RemoveRange Failed: " << status.ToString();
        return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
}

std::string RocksEngine::partKey(PartitionID partId) {
    return NebulaKeyUtils::systemPartKey(partId);
}

void RocksEngine::addPart(PartitionID partId) {
    auto ret = put(partKey(partId), "");
    if (ret == nebula::cpp2::ErrorCode::SUCCEEDED) {
        partsNum_++;
        CHECK_GE(partsNum_, 0);
    }
}

void RocksEngine::removePart(PartitionID partId) {
    rocksdb::WriteOptions options;
    options.disableWAL = FLAGS_rocksdb_disable_wal;
    std::vector<std::string> sysKeysToDelete;
    sysKeysToDelete.emplace_back(partKey(partId));
    sysKeysToDelete.emplace_back(NebulaKeyUtils::systemCommitKey(partId));
    auto code = multiRemove(sysKeysToDelete);
    if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
        partsNum_--;
        CHECK_GE(partsNum_, 0);
    }
}

std::vector<PartitionID> RocksEngine::allParts() {
    std::unique_ptr<KVIterator> iter;
    std::vector<PartitionID> parts;
    static const std::string prefixStr = NebulaKeyUtils::systemPrefix();
    auto retCode = this->prefix(prefixStr, &iter);
    if (nebula::cpp2::ErrorCode::SUCCEEDED !=  retCode) {
        return parts;
    }

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

nebula::cpp2::ErrorCode RocksEngine::ingest(const std::vector<std::string>& files,
                                            bool verifyFileChecksum) {
    rocksdb::IngestExternalFileOptions options;
    options.move_files = FLAGS_move_files;
    options.verify_file_checksum = verifyFileChecksum;
    rocksdb::Status status = db_->IngestExternalFile(files, options);
    if (status.ok()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "Ingest Failed: " << status.ToString();
        return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
}

nebula::cpp2::ErrorCode
RocksEngine::setOption(const std::string& configKey, const std::string& configValue) {
    std::unordered_map<std::string, std::string> configOptions = {{configKey, configValue}};

    rocksdb::Status status = db_->SetOptions(configOptions);
    if (status.ok()) {
        LOG(INFO) << "SetOption Succeeded: " << configKey << ":" << configValue;
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "SetOption Failed: " << configKey << ":" << configValue;
        return nebula::cpp2::ErrorCode::E_INVALID_PARM;
    }
}

nebula::cpp2::ErrorCode
RocksEngine::setDBOption(const std::string& configKey, const std::string& configValue) {
    std::unordered_map<std::string, std::string> configOptions = {{configKey, configValue}};

    rocksdb::Status status = db_->SetDBOptions(configOptions);
    if (status.ok()) {
        LOG(INFO) << "SetDBOption Succeeded: " << configKey << ":" << configValue;
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "SetDBOption Failed: " << configKey << ":" << configValue;
        return nebula::cpp2::ErrorCode::E_INVALID_PARM;
    }
}

nebula::cpp2::ErrorCode RocksEngine::compact() {
    rocksdb::CompactRangeOptions options;
    options.change_level = FLAGS_rocksdb_compact_change_level;
    options.target_level = FLAGS_rocksdb_compact_target_level;
    rocksdb::Status status = db_->CompactRange(options, nullptr, nullptr);
    if (status.ok()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "CompactAll Failed: " << status.ToString();
        return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
}

nebula::cpp2::ErrorCode RocksEngine::flush() {
    rocksdb::FlushOptions options;
    rocksdb::Status status = db_->Flush(options);
    if (status.ok()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "Flush Failed: " << status.ToString();
        return nebula::cpp2::ErrorCode::E_UNKNOWN;
    }
}

nebula::cpp2::ErrorCode RocksEngine::backup() {
    if (!backupDb_) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    LOG(INFO) << "begin to backup space " << spaceId_ << " on path " << backupPath_;
    bool flushBeforeBackup = true;
    auto status = backupDb_->CreateNewBackup(db_.get(), flushBeforeBackup);
    if (status.ok()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
        LOG(ERROR) << "backup failed: " << status.ToString();
        return nebula::cpp2::ErrorCode::E_BACKUP_FAILED;
    }
}

void RocksEngine::openBackupEngine(GraphSpaceID spaceId) {
    // If backup dir is not empty, set backup related options
    if (FLAGS_rocksdb_table_format == "PlainTable" && !FLAGS_rocksdb_backup_dir.empty()) {
        backupPath_ =
            folly::stringPrintf("%s/rocksdb_backup/%d", FLAGS_rocksdb_backup_dir.c_str(), spaceId);
        if (FileUtils::fileType(backupPath_.c_str()) == FileType::NOTEXIST) {
            if (!FileUtils::makeDir(backupPath_)) {
                LOG(FATAL) << "makeDir " << backupPath_ << " failed";
            }
        }
        rocksdb::BackupEngine* backupDb;
        rocksdb::BackupableDBOptions backupOptions(backupPath_);
        backupOptions.backup_log_files = false;
        auto status =
            rocksdb::BackupEngine::Open(rocksdb::Env::Default(), backupOptions, &backupDb);
        CHECK(status.ok()) << status.ToString();
        backupDb_.reset(backupDb);
        LOG(INFO) << "open plain table backup engine on " << backupPath_;

        std::string dataPath = folly::stringPrintf("%s/data", dataPath_.c_str());
        auto walDir = dataPath;
        if (!FLAGS_rocksdb_wal_dir.empty()) {
            walDir =
                folly::stringPrintf("%s/rocksdb_wal/%d", FLAGS_rocksdb_wal_dir.c_str(), spaceId);
        } else {
            LOG(WARNING) << "rocksdb wal is stored with data";
        }

        rocksdb::RestoreOptions restoreOptions;
        restoreOptions.keep_log_files = true;
        status = backupDb_->RestoreDBFromLatestBackup(dataPath, walDir, restoreOptions);
        LOG(INFO) << "try to restore from backup path " << backupPath_;
        if (status.IsNotFound()) {
            LOG(WARNING) << "no valid backup found";
            return;
        } else if (!status.ok()) {
            LOG(FATAL) << status.ToString();
        }
        LOG(INFO) << "restore from latest backup succesfully"
                  << ", backup path " << backupPath_
                  << ", wal path " << walDir
                  << ", data path " << dataPath;
    }
}

nebula::cpp2::ErrorCode
RocksEngine::createCheckpoint(const std::string& name) {
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
        return nebula::cpp2::ErrorCode::E_STORE_FAILURE;
    }

    auto parent = checkpointPath.substr(0, checkpointPath.rfind('/'));
    if (!FileUtils::exist(parent)) {
        if (!FileUtils::makeDir(parent)) {
            LOG(ERROR) << "Make dir " << parent << " failed";
            return nebula::cpp2::ErrorCode::E_UNKNOWN;
        }
    }

    rocksdb::Checkpoint* checkpoint;
    rocksdb::Status status = rocksdb::Checkpoint::Create(db_.get(), &checkpoint);
    std::unique_ptr<rocksdb::Checkpoint> cp(checkpoint);
    if (!status.ok()) {
        LOG(ERROR) << "Init checkpoint Failed: " << status.ToString();
        return nebula::cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT;
    }
    status = cp->CreateCheckpoint(checkpointPath, 0);
    if (!status.ok()) {
        LOG(ERROR) << "Create checkpoint Failed: " << status.ToString();
        return nebula::cpp2::ErrorCode::E_FAILED_TO_CHECKPOINT;
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<nebula::cpp2::ErrorCode, std::string> RocksEngine::backupTable(
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
            return nebula::cpp2::ErrorCode::E_BACKUP_FAILED;
        }
    }

    rocksdb::Options options;
    options.file_checksum_gen_factory = rocksdb::GetFileChecksumGenCrc32cFactory();
    rocksdb::SstFileWriter sstFileWriter(rocksdb::EnvOptions(), options);

    std::unique_ptr<KVIterator> iter;
    auto ret = prefix(tablePrefix, &iter);
    if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return nebula::cpp2::ErrorCode::E_BACKUP_EMPTY_TABLE;
    }

    if (!iter->valid()) {
        return nebula::cpp2::ErrorCode::E_BACKUP_EMPTY_TABLE;
    }

    auto s = sstFileWriter.Open(backupPath);
    if (!s.ok()) {
        LOG(ERROR) << "BackupTable failed, path: " << backupPath << ", error: " << s.ToString();
        return nebula::cpp2::ErrorCode::E_BACKUP_TABLE_FAILED;
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
            return nebula::cpp2::ErrorCode::E_BACKUP_TABLE_FAILED;
        }
        iter->next();
    }

    s = sstFileWriter.Finish();
    if (!s.ok()) {
        LOG(WARNING) << "Failure to insert data when backupTable,  " << backupPath
                     << ", error: " << s.ToString();
        return nebula::cpp2::ErrorCode::E_BACKUP_EMPTY_TABLE;
    }

    if (sstFileWriter.FileSize() == 0) {
        return nebula::cpp2::ErrorCode::E_BACKUP_EMPTY_TABLE;
    }

    if (backupPath[0] == '/') {
        return backupPath;
    }

    auto result = nebula::fs::FileUtils::realPath(backupPath.c_str());
    if (!result.ok()) {
        return nebula::cpp2::ErrorCode::E_BACKUP_TABLE_FAILED;
    }
    return result.value();
}

}   // namespace kvstore
}   // namespace nebula
