/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"

namespace nebula {
namespace kvstore {

class EventListener : public rocksdb::EventListener {
public:
    // A callback function to RocksDB which will be called before a RocksDB starts to compact.
    void OnCompactionBegin(rocksdb::DB*, const rocksdb::CompactionJobInfo& info) override {
        LOG(INFO) << "Rocksdb start compaction column family: " << info.cf_name
                  << " because of " << compactionReasonString(info.compaction_reason)
                  << ", status: " << info.status.ToString()
                  << ", compacted " << info.input_files.size()
                  << " files into " << info.output_files.size()
                  << ", base level is " << info.base_input_level
                  << ", output level is " << info.output_level;
    }

    // A callback function for RocksDB which will be called
    // whenever a registered RocksDB compacts a file.
    void OnCompactionCompleted(rocksdb::DB*, const rocksdb::CompactionJobInfo& info) override {
        LOG(INFO) << "Rocksdb compaction completed column family: " << info.cf_name
                  << " because of " << compactionReasonString(info.compaction_reason)
                  << ", status: " << info.status.ToString()
                  << ", compacted " << info.input_files.size()
                  << " files into " << info.output_files.size()
                  << ", base level is " << info.base_input_level
                  << ", output level is " << info.output_level;
    }

    // A callback function to RocksDB which will be called
    // before a RocksDB starts to flush memtables.
    void OnFlushBegin(rocksdb::DB*, const rocksdb::FlushJobInfo& info) override {
        VLOG(1) << "Rocksdb start flush column family: " << info.cf_name
                << " because of " << flushReasonString(info.flush_reason)
                << ", the newly created file: " << info.file_path
                << ", the smallest sequence number is " << info.smallest_seqno
                << ", the largest sequence number is " << info.largest_seqno
                << ", the properties of the table: " << info.table_properties.ToString();
    }

    // A callback function to RocksDB which will be called
    // whenever a registered RocksDB flushes a file.
    void OnFlushCompleted(rocksdb::DB*, const rocksdb::FlushJobInfo& info) override {
        VLOG(1) << "Rocksdb flush completed column family: " << info.cf_name
                << " because of " << flushReasonString(info.flush_reason)
                << " the newly created file: " << info.file_path
                << " the smallest sequence number is " << info.smallest_seqno
                << " the largest sequence number is " << info.largest_seqno
                << " the properties of the table: " << info.table_properties.ToString();
    }

    // A callback function for RocksDB which will be called whenever a SST file is created.
    void OnTableFileCreated(const rocksdb::TableFileCreationInfo& info) override {
        VLOG(3) << "Rocksdb SST file created: the path is " << info.file_path
                << " the file size is " << info.file_size;
    }

    // A callback function for RocksDB which will be called whenever a SST file is deleted.
    void OnTableFileDeleted(const rocksdb::TableFileDeletionInfo& info) override {
        VLOG(3) << "Rocksdb SST file deleted: the path is " << info.file_path;
    }

    // A callback function for RocksDB which will be called before a SST file is being created.
    void OnTableFileCreationStarted(const rocksdb::TableFileCreationBriefInfo& info) override {
        VLOG(3) << " database's name is " << info.db_name
                << ", column family's name is " << info.cf_name
                << ", the created file is " << info.file_path
                << ", because of " << tableFileCreationReasonString(info.reason);
    }

    // A callback function for RocksDB which will be called before
    // a memtable is made immutable.
    void OnMemTableSealed(const rocksdb::MemTableInfo& info) override {
        VLOG(3) << "MemTable Sealed column family: " << info.cf_name
                << ", the total number of entries: " << info.num_entries
                << ", the total number of deletes: " << info.num_deletes;
    }

    // A callback function for RocksDB which will be called before
    // a column family handle is deleted.
    void OnColumnFamilyHandleDeletionStarted(rocksdb::ColumnFamilyHandle* /*handle*/) override {}

    // A callback function for RocksDB which will be called after an external
    // file is ingested using IngestExternalFile.
    void OnExternalFileIngested(rocksdb::DB*,
                                const rocksdb::ExternalFileIngestionInfo& info) override {
        LOG(INFO) << "Ingest external SST file: column family " << info.cf_name
                  << ", the external file path " << info. external_file_path
                  << ", the internal file path " << info.internal_file_path
                  << ", the properties of the table: " << info.table_properties.ToString();
    }

    // A callback function for RocksDB which will be called before setting the
    // background error status to a non-OK value.
    void OnBackgroundError(rocksdb::BackgroundErrorReason reason,
                           rocksdb::Status*) override {
        LOG(INFO) << "BackgroundError: because of " << backgroundErrorReasonString(reason);
    }

    // A callback function for RocksDB which will be called whenever a change
    // of superversion triggers a change of the stall conditions.
    void OnStallConditionsChanged(const rocksdb::WriteStallInfo& info) override {
        LOG(INFO) << "Stall conditions changed column family: " << info.cf_name
                  << ", current condition: " << writeStallConditionString(info.condition.cur)
                  << ", previous condition: " << writeStallConditionString(info.condition.prev);
    }

    // A callback function for RocksDB which will be called whenever a file read
    // operation finishes.
    void OnFileReadFinish(const rocksdb::FileOperationInfo& info) override {
        VLOG(3) << "Reading file finished: file path is " << info.path
                << " offset: " << info.offset << " length: " << info.length;
    }

    // A callback function for RocksDB which will be called whenever a file write
    // operation finishes.
    void OnFileWriteFinish(const rocksdb::FileOperationInfo& info) override {
        VLOG(3) << "Writeing file finished: file path is " << info.path
                << " offset: " << info.offset << " length: " << info.length;
    }

    // A callback function for RocksDB which will be called whenever a file flush
    // operation finishes.
    void OnFileFlushFinish(const rocksdb::FileOperationInfo& info) override {
        VLOG(3) << "Flushing file finished: file path is " << info.path
                << " offset: " << info.offset << " length: " << info.length;
    }

    // A callback function for RocksDB which will be called whenever a file sync
    // operation finishes.
    void OnFileSyncFinish(const rocksdb::FileOperationInfo& info) override {
        VLOG(3) << "Syncing file finished: file path is " << info.path
                << " offset: " << info.offset << " length: " << info.length;
    }

    // A callback function for RocksDB which will be called whenever a file
    // rangeSync operation finishes.
    void OnFileRangeSyncFinish(const rocksdb::FileOperationInfo& info) override {
        VLOG(3) << "RangeSyncing file finished: file path is " << info.path
                << " offset: " << info.offset << " length: " << info.length;
    }

    // A callback function for RocksDB which will be called whenever a file
    // truncate operation finishes.
    void OnFileTruncateFinish(const rocksdb::FileOperationInfo& info) override {
        VLOG(3) << "Truncating file finished: file path is " << info.path
                << " offset: " << info.offset << " length: " << info.length;
    }

    // A callback function for RocksDB which will be called whenever a file close
    // operation finishes.
    void OnFileCloseFinish(const rocksdb::FileOperationInfo& info) override {
        VLOG(3) << "Closing file finished: file path is " << info.path;
    }

    // A callback function for RocksDB which will be called just before
    // starting the automatic recovery process for recoverable background errors。
    void OnErrorRecoveryBegin(rocksdb::BackgroundErrorReason reason,
                              rocksdb::Status /* bg_error */,
                              bool* /* auto_recovery */) override {
        LOG(INFO) << "Error recovery begin: because of " << backgroundErrorReasonString(reason);
    }

    // A callback function for RocksDB which will be called once the database
    // is recovered from read-only mode after an error.
    void OnErrorRecoveryCompleted(rocksdb::Status) override {
        LOG(INFO) << "Error Recovery Completed";
    }

private:
    std::string compactionReasonString(const rocksdb::CompactionReason& reason) {
        switch (reason) {
            case rocksdb::CompactionReason::kUnknown:
                return "Unknown";
            case rocksdb::CompactionReason::kLevelL0FilesNum:
                return "LevelL0FilesNum";
            case rocksdb::CompactionReason::kLevelMaxLevelSize:
                return "LevelMaxLevelSize";
            case rocksdb::CompactionReason::kUniversalSizeAmplification:
                return "UniversalSizeAmplification";
            case rocksdb::CompactionReason::kUniversalSizeRatio:
                return "UniversalSizeRatio";
            case rocksdb::CompactionReason::kUniversalSortedRunNum:
                return "UniversalSortedRunNum";
            case rocksdb::CompactionReason::kFIFOMaxSize:
                return "FIFOMaxSize";
            case rocksdb::CompactionReason::kFIFOReduceNumFiles:
                return "FIFOReduceNumFiles";
            case rocksdb::CompactionReason::kFIFOTtl:
                return "FIFOTtl";
            case rocksdb::CompactionReason::kManualCompaction:
                return "ManualCompaction";
            case rocksdb::CompactionReason::kFilesMarkedForCompaction:
                return "FilesMarkedForCompaction";
            case rocksdb::CompactionReason::kBottommostFiles:
                return "BottommostFiles";
            case rocksdb::CompactionReason::kTtl:
                return "Ttl";
            case rocksdb::CompactionReason::kFlush:
                return "Flush";
            case rocksdb::CompactionReason::kExternalSstIngestion:
                return "ExternalSstIngestion";
            case rocksdb::CompactionReason::kPeriodicCompaction:
                return "PeriodicCompaction";
            case rocksdb::CompactionReason::kNumOfReasons:
                return "NumOfReasons";
        }
        return "Unknown";
    }

    std::string flushReasonString(const rocksdb::FlushReason& reason) {
        switch (reason) {
            case rocksdb::FlushReason::kOthers:
                return "Others";
            case rocksdb::FlushReason::kGetLiveFiles:
                return "GetLiveFiles";
            case rocksdb::FlushReason::kShutDown:
                return "ShutDown";
            case rocksdb::FlushReason::kExternalFileIngestion:
                return "ExternalFileIngestion";
            case rocksdb::FlushReason::kManualCompaction:
                return "ManualCompaction";
            case rocksdb::FlushReason::kWriteBufferManager:
                return "WriteBufferManager";
            case rocksdb::FlushReason::kWriteBufferFull:
                return "WriteBufferFull";
            case rocksdb::FlushReason::kTest:
                return "Test";
            case rocksdb::FlushReason::kDeleteFiles:
                return "DeleteFiles";
            case rocksdb::FlushReason::kAutoCompaction:
                return "AutoCompaction";
            case rocksdb::FlushReason::kManualFlush:
                return "ManualFlush";
            case rocksdb::FlushReason::kErrorRecovery:
                return "ErrorRecovery";
            case rocksdb::FlushReason::kErrorRecoveryRetryFlush:
                return "ErrorRecoveryRetryFlush";
            default:
                return "Unknown";
        }
    }

    std::string backgroundErrorReasonString(const rocksdb::BackgroundErrorReason& reason) {
        switch (reason) {
            case rocksdb::BackgroundErrorReason::kFlush:
                return "Flush";
            case rocksdb::BackgroundErrorReason::kCompaction:
                return "Compaction";
            case rocksdb::BackgroundErrorReason::kWriteCallback:
                return "WriteCallback";
            case rocksdb::BackgroundErrorReason::kMemTable:
                return "MemTable";
            case rocksdb::BackgroundErrorReason::kManifestWrite:
                return "ManifestWrite";
            case rocksdb::BackgroundErrorReason::kFlushNoWAL:
                return "FlushNoWAL";
            default:
                return "Unknown";
        }
    }

    std::string writeStallConditionString(const rocksdb::WriteStallCondition& condition) {
        switch (condition) {
            case rocksdb::WriteStallCondition::kNormal:
                return "Normal";
            case rocksdb::WriteStallCondition::kDelayed:
                return "Delayed";
            case rocksdb::WriteStallCondition::kStopped:
                return "Stopped";
            default:
                return "Unknown";
        }
    }

    std::string tableFileCreationReasonString(const rocksdb::TableFileCreationReason& reason) {
        switch (reason) {
            case rocksdb::TableFileCreationReason::kFlush:
                return "Flush";
            case rocksdb::TableFileCreationReason::kCompaction:
                return "Compaction";
            case rocksdb::TableFileCreationReason::kRecovery:
                return "Recovery";
            case rocksdb::TableFileCreationReason::kMisc:
                return "Misc";
            default:
                return "Unknown";
        }
    }
};

}  // namespace kvstore
}  // namespace nebula
