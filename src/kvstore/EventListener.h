/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/base/Base.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"

namespace nebula {
namespace kvstore {

class EventListener : public rocksdb::EventListener {
 public:
  /**
   * @brief A callback function to RocksDB which will be called before a RocksDB starts to compact.
   *
   * @param info Compaction job information passed by rocksdb
   */
  void OnCompactionBegin(rocksdb::DB*, const rocksdb::CompactionJobInfo& info) override {
    LOG(INFO) << "Rocksdb start compaction column family: " << info.cf_name << " because of "
              << compactionReasonString(info.compaction_reason)
              << ", status: " << info.status.ToString() << ", compacted " << info.input_files.size()
              << " files into " << info.output_files.size() << ", base level is "
              << info.base_input_level << ", output level is " << info.output_level;
  }

  /**
   * @brief A callback function for RocksDB which will be called whenever a registered RocksDB
   * compacts a file.
   *
   * @param info Compaction job information passed by rocksdb
   */
  void OnCompactionCompleted(rocksdb::DB*, const rocksdb::CompactionJobInfo& info) override {
    LOG(INFO) << "Rocksdb compaction completed column family: " << info.cf_name << " because of "
              << compactionReasonString(info.compaction_reason)
              << ", status: " << info.status.ToString() << ", compacted " << info.input_files.size()
              << " files into " << info.output_files.size() << ", base level is "
              << info.base_input_level << ", output level is " << info.output_level;
  }

  /**
   * @brief A callback function to RocksDB which will be called before a RocksDB starts to flush
   * memtables.
   *
   * @param info Flush job information passed by rocksdb
   */
  void OnFlushBegin(rocksdb::DB*, const rocksdb::FlushJobInfo& info) override {
    VLOG(1) << "Rocksdb start flush column family: " << info.cf_name << " because of "
            << flushReasonString(info.flush_reason)
            << ", the newly created file: " << info.file_path
            << ", the smallest sequence number is " << info.smallest_seqno
            << ", the largest sequence number is " << info.largest_seqno
            << ", the properties of the table: " << info.table_properties.ToString();
  }

  /**
   * @brief A callback function to RocksDB which will be called whenever a registered RocksDB
   * flushes a file.
   *
   * @param info Flush job information passed by rocksdb
   */
  void OnFlushCompleted(rocksdb::DB*, const rocksdb::FlushJobInfo& info) override {
    VLOG(1) << "Rocksdb flush completed column family: " << info.cf_name << " because of "
            << flushReasonString(info.flush_reason) << " the newly created file: " << info.file_path
            << " the smallest sequence number is " << info.smallest_seqno
            << " the largest sequence number is " << info.largest_seqno
            << " the properties of the table: " << info.table_properties.ToString();
  }

  /**
   * @brief A callback function for RocksDB which will be called whenever a SST file is created.
   *
   * @param info Table file creation information passed by rocksdb
   */
  void OnTableFileCreated(const rocksdb::TableFileCreationInfo& info) override {
    VLOG(3) << "Rocksdb SST file created: the path is " << info.file_path << " the file size is "
            << info.file_size;
  }

  /**
   * @brief A callback function for RocksDB which will be called whenever a SST file is deleted
   *
   * @param info Table file deletion information passed by rocksdb
   */
  void OnTableFileDeleted(const rocksdb::TableFileDeletionInfo& info) override {
    VLOG(3) << "Rocksdb SST file deleted: the path is " << info.file_path;
  }

  /**
   * @brief A callback function for RocksDB which will be called before a SST file is being created.
   *
   * @param info Table file creation information passed by rocksdb
   */
  void OnTableFileCreationStarted(const rocksdb::TableFileCreationBriefInfo& info) override {
    VLOG(3) << " database's name is " << info.db_name << ", column family's name is "
            << info.cf_name << ", the created file is " << info.file_path << ", because of "
            << tableFileCreationReasonString(info.reason);
  }

  /**
   * @brief A callback function for RocksDB which will be called before a memtable is made
   * immutable.
   *
   * @param info MemTable informations passed by rocksdb
   */
  void OnMemTableSealed(const rocksdb::MemTableInfo& info) override {
    VLOG(3) << "MemTable Sealed column family: " << info.cf_name
            << ", the total number of entries: " << info.num_entries
            << ", the total number of deletes: " << info.num_deletes;
  }

  /**
   * @brief A callback function for RocksDB which will be called before a column family handle is
   * deleted.
   *
   * @param handle Comlumn family handle passed by rocksdb
   */
  void OnColumnFamilyHandleDeletionStarted(rocksdb::ColumnFamilyHandle* handle) override {
    UNUSED(handle);
  }

  /**
   * @brief A callback function for RocksDB which will be called after an external file is ingested
   * using IngestExternalFile.
   *
   * @param db Rocksdb instance
   * @param info Ingest infomations passed by rocksdb
   */
  void OnExternalFileIngested(rocksdb::DB* db,
                              const rocksdb::ExternalFileIngestionInfo& info) override {
    UNUSED(db);
    VLOG(1) << "Ingest external SST file: column family " << info.cf_name
            << ", the external file path " << info.external_file_path << ", the internal file path "
            << info.internal_file_path
            << ", the properties of the table: " << info.table_properties.ToString();
  }

  /**
   * @brief A callback function for RocksDB which will be called before setting the background error
   * status to a non-OK value, e.g. disk is corrupted during comapction
   *
   * @param reason Reason to start a background job
   * @param status Detail status of the background job
   */
  void OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status* status) override {
    LOG(INFO) << "BackgroundError: because of " << backgroundErrorReasonString(reason) << " "
              << status->ToString();
  }

  /**
   * @brief A callback function for RocksDB which will be called whenever a change of superversion
   * triggers a change of the stall conditions.
   *
   * @param info Current and previous status of whether write is stalled
   */
  void OnStallConditionsChanged(const rocksdb::WriteStallInfo& info) override {
    LOG(INFO) << "Stall conditions changed column family: " << info.cf_name
              << ", current condition: " << writeStallConditionString(info.condition.cur)
              << ", previous condition: " << writeStallConditionString(info.condition.prev);
  }

  /**
   * @brief A callback function for RocksDB which will be called whenever a file read operation
   * finishes.
   *
   * @param info Information when read a rocksdb file
   */
  void OnFileReadFinish(const rocksdb::FileOperationInfo& info) override {
    VLOG(4) << "Reading file finished: file path is " << info.path << " offset: " << info.offset
            << " length: " << info.length;
  }

  /**
   * @brief A callback function for RocksDB which will be called whenever a file write operation
   * finishes.
   *
   * @param info Information when write a rocksdb file
   */
  void OnFileWriteFinish(const rocksdb::FileOperationInfo& info) override {
    VLOG(4) << "Writeing file finished: file path is " << info.path << " offset: " << info.offset
            << " length: " << info.length;
  }

  /**
   * @brief A callback function for RocksDB which will be called whenever a file flush operation
   * finishes.
   *
   * @param info Information when flush a rocksdb file
   */
  void OnFileFlushFinish(const rocksdb::FileOperationInfo& info) override {
    VLOG(4) << "Flushing file finished: file path is " << info.path << " offset: " << info.offset
            << " length: " << info.length;
  }

  /**
   * @brief A callback function for RocksDB which will be called whenever a file sync operation
   * finishes.
   *
   * @param info Information when sync a rocksdb file
   */
  void OnFileSyncFinish(const rocksdb::FileOperationInfo& info) override {
    VLOG(4) << "Syncing file finished: file path is " << info.path << " offset: " << info.offset
            << " length: " << info.length;
  }

  /**
   * @brief A callback function for RocksDB which will be called whenever a file rangeSync operation
   * finishes.
   *
   * @param info Information when range sync a rocksdb file
   */
  void OnFileRangeSyncFinish(const rocksdb::FileOperationInfo& info) override {
    VLOG(4) << "RangeSyncing file finished: file path is " << info.path
            << " offset: " << info.offset << " length: " << info.length;
  }

  /**
   * @brief A callback function for RocksDB which will be called whenever a file truncate operation
   * finishes.
   *
   * @param info Information when truncate a rocksdb file
   */
  void OnFileTruncateFinish(const rocksdb::FileOperationInfo& info) override {
    VLOG(4) << "Truncating file finished: file path is " << info.path << " offset: " << info.offset
            << " length: " << info.length;
  }

  /**
   * @brief A callback function for RocksDB which will be called whenever a file close operation
   * finishes.
   *
   * @param info Information when close a rocksdb file
   */
  void OnFileCloseFinish(const rocksdb::FileOperationInfo& info) override {
    VLOG(4) << "Closing file finished: file path is " << info.path;
  }

  /**
   * @brief A callback function for RocksDB which will be called just before starting the automatic
   * recovery process for recoverable background errors。
   *
   * @param reason Reason to start a background job
   * @param status Background job status
   * @param autoRecovery Whether is recovered automatically
   */
  void OnErrorRecoveryBegin(rocksdb::BackgroundErrorReason reason,
                            rocksdb::Status status,
                            bool* autoRecovery) override {
    UNUSED(autoRecovery);
    LOG(INFO) << "Error recovery begin: because of " << backgroundErrorReasonString(reason)
              << ", previously failed because of " << status.ToString();
  }

  /**
   * @brief A callback function for RocksDB which will be called once the database is recovered from
   * read-only mode after an error.
   *
   * @param oldBgStatus Previous background job status
   */
  void OnErrorRecoveryCompleted(rocksdb::Status oldBgStatus) override {
    LOG(INFO) << "Error Recovery Completed, previously failed because of "
              << oldBgStatus.ToString();
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
      default:
        return "Unknown";
    }
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
