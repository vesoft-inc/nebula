/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef WAL_FILEBASEDWAL_H_
#define WAL_FILEBASEDWAL_H_

#include <folly/Function.h>
#include <gtest/gtest_prod.h>

#include "common/base/Base.h"
#include "common/base/Cord.h"
#include "kvstore/DiskManager.h"
#include "kvstore/wal/AtomicLogBuffer.h"
#include "kvstore/wal/Wal.h"
#include "kvstore/wal/WalFileInfo.h"

namespace nebula {
namespace wal {

struct FileBasedWalPolicy {
  // The maximum size of each log message file (in byte). When the existing
  // log file reaches this size, a new file will be created
  size_t fileSize = 16 * 1024L * 1024L;

  // Size of each buffer (in byte)
  size_t bufferSize = 8 * 1024L * 1024L;

  // Whether fsync needs to be called every write
  bool sync = false;
};

struct FileBasedWalInfo {
  std::string idStr_;
  GraphSpaceID spaceId_;
  PartitionID partId_;
};

using PreProcessor = folly::Function<bool(LogID, TermID, ClusterID, folly::StringPiece)>;

class FileBasedWal final : public Wal, public std::enable_shared_from_this<FileBasedWal> {
  FRIEND_TEST(FileBasedWal, TTLTest);
  FRIEND_TEST(FileBasedWal, CheckLastWalTest);
  FRIEND_TEST(FileBasedWal, LinkTest);
  FRIEND_TEST(FileBasedWal, CleanWalBeforeIdTest);
  FRIEND_TEST(WalFileIter, MultiFilesReadTest);
  friend class FileBasedWalIterator;
  friend class WalFileIterator;

 public:
  /**
   * @brief Invalid term when wal not found, used in getLogTerm
   */
  static constexpr TermID INVALID_TERM{-1};

  // A factory method to create a new WAL
  /**
   * @brief Build the file based wal
   *
   * @param dir Directory to save wal
   * @param info Wal info
   * @param policy Wal config
   * @param preProcessor The pre-process function
   * @param diskMan Disk manager to monitor remaining spaces
   * @return std::shared_ptr<FileBasedWal>
   */
  static std::shared_ptr<FileBasedWal> getWal(
      const folly::StringPiece dir,
      FileBasedWalInfo info,
      FileBasedWalPolicy policy,
      PreProcessor preProcessor,
      std::shared_ptr<kvstore::DiskManager> diskMan = nullptr);

  /**
   * @brief Destroy the file based wal
   */
  virtual ~FileBasedWal();

  /**
   * @brief Return the ID of the first log message in the WAL
   */
  LogID firstLogId() const override {
    return firstLogId_;
  }

  /**
   * @brief Return the ID of the last log message in the WAL
   */
  LogID lastLogId() const override {
    return lastLogId_;
  }

  /**
   * @brief Return the term of the last log message in the WAL
   */
  TermID lastLogTerm() const override {
    return lastLogTerm_;
  }

  /**
   * @brief Return the term of specified logId, if not exist，return -1
   */
  TermID getLogTerm(LogID id) override;

  /**
   * @brief Append one log messages to the WAL. This method **IS NOT** thread-safe. We **DO NOT**
   * expect multiple threads will append logs simultaneously
   *
   * @param id Log id to append
   * @param term Log term to append
   * @param cluster Cluster id in log to append
   * @param msg Log messgage to append
   * @return Whether append succeed
   */
  bool appendLog(LogID id, TermID term, ClusterID cluster, std::string msg) override;

  /**
   * @brief Append a list of log messages to the WAL. This method **IS NOT** thread-safe. We **DO
   * NOT** expect multiple threads will append logs simultaneously
   *
   * @param iter Log iterator to append
   * @return Whether append succeed
   */
  bool appendLogs(LogIterator& iter) override;

  /**
   * @brief Rollback to the given ID, all logs after the ID will be discarded. This method **IS
   * NOT** thread-safe. We **EXPECT** the thread rolling back logs is the same one appending logs
   *
   * @param id The log id to rollback
   * @return Whether rollback succeed
   */
  bool rollbackToLog(LogID id) override;

  /**
   * @brief Reset the WAL. This method is *NOT* thread safe
   *
   * @return Whether reset succeed
   */
  bool reset() override;

  /**
   * @brief Clean time expired wal by ttl
   */
  void cleanWAL() override;

  /**
   * @brief Clean wal by given log id
   */
  void cleanWAL(LogID id) override;

  /**
   * @brief Scan the wal in range [firstLogId, lastLogId]. This method is thread-safe
   *
   * @param firstLogId Start log id, inclusive
   * @param lastLogId End log id, inclusive
   * @return std::unique_ptr<LogIterator>
   */
  std::unique_ptr<LogIterator> iterator(LogID firstLogId, LogID lastLogId) override;

  /**
   * @brief Hard link the wal files to a new path
   *
   * @param newPath New wal path
   * @return Whether link succeed
   */
  bool linkCurrentWAL(const char* newPath) override;

  /**
   * @brief Iterates through all wal file info in reversed order (from the latest to the earliest).
   * The iteration finishes when the functor returns false or reaches the end.
   *
   * @param fn The function to process wal info
   * @return size_t The num of wal file info being accessed
   */
  size_t accessAllWalInfo(std::function<bool(WalFileInfoPtr info)> fn) const;

  /**
   * @brief Return the log buffer in memory
   */
  std::shared_ptr<AtomicLogBuffer> buffer() {
    return logBuffer_;
  }

 private:
  /***************************************
   *
   * Private methods
   *
   **************************************/
  /**
   * @brief Construct a new file based wal. Callers **SHOULD NEVER** use this constructor directly.
   * Callers should use static method getWal() instead
   *
   * @param dir Directory to save wal
   * @param info Wal info
   * @param policy Wal config
   * @param preProcessor The pre-process function
   * @param diskMan Disk manager to monitor remaining spaces
   */
  FileBasedWal(const folly::StringPiece dir,
               FileBasedWalInfo info,
               FileBasedWalPolicy policy,
               PreProcessor preProcessor,
               std::shared_ptr<kvstore::DiskManager> diskMan);

  /**
   * @brief Scan all WAL files
   */
  void scanAllWalFiles();

  /**
   * @brief Scan the last wal file by each wal log
   *
   * @param info Wal file info
   * @param firstId The first log id in the last wal file
   */
  void scanLastWal(WalFileInfoPtr info, LogID firstId);

  /**
   * @brief Close down the current wal file
   */
  void closeCurrFile();

  /**
   * @brief Prepare a new wal file starting from the given log id
   *
   * @param startLogId The first log id of new wal file
   */
  void prepareNewFile(LogID startLogId);

  /**
   * @brief Rollback to logId in given file
   *
   * @param info The wal file to rollback
   * @param logId The wal log id, it should be the last log id in file after rollback
   */
  void rollbackInFile(WalFileInfoPtr info, LogID logId);

  /**
   * @brief The actual implementation of appendLog()
   *
   * @param id Log id to append
   * @param term Log term to append
   * @param cluster Cluster id in log to append
   * @param msg Log messgage to append
   * @return Whether append succeed
   */
  bool appendLogInternal(LogID id, TermID term, ClusterID cluster, folly::StringPiece msg);

 private:
  using WalFiles = std::map<LogID, WalFileInfoPtr>;

  /***************************************
   *
   * FileBasedWal Member Fields
   *
   **************************************/
  const std::string dir_;
  std::string idStr_;
  GraphSpaceID spaceId_;
  PartitionID partId_;

  const FileBasedWalPolicy policy_;
  LogID firstLogId_{0};
  LogID lastLogId_{0};
  TermID lastLogTerm_{0};

  // firstLogId -> WalInfo
  // The last entry is the current opened WAL file
  WalFiles walFiles_;
  mutable std::mutex walFilesMutex_;

  // The current fd (which is the last file in walFiles_)
  // for appending new log messages.
  // Please be aware: accessing currFd_ is not thread-safe
  int32_t currFd_{-1};
  // The WalFileInfo corresponding to the currFd_
  WalFileInfoPtr currInfo_;

  std::shared_ptr<AtomicLogBuffer> logBuffer_;

  PreProcessor preProcessor_;

  std::shared_ptr<kvstore::DiskManager> diskMan_;

  folly::RWSpinLock rollbackLock_;
};

}  // namespace wal
}  // namespace nebula
#endif  // WAL_FILEBASEDWAL_H_
