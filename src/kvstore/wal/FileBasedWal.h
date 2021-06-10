/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef WAL_FILEBASEDWAL_H_
#define WAL_FILEBASEDWAL_H_

#include "common/base/Base.h"
#include "common/base/Cord.h"
#include <folly/Function.h>
#include <gtest/gtest_prod.h>
#include "kvstore/wal/Wal.h"
#include "kvstore/wal/WalFileInfo.h"
#include "kvstore/wal/AtomicLogBuffer.h"
#include "kvstore/DiskManager.h"

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

using PreProcessor = folly::Function<bool(LogID, TermID, ClusterID, const std::string& log)>;


class FileBasedWal final : public Wal
                         , public std::enable_shared_from_this<FileBasedWal> {
    FRIEND_TEST(FileBasedWal, TTLTest);
    FRIEND_TEST(FileBasedWal, CheckLastWalTest);
    FRIEND_TEST(FileBasedWal, LinkTest);
    FRIEND_TEST(FileBasedWal, CleanWalBeforeIdTest);
    FRIEND_TEST(WalFileIter, MultiFilesReadTest);
    friend class FileBasedWalIterator;
    friend class WalFileIterator;
public:
    // A factory method to create a new WAL
    static std::shared_ptr<FileBasedWal> getWal(
        const folly::StringPiece dir,
        FileBasedWalInfo info,
        FileBasedWalPolicy policy,
        PreProcessor preProcessor,
        std::shared_ptr<kvstore::DiskManager> diskMan = nullptr);

    virtual ~FileBasedWal();

    // Signal all WAL holders to stop using this WAL
    void stop() {
        stopped_ = true;
    }
    bool isStopped() const {
        return stopped_.load();
    }

    // Return the ID of the first log message in the WAL
    LogID firstLogId() const override {
        return firstLogId_;
    }

    // Return the ID of the last log message in the WAL
    LogID lastLogId() const override {
        return lastLogId_;
    }

    // Return the term when the the last log is received
    TermID lastLogTerm() const override {
        return lastLogTerm_;
    }

    // Append one log messages to the WAL
    // This method **IS NOT** thread-safe
    // we **DO NOT** expect multiple threads will append logs simultaneously
    bool appendLog(LogID id,
                   TermID term,
                   ClusterID cluster,
                   std::string msg) override;

    // Append a list of log messages to the WAL
    // This method **IS NOT** thread-safe
    // we **DO NOT** expect multiple threads will append logs
    // simultaneously
    bool appendLogs(LogIterator& iter) override;

    // Rollback to the given ID, all logs after the ID will be discarded
    // This method **IS NOT** thread-safe
    // we **EXPECT** the thread rolling back logs is the same one
    // appending logs
    bool rollbackToLog(LogID id) override;

    // Reset the WAL
    // This method is *NOT* thread safe
    bool reset() override;

    void cleanWAL() override;

    void cleanWAL(LogID id) override;

    // Scan [firstLogId, lastLogId]
    // This method IS thread-safe
    std::unique_ptr<LogIterator> iterator(LogID firstLogId,
                                          LogID lastLogId) override;

    /** It is not thread-safe */
    bool linkCurrentWAL(const char* newPath) override;

    // Iterates through all wal file info in reversed order
    // (from the latest to the earliest)
    // The iteration finishes when the functor returns false or reaches
    // the end
    // The method returns the number of wal file info being accessed
    size_t accessAllWalInfo(std::function<bool(WalFileInfoPtr info)> fn) const;


private:
    /***************************************
     *
     * Private methods
     *
     **************************************/
    // Callers **SHOULD NEVER** use this constructor directly
    // Callers should use static method getWal() instead
    FileBasedWal(const folly::StringPiece dir,
                 FileBasedWalInfo info,
                 FileBasedWalPolicy policy,
                 PreProcessor preProcessor,
                 std::shared_ptr<kvstore::DiskManager> diskMan);

    // Scan all WAL files
    void scanAllWalFiles();

    void scanLastWal(WalFileInfoPtr info, LogID firstId);

    // Close down the current wal file
    void closeCurrFile();
    // Prepare a new wal file starting from the given log id
    void prepareNewFile(LogID startLogId);
    // Rollback to logId in given file
    void rollbackInFile(WalFileInfoPtr info, LogID logId);

    // Implementation of appendLog()
    bool appendLogInternal(LogID id,
                           TermID term,
                           ClusterID cluster,
                           std::string msg);


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

    std::atomic<bool> stopped_{false};

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

