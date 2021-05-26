/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef WAL_FILEBASEDWAL_H_
#define WAL_FILEBASEDWAL_H_

#include "base/Base.h"
#include <folly/Function.h>
#include <gtest/gtest_prod.h>
#include "base/Cord.h"
#include "kvstore/wal/Wal.h"
#include "kvstore/wal/InMemoryLogBuffer.h"
#include "kvstore/wal/WalFileInfo.h"

namespace nebula {
namespace wal {

struct FileBasedWalPolicy {
    // The life span of the log messages (number of seconds)
    // This is only a hint, the FileBasedWal will try to keep all messages
    // newer than ttl, but not guarantee to remove old messages right away
    int32_t ttl = 86400;
    // The maximum size of each log message file (in byte). When the existing
    // log file reaches this size, a new file will be created
    size_t fileSize = 16 * 1024L * 1024L;

    // Size of each buffer (in byte)
    size_t bufferSize = 8 * 1024L * 1024L;

    // Number of buffers allowed. When the number of buffers reach this
    // number, appendLogs() will be blocked until some buffers are flushed
    size_t numBuffers = 2;
    // Whether fsync needs to be called every write
    bool sync = false;
};


using PreProcessor = folly::Function<bool(LogID, TermID, ClusterID, const std::string& log)>;


class FileBasedWal final : public Wal
                         , public std::enable_shared_from_this<FileBasedWal> {
    FRIEND_TEST(FileBasedWal, TTLTest);
    FRIEND_TEST(FileBasedWal, CheckLastWalTest);
    FRIEND_TEST(FileBasedWal, LinkTest);
    friend class FileBasedWalIterator;
public:
    // A factory method to create a new WAL
    static std::shared_ptr<FileBasedWal> getWal(
        const folly::StringPiece dir,
        const std::string& idStr,
        FileBasedWalPolicy policy,
        PreProcessor preProcessor);

    ~FileBasedWal() override;

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

    void cleanWAL(int32_t ttl = 0) override;

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

    // Iterates through all log buffers in reversed order
    // (from the latest to the earliest)
    // The iteration finishes when the functor returns false or reaches
    // the end
    // The method returns the number of buffers being accessed
    size_t accessAllBuffers(std::function<bool(BufferPtr buffer)> fn) const;

    // Dump a buffer into a WAL file
    void flushBuffer(BufferPtr buffer);


private:
    /***************************************
     *
     * Private methods
     *
     **************************************/
    // Callers **SHOULD NEVER** use this constructor directly
    // Callers should use static method getWal() instead
    FileBasedWal(const folly::StringPiece dir,
                 const std::string& idStr,
                 FileBasedWalPolicy policy,
                 PreProcessor preProcessor);

    // Scan all WAL files
    void scanAllWalFiles();

    void scanLastWal(WalFileInfoPtr info, LogID firstId);

    // Close down the current wal file
    void closeCurrFile();
    // Prepare a new wal file starting from the given log id
    void prepareNewFile(LogID startLogId);
    // Rollback to logId in given file
    void rollbackInFile(WalFileInfoPtr info, LogID logId);

    // Return the last buffer.
    // If the last buffer is big enough, create a new one
    BufferPtr getLastBuffer(LogID id, size_t expectedToWrite);

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

    std::atomic<bool> stopped_{false};

    const FileBasedWalPolicy policy_;
    const size_t maxFileSize_;
    const size_t maxBufferSize_;
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

    // The purpose of the memory buffer is to provide a read cache
    BufferList buffers_;
    mutable std::mutex buffersMutex_;

    PreProcessor preProcessor_;

    folly::RWSpinLock rollbackLock_;
};

}  // namespace wal
}  // namespace nebula
#endif  // WAL_FILEBASEDWAL_H_

