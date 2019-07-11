/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef WAL_FILEBASEDWAL_H_
#define WAL_FILEBASEDWAL_H_

#include "base/Base.h"
#include <folly/Function.h>
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

    // The maximum size of each log message file (in MB). When the existing
    // log file reaches this size, a new file will be created
    size_t fileSize = 128;

    // Size of each buffer (in MB)
    size_t bufferSize = 8;

    // Number of buffers allowed. When the number of buffers reach this
    // number, appendLogs() will be blocked until some buffers are flushed
    size_t numBuffers = 4;
};


class BufferFlusher;

using PreProcessor = folly::Function<bool(LogID, TermID, ClusterID, const std::string& log)>;
class FileBasedWal final
        : public Wal
        , public std::enable_shared_from_this<FileBasedWal> {
public:
    // A factory method to create a new WAL
    static std::shared_ptr<FileBasedWal> getWal(
        const folly::StringPiece dir,
        FileBasedWalPolicy policy,
        BufferFlusher* flusher,
        PreProcessor preProcessor);

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

    // Scan [firstLogId, lastLogId]
    // This method IS thread-safe
    std::unique_ptr<LogIterator> iterator(LogID firstLogId,
                                          LogID lastLogId) override;

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
                 FileBasedWalPolicy policy,
                 BufferFlusher* flusher,
                 PreProcessor preProcessor);

    // Scan all WAL files
    void scanAllWalFiles();

    // Dump a Cord to the current file
    void dumpCord(Cord& cord,
                  LogID firstId,
                  LogID lastId,
                  TermID lastTerm);

    // Close down the current wal file
    void closeCurrFile();
    // Prepare a new wal file starting from the given log id
    void prepareNewFile(LogID startLogId);

    // Create a new buffer at the end of the buffer list
    BufferPtr createNewBuffer(LogID firstId,
                              std::unique_lock<std::mutex>& guard);

    // Implementation of appendLog()
    bool appendLogInternal(BufferPtr& buffer,
                           LogID id,
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
    BufferFlusher* flusher_;

    std::atomic<bool> stopped_{false};

    const std::string dir_;
    const FileBasedWalPolicy policy_;
    const size_t maxFileSize_;
    const size_t maxBufferSize_;
    const LogID firstLogId_{0};
    LogID lastLogId_{0};
    TermID lastLogTerm_{0};

    // firstLogId -> WalInfo
    // The last entry is the current opened WAL file
    WalFiles walFiles_;
    mutable std::mutex walFilesMutex_;

    // The current fd (which is the last file in walFiles_)
    // for appending new log messages
    // Accessing currFd_ **IS NOT** thread-safe, because only
    // the buffer-flush thread is expected to access it
    int32_t currFd_{-1};
    // The WalFileInfo corresponding to the currFd_
    WalFileInfoPtr currInfo_;

    // All buffers except the last one are ready to be persisted
    // The last entry is the one being appended
    BufferList buffers_;
    mutable std::mutex buffersMutex_;
    // There is a vacancy for a new buffer
    std::condition_variable slotReadyCV_;

    mutable std::mutex flushMutex_;
    PreProcessor preProcessor_;
};

}  // namespace wal
}  // namespace nebula
#endif  // WAL_FILEBASEDWAL_H_

