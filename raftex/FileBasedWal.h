/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef RAFTEX_WAL_FILEBASEDWAL_H_
#define RAFTEX_WAL_FILEBASEDWAL_H_

#include "base/Base.h"
#include "base/Cord.h"
#include "raftex/Wal.h"

namespace vesoft {
namespace vgraph {
namespace raftex {

struct FileBasedWalPolicy {
    // The life span of the log messages (number of seconds)
    // This is only a hint, the FileBasedWal will try to keep all messages
    // newer than ttl, but not guarantee to remove old messages right away
    int32_t ttl = 86400;

    // The maximum size of each log message file (in MB). When the existing
    // log file reaches this size, a new file will be created
    int32_t fileSize = 128;

    // Size of each buffer (in MB)
    int32_t bufferSize = 8;

    // Number of buffers allowed. When the number of buffers reach this
    // number, appendLogs() will be blocked until some buffers are flushed
    int32_t numBuffers = 4;
};


namespace internal {
    class WalFileLess;
    class FileBasedWalIterator;
}  // namespace internal


class FileBasedWal final : public Wal,
                           public std::enable_shared_from_this<FileBasedWal> {
    friend class internal::FileBasedWalIterator;

public:
    struct WalFileInfo {
        std::string fullname;
        LogID firstLogId;
        LogID lastLogId;
        time_t mtime;
        size_t size;
    };
    using WalFiles = std::vector<std::pair<LogID, WalFileInfo>>;

    static std::shared_ptr<FileBasedWal> getWal(const folly::StringPiece dir,
                                                FileBasedWalPolicy policy);
    virtual ~FileBasedWal();

    // Return the ID of the last log message in the WAL
    LogID lastLogId() const override {
        return lastLogId_;
    }

    // Append one log messages to the WAL
    // This method **IS NOT** thread-safe
    bool appendLog(LogID id, std::string msg) override;

    // Append a list of log messages to the WAL
    // This method **IS NOT** thread-safe
    bool appendLogs(LogIterator& iter) override;

    // Scan [firstLogId, lastLogId]
    // This method IS thread-safe
    std::unique_ptr<LogIterator> iterator(LogID firstLogId) override;

private:  // Members
    class Buffer final {
        friend class internal::FileBasedWalIterator;
    public:
        explicit Buffer(LogID firstLogId) : firstLogId_(firstLogId) {}

        void push(std::string&& msg) {
            totalLen_ += msg.size() + sizeof(LogID);
            logs_.emplace_back(std::move(msg));
        }

        size_t size() const {
            return totalLen_;
        }

        bool empty() const {
            return logs_.empty();
        }

        LogID firstLogId() const {
            return firstLogId_;
        }

        void freeze() {
            frozen_ = true;
        }

        bool isFrozen() const {
            return frozen_;
        }

        const std::vector<std::string>& logs() const {
            return logs_;
        }

    private:
        std::vector<std::string> logs_;
        LogID firstLogId_{-1};
        size_t totalLen_{0};

        // When a buffer is frozen, no futher write will be allowed.
        // It's ready to be flushed out
        std::atomic<bool> frozen_{false};
    };


    /***************************************
     *
     * FileBasedWal Member Fields
     *
     **************************************/
    std::atomic<bool> stopped_{false};

    const std::string dir_;
    const FileBasedWalPolicy policy_;
    const int64_t maxFileSize_;
    const int64_t maxBufferSize_;
    LogID firstLogId_{-1};
    LogID lastLogId_{-1};

    // firstLogId -> WalInfo
    // The last entry is the current opened WAL file
    WalFiles walFiles_;
    std::mutex walFilesLock_;

    // The current fd for appending new log messages
    int32_t currFileDesc_{-1};

    // All buffers except the last one are ready to be persisted
    // The last entry is teh one being appended
    std::list<std::shared_ptr<Buffer>> buffers_;
    std::mutex bufferMutex_;
    std::condition_variable bufferReadyCV_;
    std::condition_variable slotReadyCV_;

    std::unique_ptr<thread::NamedThread> bufferFlushThread_;

private:  // Methods
    // Private constructor to prevent instantiating the class directly
    // Callers should use static method getWal() instead
    FileBasedWal(const folly::StringPiece dir,
                 FileBasedWalPolicy policy);

    // The functor for the bufferFlushThread
    void bufferFlushLoop();
    // Write buffer into wal files
    void flushBuffer(std::shared_ptr<Buffer> buffer);
    // Dump a Cord to the current file
    void dumpCord(Cord& cord, LogID lastId);

    // Scan all WAL files
    void scanAllWalFiles();

    // Close down the current wal file
    void closeCurrFile();
    // Prepare a new wal file starting from the given log id
    void prepareNewFile(LogID startLogId);

    // Implementation of appendLog()
    bool appendLogInternal(std::shared_ptr<Buffer>& buffer,
                           LogID id,
                           std::string msg);
};

}  // namespace raftex
}  // namespace vgraph
}  // namespace vesoft
#endif  // RAFTEX_WAL_FILEBASEDWAL_H_

