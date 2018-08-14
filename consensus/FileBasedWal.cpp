/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "consensus/FileBasedWal.h"
#include "fs/FileUtils.h"

namespace vesoft {
namespace vgraph {
namespace consensus {

using WalFileInfoPair = FileBasedWal::WalFiles::value_type;
using namespace vesoft::fs;


namespace internal {

// Compare two WalFileInfo
class WalFileLess final {
public:
    bool operator()(LogID logId, const WalFileInfoPair& right) {
        return logId < right.first;
    }
};


/**
 * Log message iterator
 *
 * The iterator tries to find the first log message in either wal files, or
 * from the in-memory buffers. If the given log id is out of range, an
 * invalid (valid() method will return false) iterator will be constructed
 */
class FileBasedWalIterator final : public LogIterator {
public:
    // The range is [startId, lastId]
    // if the lastId < 0, the wal_->lastId_ will be used
    FileBasedWalIterator(std::shared_ptr<FileBasedWal> wal,
                         LogID startId,
                         LogID lastId = -1)
            : wal_(wal)
            , currId_(startId) {

        if (lastId >= 0) {
            lastId_ = lastId;
        } else {
            lastId_ = wal_->lastLogId_;
        }
        CHECK_LE(currId_, lastId_);

        if (startId < wal_->firstLogId_) {
            LOG(ERROR) << "The given log id " << startId
                       << " is out of the range";
            currId_ = lastId_ + 1;
            return;
        } else {
            //  Pick any buffer if needed
            firstIdInBuffer_ = std::numeric_limits<LogID>::max();
            std::lock_guard<std::mutex> guard(wal_->bufferMutex_);
            for (auto it = wal_->buffers_.rbegin();
                 it != wal_->buffers_.rend();
                 ++it) {
                buffers_.push_front(*it);
                firstIdInBuffer_ = (*it)->firstLogId();
                if (firstIdInBuffer_ <= currId_) {
                    // Go no futher
                    currBuffer_ = buffers_.begin();
                    currIdx_ = currId_ - firstIdInBuffer_;
                    break;
                }
            }
        }

        if (firstIdInBuffer_ > currId_) {
            // We need to read from the WAL files
            std::lock_guard<std::mutex> guard(wal_->walFilesLock_);
            auto it = std::upper_bound(wal_->walFiles_.begin(),
                                       wal_->walFiles_.end(),
                                       currId_,
                                       WalFileLess());
            if (it == wal_->walFiles_.end()) {
                // Only need to read the last file
                auto& info = wal_->walFiles_.back();
                int32_t fd = open(info.second.fullname.c_str(), O_RDONLY);
                if (fd < 0) {
                    LOG(ERROR) << "Failed to open wal file \""
                               << info.second.fullname
                               << "\" (" << errno << "): " << strerror(errno);
                    currId_ = lastId_ + 1;
                    return;
                }
                fds_.push_back(fd);
                idRanges_.push_back(
                    std::make_pair(info.second.firstLogId,
                                   info.second.lastLogId));
            } else if (it != wal_->walFiles_.begin()) {
                // Need to start reading from the previous file
                --it;
                CHECK_LE(it->first, currId_);
                // Now let's open every files starting from **it** to
                // prevent being deleted
                while (it != wal_->walFiles_.end()) {
                    int32_t fd = open(it->second.fullname.c_str(), O_RDONLY);
                    if (fd < 0) {
                        LOG(ERROR) << "Failed to open wal file \""
                                   << it->second.fullname
                                   << "\" (" << errno << "): "
                                   << strerror(errno);
                        currId_ = lastId_ + 1;
                        return;
                    }
                    fds_.push_back(fd);
                    idRanges_.push_back(
                        std::make_pair(it->second.firstLogId,
                                       it->second.lastLogId));
                }
            } else {
                LOG(ERROR) << "LogID " << currId_
                           << " is out of the wal files range";
                currId_ = lastId_ + 1;
                return;
            }

            currFile_ = 0;
            CHECK_LE(idRanges_[currFile_].first, currId_);
            CHECK_LE(currId_, idRanges_[currFile_].second);

            // Find the correct position
            currPos_ = 0;
            while (true) {
                LogID logId;
                CHECK_EQ(pread(fds_[currFile_],
                               reinterpret_cast<char*>(&logId),
                               sizeof(LogID),
                               currPos_),
                         sizeof(LogID));
                CHECK_EQ(pread(fds_[currFile_],
                               reinterpret_cast<char*>(&currMsgLen_),
                               sizeof(int32_t),
                               currPos_ + sizeof(LogID)),
                         sizeof(int32_t));
                if (logId == currId_) {
                    break;
                }
                currPos_ += sizeof(LogID) + sizeof(int32_t) * 2 + currMsgLen_;
            }
        }
    }

    virtual ~FileBasedWalIterator() {
        for (auto& fd : fds_) {
            close(fd);
        }
    }

    LogIterator& operator++() override {
        ++currId_;
        if (currId_ < firstIdInBuffer_) {
            // Need to read file
            if (currId_ > idRanges_[currFile_].second) {
                VLOG(2) << "Current ID is " << currId_
                        << ", and the last ID in the current file is "
                        << idRanges_[currFile_].second
                        << ", so need to move to file #" << currFile_ + 1;
                // Need to roll over to next file
                ++currFile_;
                if (currFile_ >= fds_.size()) {
                    // Reached the end of wal files
                    CHECK_EQ(std::numeric_limits<LogID>::max(),
                             firstIdInBuffer_);
                    currId_ = lastId_ + 1;
                    return *this;
                }

                CHECK_EQ(currId_, idRanges_[currFile_].first);
                currPos_ = 0;
            } else {
                currPos_ += sizeof(LogID) + sizeof(int32_t) * 2 + currMsgLen_;
            }

            LogID logId;
            CHECK_EQ(pread(fds_[currFile_],
                           reinterpret_cast<char*>(&logId),
                           sizeof(LogID),
                           currPos_),
                     sizeof(LogID));
            CHECK_EQ(currId_, logId);
            CHECK_EQ(pread(fds_[currFile_],
                           reinterpret_cast<char*>(&currMsgLen_),
                           sizeof(int32_t),
                           currPos_ + sizeof(LogID)),
                     sizeof(int32_t));
        } else if (currId_ <= lastId_) {
            // Read from buffer
            if ((++currIdx_) >= (*currBuffer_)->logs_.size()) {
                // Roll over to next buffer
                ++currBuffer_;
                CHECK(currBuffer_ != buffers_.end());
                CHECK_EQ(currId_, (*currBuffer_)->firstLogId());
                currIdx_ = 0;
            } else {
                ++currIdx_;
            }
        }

        return *this;
    }

    bool valid() const override {
        return currId_ <= lastId_;
    }

    LogID logId() const override {
        return currId_;
    }

    std::string logMsg() const override {
        if (currId_ >= firstIdInBuffer_) {
            // Retrieve from the buffer
            DCHECK(currBuffer_ != buffers_.end());
            return (*currBuffer_)->logs_[currIdx_];
        } else {
            // Retrieve from the file
            DCHECK_GE(currFile_, 0);
            DCHECK_LT(currFile_, fds_.size());

            char buf[currMsgLen_];
            CHECK_EQ(pread(fds_[currFile_],
                           buf,
                           currMsgLen_,
                           currPos_ + sizeof(LogID) + sizeof(int32_t)),
                     currMsgLen_)
                << "Failed to read. Curr position is " << currPos_
                << ", expected read length is " << currMsgLen_
                << " (errno: " << errno << "): " << strerror(errno);

            return std::string(buf, currMsgLen_);
        }
    }

private:
    // Holds a Wal ponter, so that wal will not be destroyed before the iterator
    std::shared_ptr<FileBasedWal> wal_;

    LogID lastId_;
    LogID currId_;
    LogID firstIdInBuffer_;

    std::list<std::shared_ptr<FileBasedWal::Buffer>> buffers_;
    decltype(buffers_)::iterator currBuffer_;
    int32_t currIdx_;

    // [firstId, lastId]
    std::vector<std::pair<LogID, LogID>> idRanges_;
    std::vector<int32_t> fds_;
    int32_t currFile_;
    int64_t currPos_;
    int32_t currMsgLen_;
};

}  // namespace internal


/**********************************************
 *
 * Implementation of FileBasedWal
 *
 *********************************************/
// static
std::shared_ptr<FileBasedWal> FileBasedWal::getWal(
        const folly::StringPiece dir,
        FileBasedWalPolicy policy) {
    return std::shared_ptr<FileBasedWal>(
        new FileBasedWal(dir, std::move(policy)));
}


FileBasedWal::FileBasedWal(const folly::StringPiece dir,
                           FileBasedWalPolicy policy)
        : dir_(dir.toString())
        , policy_(std::move(policy))
        , maxFileSize_(policy_.fileSize * 1024L * 1024L)
        , maxBufferSize_(policy_.bufferSize * 1024L * 1024L) {
    scanAllWalFiles();
    if (!walFiles_.empty()) {
        const WalFileInfoPair& info = walFiles_.back();
        lastLogId_ = info.second.lastLogId;
        if (info.second.size < maxFileSize_ * 15 / 16) {
            // The last log file is small enough, so let's continue to use it
            currFileDesc_ = open(info.second.fullname.c_str(),
                                 O_WRONLY | O_APPEND);
            CHECK_GE(currFileDesc_, 0);
        }
    }

    if (currFileDesc_ < 0) {
        // prepareNewFile() expect the caller to hold the lock
        std::lock_guard<std::mutex> guard(walFilesLock_);
        prepareNewFile(lastLogId_ + 1);
    }

    bufferFlushThread_.reset(
        new std::thread(std::bind(&FileBasedWal::bufferFlushLoop, this)));
}


FileBasedWal::~FileBasedWal() {
    {
        std::unique_lock<std::mutex> guard(bufferMutex_);
        if (!buffers_.empty()) {
            buffers_.back()->freeze();
        }
    }
    bufferReadyCV_.notify_one();

    // Send the stop signal
    stopped_ = true;

    // Wait for threads to finish
    bufferFlushThread_->join();

    // Close the last file
    {
        std::unique_lock<std::mutex> guard(walFilesLock_);
        if (currFileDesc_ >= 0) {
            closeCurrFile();
        }
    }
}


void FileBasedWal::scanAllWalFiles() {
    std::vector<std::string> files = FileUtils::listAllFilesInDir(dir_.c_str(),
                                                                  false,
                                                                  "*.wal");
    for (auto& fn : files) {
        std::vector<std::string> parts;
        folly::split('.', fn, parts);
        if (parts.size() != 2) {
            LOG(ERROR) << "Ignore unknown file \"" << fn << "\"";
            continue;
        }
        int64_t startIdFromName;
        try {
            startIdFromName = folly::to<int64_t>(parts[0]);
        } catch (const std::exception& ex) {
            LOG(ERROR) << "Ignore bad file name \"" << fn << "\"";
            continue;
        }

        WalFileInfo info;
        info.fullname = FileUtils::joinPath(dir_, fn);

        // Get the size of the file and the mtime
        struct stat st;
        if (lstat(info.fullname.c_str(), &st)) {
            LOG(ERROR) << "Failed to get the size and mtime for the file \""
                       << fn << "\", ignore it";
            continue;
        }
        info.size = st.st_size;
        info.mtime = st.st_mtime;

        if (info.size == 0) {
            LOG(ERROR) << "Found empty wal file \"" << fn << "\", ignore it";
            continue;
        }

        // Open the file
        int32_t fd = open(info.fullname.c_str(), O_RDONLY);
        if (fd < 0) {
            LOG(ERROR) << "Failed to open the file \"" << fn << "\" ("
                       << errno << "): " << strerror(errno);
            continue;
        }

        // Read the first log id
        if (read(fd, &(info.firstLogId), sizeof(int64_t)) != sizeof(int64_t)) {
            LOG(ERROR) << "Failed to read the first log id from the file \""
                       << fn << "\" (" << errno << "): " << strerror(errno);
            close(fd);
            continue;
        }
        
        if (info.firstLogId != startIdFromName) {
            LOG(ERROR) << "The first log id " << info.firstLogId
                       << " does not match the file name \""
                       << fn << "\", ignore it!";
            close(fd);
            continue;
        }

        // Read the last log length
        if (lseek(fd, -sizeof(int32_t), SEEK_END) < 0) {
            LOG(ERROR) << "Failed to seek the last log length from the file \""
                       << fn << "\" (" << errno << "): " << strerror(errno);
            close(fd);
            continue;
        }
        int32_t msgLen;
        if (read(fd, &msgLen, sizeof(int32_t)) != sizeof(int32_t)) {
            LOG(ERROR) << "Failed to read the last log length from the file \""
                       << fn << "\" (" << errno << "): " << strerror(errno);
            close(fd);
            continue;
        }
        if (lseek(fd, -(sizeof(int32_t) * 2 + msgLen), SEEK_END) < 0) {
            LOG(ERROR) << "Failed to seek the last log length from the file \""
                       << fn << "\" (" << errno << "): " << strerror(errno);
            close(fd);
            continue;
        }
        int32_t firstMsgLen;
        if (read(fd, &firstMsgLen, sizeof(int32_t)) != sizeof(int32_t)) {
            LOG(ERROR) << "Failed to read the last log length from the file \""
                       << fn << "\" (" << errno << "): " << strerror(errno);
            close(fd);
            continue;
        }
        if (msgLen != firstMsgLen) {
            LOG(ERROR) << "It seems the wal file \"" << fn
                       << "\" is corrupted. Ignore it";
            // TODO We might want to fix it as much as possible
            continue;
        }

        // Read the last log id
        if (lseek(fd,
                  -(sizeof(int32_t) * 2 + msgLen + sizeof(LogID)),
                  SEEK_END) < 0) {
            LOG(ERROR) << "Failed to seek the last log id from the file \""
                       << fn << "\" (" << errno << "): " << strerror(errno);
            close(fd);
            continue;
        }
        if (read(fd, &(info.lastLogId), sizeof(LogID)) != sizeof(LogID)) {
            LOG(ERROR) << "Failed to read the last log id from the file \""
                       << fn << "\" (" << errno << "): " << strerror(errno);
            close(fd);
            continue;
        }

        // We now get all necessary info
        close(fd);
        walFiles_.emplace_back(std::make_pair(startIdFromName, std::move(info)));
    }

    // Make sure there is no gap in the logs
    if (!walFiles_.empty()) {
        LogID logIdAfterLastGap = -1;
        auto it = walFiles_.begin();
        LogID prevLastId = it->second.lastLogId;
        for (++it; it != walFiles_.end(); ++it) {
            if (it->second.firstLogId != prevLastId + 1) {
                // Found a gap
                LOG(ERROR) << "Found a log id gap before "
                           << it->second.firstLogId
                           << ", the previous log id is " << prevLastId;
                logIdAfterLastGap = it->second.firstLogId;
            }
            prevLastId = it->second.lastLogId;
        }
        if (logIdAfterLastGap > 0) {
            // Found gap, remove all logs before the last gap
            it = walFiles_.begin();
            while (it->second.firstLogId < logIdAfterLastGap) {
                LOG(INFO) << "Removing the wal file \""
                          << it->second.fullname << "\"";
                unlink(it->second.fullname.c_str());
                it = walFiles_.erase(it);
            }
        }
    }
}


void FileBasedWal::closeCurrFile() {
    // Expect to execute in protected context
    // TODO: This is not a *CORRECT* to check the ownership, need to fix
    CHECK(!walFilesLock_.try_lock());

    CHECK_GE(currFileDesc_, 0);

    auto& info = walFiles_.back();

    // Close the file
    CHECK_EQ(close(currFileDesc_), 0);
    info.second.mtime = time(nullptr);

    currFileDesc_ = -1;

    CHECK_EQ(info.second.size,
             FileUtils::fileSize(info.second.fullname.c_str()));
}


void FileBasedWal::prepareNewFile(LogID startLogId) {
    // Expect to execute in protected context
    // TODO: This is not a *CORRECT* to check the ownership, need to fix
    CHECK(!walFilesLock_.try_lock());

    CHECK_LT(currFileDesc_, 0);

    // Prepare the last entry in walFiles_
    walFiles_.emplace_back(std::make_pair(startLogId, WalFileInfo()));
    WalFileInfoPair& info = walFiles_.back();
    info.second.fullname = FileUtils::joinPath(
        dir_,
        folly::stringPrintf("%019ld.wal", startLogId));
    info.second.firstLogId = startLogId;
    info.second.lastLogId = -1;
    info.second.mtime = 0;
    info.second.size = 0;

    // Create the file for write
    currFileDesc_ = open(
        info.second.fullname.c_str(),
        O_CREAT | O_EXCL | O_WRONLY | O_APPEND | O_CLOEXEC | O_LARGEFILE,
        0644);
    if (currFileDesc_ < 0) {
        LOG(FATAL) << "Failed to open file \"" << info.second.fullname
                   << "\" (errno: " << errno << "): "
                   << strerror(errno);
    }
}


void FileBasedWal::bufferFlushLoop() {
    while (true) {
        std::shared_ptr<Buffer> buf;
        std::unique_lock<std::mutex> guard(bufferMutex_);
        if (buffers_.empty() && stopped_) {
            break;
        }

        if (buffers_.empty() || !(buffers_.front()->isFrozen())) {
            // Need to wait
            bufferReadyCV_.wait(guard, [this] {
                return !buffers_.empty() || stopped_;
            });
        } else {
            // Pick one buffer and flush it
            buf = buffers_.front();
            buffers_.pop_front();
        }
        guard.unlock();
        
        if (buf) {
            // Notify the appendLogs method, if it is waiting for vacanc
            slotReadyCV_.notify_one();

            flushBuffer(buf);
        }
    }
}


void FileBasedWal::dumpCord(Cord& cord, LogID lastId) {
    // Expect to execute in protected context
    // TODO: This is not a *CORRECT* to check the ownership, need to fix
    CHECK(!walFilesLock_.try_lock());

    if (cord.size() <= 0) {
        return;
    }

    auto cb = [this](const char* p, int32_t s) -> bool {
        const char* start = p;
        int32_t size = s;
        do {
            ssize_t res = write(currFileDesc_, start, size);
            if (res < 0) {
                LOG(ERROR) << "Failed to write wal file (" << errno
                           << "): " << strerror(errno);
                return false;
            }
            size -= res;
            start += res;
        } while (size > 0);
        return true;
    };
    if (!cord.applyTo(cb)) {
        LOG(FATAL) << "Failed to flush the wal file";
    }
    // Succeeded writing all buffered content, adjust the file size
    walFiles_.back().second.size += cord.size();
    walFiles_.back().second.lastLogId = lastId;
}


void FileBasedWal::flushBuffer(std::shared_ptr<Buffer> buffer) {
    if (!buffer || buffer->empty()) {
        return;
    }

    std::lock_guard<std::mutex> guard(walFilesLock_);

    auto& info = walFiles_.back();
    Cord cord;
    int64_t logId = buffer->firstLogId();
    auto it = buffer->logs().begin();
    while (it != buffer->logs().end()) {
        if (info.second.size + cord.size()
            + it->size()
            + sizeof(LogID)
            + sizeof(int32_t) * 2
                > maxFileSize_) {
            dumpCord(cord, logId - 1);
            // Reset the cord
            cord.clear();

            // Need to close the current file and create a new file
            closeCurrFile();
            prepareNewFile(logId);
            info = walFiles_.back();
        }

        cord << logId << int32_t(it->size());
        cord.write(it->data(), it->size());
        cord << int32_t(it->size());

        ++logId;
        ++it;
    }
    // Dump the rest
    dumpCord(cord, logId - 1);

    // Flush the wal file
    CHECK_EQ(fsync(currFileDesc_), 0);
}


bool FileBasedWal::appendLogInternal(std::shared_ptr<Buffer>& buffer,
                                     LogID id,
                                     std::string msg) {
    if (id != lastLogId_ + 1) {
        LOG(ERROR) << "There is a gap in the log id. The last log id is "
                   << lastLogId_ << ", and the id being appended is " << id;
        return false;
    }

    if (buffer &&
        (buffer->size() + msg.size() + sizeof(LogID) > maxBufferSize_)) {
        // Freeze the current buffer
        buffer->freeze();
        bufferReadyCV_.notify_one();
        buffer.reset();
    }

    // Create a new buffer if needed
    if (!buffer) {
        std::unique_lock<std::mutex> guard(bufferMutex_);
        if (buffers_.size() >= policy_.numBuffers) {
            // Log appending is way too fast
            LOG(WARNING) << "Write buffer is exausted,"
                            " need to wait for vacancy";
            // TODO: Output a counter here
            // Need to wait for a vacant slot
            slotReadyCV_.wait(guard, [this] {
                return buffers_.size() < policy_.numBuffers;
            });
        }

        // Create a new buffer to use
        buffers_.emplace_back(std::make_shared<Buffer>(id));
        buffer = buffers_.back();
    }

    DCHECK_EQ(id, buffer->firstLogId() + buffer->logs().size());
    buffer->push(std::move(msg));
    lastLogId_ = id;

    return true;
}


bool FileBasedWal::appendLog(LogID id, std::string msg) {
    std::shared_ptr<Buffer> buffer;
    {
        std::unique_lock<std::mutex> guard(bufferMutex_);
        if (!buffers_.empty()) {
            buffer = buffers_.back();
        }
    }

    if (!appendLogInternal(buffer, id, std::move(msg))) {
        LOG(ERROR) << "Failed to append log for logId " << id;
        return false;
    }

    return true;
}


bool FileBasedWal::appendLogs(LogIterator& iter) {
    std::shared_ptr<Buffer> buffer;
    {
        std::unique_lock<std::mutex> guard(bufferMutex_);
        if (!buffers_.empty()) {
            buffer = buffers_.back();
        }
    }

    for (; iter.valid(); ++iter) {
        if (!appendLogInternal(buffer,
                               iter.logId(),
                               std::move(iter.logMsg()))) {
            LOG(ERROR) << "Failed to append log for logId " << iter.logId();
            return false;
        }
    }

    return true;
}


std::unique_ptr<LogIterator> FileBasedWal::iterator(LogID firstLogId) {
    return std::unique_ptr<LogIterator>(
        new internal::FileBasedWalIterator(shared_from_this(), firstLogId));
}

}  // namespace consensus
}  // namespace vgraph
}  // namespace vesoft

