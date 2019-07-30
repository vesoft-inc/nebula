/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/wal/FileBasedWal.h"
#include "kvstore/wal/FileBasedWalIterator.h"
#include "kvstore/wal/BufferFlusher.h"
#include "fs/FileUtils.h"

namespace nebula {
namespace wal {

using nebula::fs::FileUtils;

/**********************************************
 *
 * Implementation of FileBasedWal
 *
 *********************************************/
// static
std::shared_ptr<FileBasedWal> FileBasedWal::getWal(
        const folly::StringPiece dir,
        FileBasedWalPolicy policy,
        BufferFlusher* flusher,
        PreProcessor preProcessor) {
    return std::shared_ptr<FileBasedWal>(
        new FileBasedWal(dir, std::move(policy), flusher, std::move(preProcessor)));
}


FileBasedWal::FileBasedWal(const folly::StringPiece dir,
                           FileBasedWalPolicy policy,
                           BufferFlusher* flusher,
                           PreProcessor preProcessor)
        : flusher_(flusher)
        , dir_(dir.toString())
        , policy_(std::move(policy))
        , maxFileSize_(policy_.fileSize * 1024L * 1024L)
        , maxBufferSize_(policy_.bufferSize * 1024L * 1024L)
        , preProcessor_(std::move(preProcessor)) {
    // Make sure WAL directory exist
    if (FileUtils::fileType(dir_.c_str()) == fs::FileType::NOTEXIST) {
        FileUtils::makeDir(dir_);
    }

    scanAllWalFiles();
    if (!walFiles_.empty()) {
        auto& info = walFiles_.rbegin()->second;
        firstLogId_ = walFiles_.begin()->second->firstId();
        lastLogId_ = info->lastId();
        lastLogTerm_ = info->lastTerm();
        currFd_ = open(info->path(), O_WRONLY | O_APPEND);
        currInfo_ = info;
        CHECK_GE(currFd_, 0);
    }
}


FileBasedWal::~FileBasedWal() {
    // FileBasedWal inherits from std::enable_shared_from_this, so at this
    // moment, there should have no other thread holding this WAL object
    if (!buffers_.empty()) {
        // There should be only one left
        CHECK_EQ(buffers_.size(), 1UL);
        if (buffers_.back()->freeze()) {
            flushBuffer(buffers_.back());
        }
    }

    // Close the last file
    closeCurrFile();
    LOG(INFO) << "~FileBasedWal, dir = " << dir_;
}


void FileBasedWal::scanAllWalFiles() {
    std::vector<std::string> files =
        FileUtils::listAllFilesInDir(dir_.c_str(), false, "*.wal");
    for (auto& fn : files) {
        // Split the file name
        // The file name convention is "<first id in the file>.wal"
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

        WalFileInfoPtr info = std::make_shared<WalFileInfo>(
            FileUtils::joinPath(dir_, fn),
            startIdFromName);

        // Get the size of the file and the mtime
        struct stat st;
        if (lstat(info->path(), &st) < 0) {
            LOG(ERROR) << "Failed to get the size and mtime for \""
                       << fn << "\", ignore it";
            continue;
        }
        info->setSize(st.st_size);
        info->setMTime(st.st_mtime);

        if (info->size() == 0) {
            LOG(ERROR) << "Found empty wal file \"" << fn
                       << "\", ignore it";
            continue;
        }

        // Open the file
        int32_t fd = open(info->path(), O_RDONLY);
        if (fd < 0) {
            LOG(ERROR) << "Failed to open the file \"" << fn << "\" ("
                       << errno << "): " << strerror(errno);
            continue;
        }

        // Read the first log id
        LogID firstLogId = -1;
        if (read(fd, &firstLogId, sizeof(LogID)) != sizeof(LogID)) {
            LOG(ERROR) << "Failed to read the first log id from \""
                       << fn << "\" (" << errno << "): "
                       << strerror(errno);
            close(fd);
            continue;
        }

        if (firstLogId != startIdFromName) {
            LOG(ERROR) << "The first log id " << firstLogId
                       << " does not match the file name \""
                       << fn << "\", ignore it!";
            close(fd);
            continue;
        }

        // Read the last log length
        if (lseek(fd, -sizeof(int32_t), SEEK_END) < 0) {
            LOG(ERROR) << "Failed to seek the last log length from \""
                       << fn << "\" (" << errno << "): "
                       << strerror(errno);
            close(fd);
            continue;
        }
        int32_t succMsgLen;
        if (read(fd, &succMsgLen, sizeof(int32_t)) != sizeof(int32_t)) {
            LOG(ERROR) << "Failed to read the last log length from \""
                       << fn << "\" (" << errno << "): "
                       << strerror(errno);
            close(fd);
            continue;
        }

        // Verify the last log length
        if (lseek(fd,
                  -(sizeof(int32_t) * 2 + succMsgLen + sizeof(ClusterID)),
                  SEEK_END) < 0) {
            LOG(ERROR) << "Failed to seek the last log length from \""
                       << fn << "\" (" << errno << "): "
                       << strerror(errno);
            close(fd);
            continue;
        }
        int32_t precMsgLen;
        if (read(fd, &precMsgLen, sizeof(int32_t)) != sizeof(int32_t)) {
            LOG(ERROR) << "Failed to read the last log length from \""
                       << fn << "\" (" << errno << "): "
                       << strerror(errno);
            close(fd);
            continue;
        }
        if (precMsgLen != succMsgLen) {
            LOG(ERROR) << "It seems the wal file \"" << fn
                       << "\" is corrupted. Ignore it";
            // TODO We might want to fix it as much as possible
            close(fd);
            continue;
        }

        // Read the last log term
        if (lseek(fd,
                  -(sizeof(int32_t) * 2
                    + succMsgLen
                    + sizeof(ClusterID)
                    + sizeof(TermID)),
                  SEEK_END) < 0) {
            LOG(ERROR) << "Failed to seek the last log term from \""
                       << fn << "\" (" << errno << "): "
                       << strerror(errno);
            close(fd);
            continue;
        }
        TermID term = -1;
        if (read(fd, &term, sizeof(TermID)) != sizeof(TermID)) {
            LOG(ERROR) << "Failed to read the last log term from \""
                       << fn << "\" (" << errno << "): "
                       << strerror(errno);
            close(fd);
            continue;
        }
        info->setLastTerm(term);

        // Read the last log id
        if (lseek(fd,
                  -(sizeof(int32_t) * 2
                    + succMsgLen
                    + sizeof(ClusterID)
                    + sizeof(TermID)
                    + sizeof(LogID)),
                  SEEK_END) < 0) {
            LOG(ERROR) << "Failed to seek the last log id from \""
                       << fn << "\" (" << errno << "): "
                       << strerror(errno);
            close(fd);
            continue;
        }
        LogID lastLogId = -1;
        if (read(fd, &lastLogId, sizeof(LogID)) != sizeof(LogID)) {
            LOG(ERROR) << "Failed to read the last log id from \""
                       << fn << "\" (" << errno << "): "
                       << strerror(errno);
            close(fd);
            continue;
        }
        info->setLastId(lastLogId);

        // We now get all necessary info
        close(fd);
        walFiles_.insert(std::make_pair(startIdFromName, info));
    }

    // Make sure there is no gap in the logs
    if (!walFiles_.empty()) {
        LogID logIdAfterLastGap = -1;
        auto it = walFiles_.begin();
        LogID prevLastId = it->second->lastId();
        for (++it; it != walFiles_.end(); ++it) {
            if (it->second->firstId() > prevLastId + 1) {
                // Found a gap
                LOG(ERROR) << "Found a log id gap before "
                           << it->second->firstId()
                           << ", the previous log id is " << prevLastId;
                logIdAfterLastGap = it->second->firstId();
            }
            prevLastId = it->second->lastId();
        }
        if (logIdAfterLastGap > 0) {
            // Found gap, remove all logs before the last gap
            it = walFiles_.begin();
            while (it->second->firstId() < logIdAfterLastGap) {
                LOG(INFO) << "Removing the wal file \""
                          << it->second->path() << "\"";
                unlink(it->second->path());
                it = walFiles_.erase(it);
            }
        }
    }
}


void FileBasedWal::closeCurrFile() {
    if (currFd_ < 0) {
        // Already closed
        CHECK(!currInfo_);
        return;
    }

    // Close the file
    CHECK_EQ(close(currFd_), 0);
    currFd_ = -1;

    currInfo_->setMTime(time(nullptr));
    DCHECK_EQ(currInfo_->size(), FileUtils::fileSize(currInfo_->path()))
        << currInfo_->path() << " size does not match";
    currInfo_.reset();
}


void FileBasedWal::prepareNewFile(LogID startLogId) {
    std::lock_guard<std::mutex> g(walFilesMutex_);

    CHECK_LT(currFd_, 0)
        << "The current file needs to be closed first";

    // Prepare the last entry in walFiles_
    WalFileInfoPtr info = std::make_shared<WalFileInfo>(
        FileUtils::joinPath(dir_,
                            folly::stringPrintf("%019ld.wal", startLogId)),
        startLogId);
    walFiles_.emplace(std::make_pair(startLogId, info));

    // Create the file for write
    currFd_ = open(
        info->path(),
        O_CREAT | O_EXCL | O_WRONLY | O_APPEND | O_CLOEXEC | O_LARGEFILE,
        0644);
    if (currFd_ < 0) {
        LOG(FATAL) << "Failed to open file \"" << info->path()
                   << "\" (errno: " << errno << "): "
                   << strerror(errno);
    }
    currInfo_ = info;
}


void FileBasedWal::dumpCord(Cord& cord,
                            LogID firstId,
                            LogID lastId,
                            TermID lastTerm) {
    if (cord.size() <= 0) {
        return;
    }

    if (currFd_ < 0) {
        // Need to prepare a new file
        prepareNewFile(firstId);
    }

    auto cb = [this](const char* p, int32_t s) -> bool {
        const char* start = p;
        int32_t size = s;
        do {
            ssize_t res = write(currFd_, start, size);
            if (res < 0) {
                LOG(ERROR) << "Failed to write wal file (" << errno
                           << "): " << strerror(errno) << ", fd " << currFd_;
                return false;
            }
            size -= res;
            start += res;
        } while (size > 0);
        return true;
    };
    if (!cord.applyTo(cb)) {
        LOG(FATAL) << "Failed to flush the wal file";
    } else {
        // Succeeded writing all buffered content, adjust the file size
        currInfo_->setSize(currInfo_->size() + cord.size());
        currInfo_->setLastId(lastId);
        currInfo_->setLastTerm(lastTerm);
    }
}


void FileBasedWal::flushBuffer(BufferPtr buffer) {
    std::lock_guard<std::mutex> flushGuard(flushMutex_);
    if (!buffer || buffer->empty() || buffer->invalid()) {
        return;
    }

    // Rollover if required
    if (buffer->needToRollover()) {
        closeCurrFile();
    }

    Cord cord;
    LogID firstIdInCord = buffer->firstLogId();
    auto accessFn = [&cord, &firstIdInCord, this] (
            LogID id,
            TermID term,
            ClusterID cluster,
            const std::string& log) {
        cord << id << term << int32_t(log.size()) << cluster;
        cord.write(log.data(), log.size());
        cord << int32_t(log.size());

        size_t currSize = currFd_ >= 0 ? currInfo_->size() : 0;
        if (currSize + cord.size() > maxFileSize_) {
            dumpCord(cord, firstIdInCord, id, term);
            // Reset the cord
            cord.clear();
            firstIdInCord = id + 1;

            // Need to close the current file and create a new file
            closeCurrFile();
        }
    };

    // Dump the buffer to file
    auto lastLog = buffer->accessAllLogs(accessFn);

    // Dump the rest if any
    if (!cord.empty()) {
        dumpCord(cord, firstIdInCord, lastLog.first, lastLog.second);
    }

    // Flush the wal file
    if (currFd_ >= 0) {
        CHECK_EQ(fsync(currFd_), 0);
    }

    // Remove the buffer from the list
    {
        std::lock_guard<std::mutex> g(buffersMutex_);

        CHECK_EQ(buffer.get(), buffers_.front().get());
        buffers_.pop_front();
    }
    slotReadyCV_.notify_one();
}


BufferPtr FileBasedWal::createNewBuffer(
        LogID firstId,
        std::unique_lock<std::mutex>& guard) {
    if (buffers_.size() >= policy_.numBuffers) {
        // Log appending is way too fast
        LOG(WARNING) << "Write buffer is exhausted,"
                        " need to wait for vacancy";
        // TODO: Output a counter here
        // Need to wait for a vacant slot
        slotReadyCV_.wait(guard, [self = shared_from_this()] {
            return self->buffers_.size() < self->policy_.numBuffers;
        });
    }

    // Create a new buffer to use
    buffers_.emplace_back(std::make_shared<InMemoryLogBuffer>(firstId));
    return buffers_.back();
}

bool FileBasedWal::appendLogInternal(BufferPtr& buffer,
                                     LogID id,
                                     TermID term,
                                     ClusterID cluster,
                                     std::string msg) {
    if (stopped_) {
        LOG(ERROR) << "WAL has stopped. Do not accept logs any more";
        return false;
    }

    if (lastLogId_ != 0 && firstLogId_ != 0 && id != lastLogId_ + 1) {
        LOG(ERROR) << "There is a gap in the log id. The last log id is "
                   << lastLogId_
                   << ", and the id being appended is " << id;
        return false;
    }

    if (!preProcessor_(id, term, cluster, msg)) {
        LOG(ERROR) << "Pre process failed for log " << id;
        return false;
    }

    if (buffer &&
        (buffer->size() +
         msg.size() +
         sizeof(ClusterID) +
         sizeof(TermID) +
         sizeof(LogID) > maxBufferSize_)) {
        // Freeze the current buffer
        if (buffer->freeze()) {
            flusher_->flushBuffer(shared_from_this(), buffer);
        }
        buffer.reset();
    }

    // Create a new buffer if needed
    if (!buffer) {
        std::unique_lock<std::mutex> guard(buffersMutex_);
        buffer = createNewBuffer(id, guard);
    }

    DCHECK_EQ(
        id,
        static_cast<int64_t>(buffer->firstLogId() + buffer->numLogs()));
    buffer->push(term, cluster, std::move(msg));
    lastLogId_ = id;
    lastLogTerm_ = term;
    if (firstLogId_ == 0) {
        firstLogId_ = id;
    }

    return true;
}

bool FileBasedWal::appendLog(LogID id,
                             TermID term,
                             ClusterID cluster,
                             std::string msg) {
    BufferPtr buffer;
    {
        std::lock_guard<std::mutex> g(buffersMutex_);
        if (!buffers_.empty()) {
            buffer = buffers_.back();
        }
    }

    if (!appendLogInternal(buffer, id, term, cluster, std::move(msg))) {
        LOG(ERROR) << "Failed to append log for logId " << id;
        return false;
    }
    return true;
}

bool FileBasedWal::appendLogs(LogIterator& iter) {
    BufferPtr buffer;
    {
        std::lock_guard<std::mutex> g(buffersMutex_);
        if (!buffers_.empty()) {
            buffer = buffers_.back();
        }
    }

    for (; iter.valid(); ++iter) {
        if (!appendLogInternal(buffer,
                               iter.logId(),
                               iter.logTerm(),
                               iter.logSource(),
                               iter.logMsg().toString())) {
            LOG(ERROR) << "Failed to append log for logId "
                       << iter.logId();
            return false;
        }
    }

    return true;
}


std::unique_ptr<LogIterator> FileBasedWal::iterator(LogID firstLogId,
                                                    LogID lastLogId) {
    return std::unique_ptr<LogIterator>(
        new FileBasedWalIterator(shared_from_this(),
                                 firstLogId,
                                 lastLogId));
}


bool FileBasedWal::rollbackToLog(LogID id) {
    std::lock_guard<std::mutex> flushGuard(flushMutex_);
    bool foundTarget{false};
    if (id < firstLogId_ - 1 || id > lastLogId_) {
        LOG(ERROR) << "Rollback target id " << id
                   << " is not in the range of ["
                   << firstLogId_ << ","
                   << lastLogId_ << "] of WAL";
        return false;
    }

    BufferPtr buf = nullptr;
    {
        // First rollback from buffers
        std::unique_lock<std::mutex> g(buffersMutex_);

        // Remove all buffers that are rolled back
        auto it = buffers_.begin();
        while (it != buffers_.end() && (*it)->firstLogId() <= id) {
            ++it;
        }
        while (it != buffers_.end()) {
            (*it)->markInvalid();
            it = buffers_.erase(it);
        }

        if (!buffers_.empty()) {
            // Found the target log id
            buf = buffers_.back();
            foundTarget = true;
            lastLogId_ = id;
            lastLogTerm_ = buf->getTerm(lastLogId_ - buf->firstLogId());

            // Create a new buffer starting from id + 1
            createNewBuffer(id + 1, g);
            // Since the log id is rolled back, we need to close
            // the previous wal file
            buffers_.back()->rollover();
        }
    }

    if (foundTarget) {
        CHECK(buf != nullptr);
       if (buf->freeze()) {
            // Flush the incomplete buffer which the target log id resides in
            flusher_->flushBuffer(shared_from_this(), buf);
        }
    }

    int fd{-1};
    while (!foundTarget) {
        LOG(WARNING) << "Need to rollback from files."
                        " This is an expensive operation."
                        " Please make sure it is correct and necessary";

        // Close the current file fist
        closeCurrFile();

        std::lock_guard<std::mutex> g(walFilesMutex_);
        if (walFiles_.empty()) {
            CHECK_EQ(id, 0);
            foundTarget = true;
            lastLogId_ = 0;
            break;
        }

        auto it = walFiles_.upper_bound(id);
        CHECK(it != walFiles_.end());

        // We need to remove wal files whose entire log range
        // are rolled back
        while (it != walFiles_.end()) {
            // Need to remove the file
            VLOG(2) << "Removing file " << it->second->path();
            unlink(it->second->path());
            it = walFiles_.erase(it);
        }

        if (walFiles_.empty()) {
            CHECK_EQ(id, 0);
            foundTarget = true;
            lastLogId_ = 0;
            break;
        }

        fd = open(walFiles_.rbegin()->second->path(), O_RDONLY);
        CHECK_GE(fd, 0) << "Failed to open file \""
                        << walFiles_.rbegin()->second->path()
                        << "\" (" << errno << "): "
                        << strerror(errno);
        lastLogId_ = id;
        break;
    }

    // Find the current log entry
    if (fd >= 0) {
        size_t pos = 0;
        while (true) {
            LogID logId;
            // Read the logID
            CHECK_EQ(pread(fd,
                           reinterpret_cast<char*>(&logId),
                           sizeof(LogID),
                           pos),
                     static_cast<ssize_t>(sizeof(LogID)));
            // Read the termID
            CHECK_EQ(pread(fd,
                           reinterpret_cast<char*>(&lastLogTerm_),
                           sizeof(TermID),
                           pos + sizeof(LogID)),
                     static_cast<ssize_t>(sizeof(TermID)));
            // Read the log length
            int32_t msgLen = 0;
            CHECK_EQ(pread(fd,
                           reinterpret_cast<char*>(&msgLen),
                           sizeof(int32_t),
                           pos + sizeof(LogID) + sizeof(TermID)),
                     static_cast<ssize_t>(sizeof(int32_t)));
            if (logId == lastLogId_) {
                foundTarget = true;
                break;
            }
            pos += sizeof(LogID)
                   + sizeof(TermID)
                   + sizeof(int32_t) * 2
                   + msgLen
                   + sizeof(ClusterID);
        }
        close(fd);
    }

    CHECK(foundTarget);

    return true;
}

bool FileBasedWal::reset() {
    std::lock_guard<std::mutex> flushGuard(flushMutex_);
    closeCurrFile();
    {
        std::lock_guard<std::mutex> g(buffersMutex_);
        buffers_.clear();
    }
    {
        std::lock_guard<std::mutex> g(walFilesMutex_);
        walFiles_.clear();
    }
    std::vector<std::string> files =
        FileUtils::listAllFilesInDir(dir_.c_str(), false, "*.wal");
    for (auto& fn : files) {
        auto absFn = FileUtils::joinPath(dir_, fn);
        LOG(INFO) << "Removing " << absFn;
        unlink(absFn.c_str());
    }
    lastLogId_ = firstLogId_ = 0;
    return true;
}

size_t FileBasedWal::accessAllWalInfo(std::function<bool(WalFileInfoPtr info)> fn) const {
    std::lock_guard<std::mutex> g(walFilesMutex_);

    size_t count = 0;
    for (auto it = walFiles_.rbegin(); it != walFiles_.rend(); ++it) {
        ++count;
        if (!fn(it->second)) {
            break;
        }
    }

    return count;
}


size_t FileBasedWal::accessAllBuffers(std::function<bool(BufferPtr buffer)> fn) const {
    std::lock_guard<std::mutex> g(buffersMutex_);

    size_t count = 0;
    for (auto it = buffers_.rbegin(); it != buffers_.rend(); ++it) {
        ++count;
        if (!fn(*it)) {
            break;
        }
    }

    return count;
}

}  // namespace wal
}  // namespace nebula

