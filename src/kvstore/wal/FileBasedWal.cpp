/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/wal/FileBasedWal.h"
#include "kvstore/wal/FileBasedWalIterator.h"
#include "fs/FileUtils.h"
#include "time/WallClock.h"

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
        const std::string& idStr,
        FileBasedWalPolicy policy,
        PreProcessor preProcessor) {
    return std::shared_ptr<FileBasedWal>(
        new FileBasedWal(dir, idStr, std::move(policy), std::move(preProcessor)));
}


FileBasedWal::FileBasedWal(const folly::StringPiece dir,
                           const std::string& idStr,
                           FileBasedWalPolicy policy,
                           PreProcessor preProcessor)
        : dir_(dir.toString())
        , idStr_(idStr)
        , policy_(std::move(policy))
        , maxFileSize_(policy_.fileSize)
        , maxBufferSize_(policy_.bufferSize)
        , preProcessor_(std::move(preProcessor)) {
    // Make sure WAL directory exist
    if (FileUtils::fileType(dir_.c_str()) == fs::FileType::NOTEXIST) {
        FileUtils::makeDir(dir_);
    }

    scanAllWalFiles();
    if (!walFiles_.empty()) {
        firstLogId_ = walFiles_.begin()->second->firstId();
        auto& info = walFiles_.rbegin()->second;
        if (info->lastId() <= 0 && walFiles_.size() > 1) {
            auto it = walFiles_.rbegin();
            it++;
            lastLogId_ = info->firstId() - 1;
            lastLogTerm_ = readTermId(it->second->path(), lastLogId_);
        } else {
            lastLogId_ = info->lastId();
            lastLogTerm_ = info->lastTerm();
        }
        currFd_ = open(info->path(), O_WRONLY | O_APPEND);
        currInfo_ = info;
        CHECK_GE(currFd_, 0);
    }
}


FileBasedWal::~FileBasedWal() {
    // FileBasedWal inherits from std::enable_shared_from_this, so at this
    // moment, there should have no other thread holding this WAL object
    // Close the last file
    closeCurrFile();
    LOG(INFO) << idStr_ << "~FileBasedWal, dir = " << dir_;
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
            // Found an empty WAL file
            LOG(WARNING) << "Found empty wal file \"" << fn << "\"";
            info->setLastId(0);
            info->setLastTerm(0);
            walFiles_.insert(std::make_pair(startIdFromName, info));
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

    CHECK_EQ(fsync(currFd_), 0);
    // Close the file
    CHECK_EQ(close(currFd_), 0);
    currFd_ = -1;

    currInfo_->setMTime(::time(nullptr));
    DCHECK_EQ(currInfo_->size(), FileUtils::fileSize(currInfo_->path()))
        << currInfo_->path() << " size does not match";
    currInfo_.reset();
}


void FileBasedWal::prepareNewFile(LogID startLogId) {
    CHECK_LT(currFd_, 0)
        << "The current file needs to be closed first";

    // Prepare the last entry in walFiles_
    WalFileInfoPtr info = std::make_shared<WalFileInfo>(
        FileUtils::joinPath(dir_,
                            folly::stringPrintf("%019ld.wal", startLogId)),
        startLogId);
    VLOG(1) << idStr_ << "Write new file " << info->path();
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


TermID FileBasedWal::readTermId(const char* path, LogID logId) {
    int32_t fd = open(path, O_RDONLY);
    if (fd < 0) {
        LOG(FATAL) << "Failed to open file \"" << path
                   << "\" (errno: " << errno << "): "
                   << strerror(errno);
    }

    size_t pos = 0;
    while (true) {
        // Read the log Id
        LogID id = 0;
        if (pread(fd, &id, sizeof(LogID), pos) != sizeof(LogID)) {
            LOG(ERROR) << "Failed to read the log id (errno "
                       << errno << "): " << strerror(errno);
            close(fd);
            return 0;
        }

        // Read the term Id
        TermID term = 0;
        if (pread(fd, &term, sizeof(TermID), pos + sizeof(LogID)) != sizeof(TermID)) {
            LOG(ERROR) << "Failed to read the term id (errno "
                       << errno << "): " << strerror(errno);
            close(fd);
            return 0;
        }

        if (id == logId) {
            // Found
            close(fd);
            return term;
        }

        // Read the message length
        int32_t len;
        if (pread(fd, &len, sizeof(int32_t), pos + sizeof(LogID) + sizeof(TermID))
                != sizeof(int32_t)) {
            LOG(ERROR) << "Failed to read the message length (errno "
                       << errno << "): " << strerror(errno);
            close(fd);
            return 0;
        }

        // Move to the next log
        pos += sizeof(LogID)
               + sizeof(TermID)
               + sizeof(ClusterID)
               + 2 * sizeof(int32_t)
               + len;
    }

    LOG(FATAL) << "Should never reach here";
}


BufferPtr FileBasedWal::getLastBuffer(LogID id, size_t expectedToWrite) {
    std::unique_lock<std::mutex> g(buffersMutex_);
    if (!buffers_.empty()) {
        if (buffers_.back()->size() + expectedToWrite <= maxBufferSize_) {
            return buffers_.back();
        }
        // Need to rollover to a new buffer
        if (buffers_.size() == policy_.numBuffers) {
            // Need to pop the first one
            buffers_.pop_front();
        }
        CHECK_LT(buffers_.size(), policy_.numBuffers);
    }
    buffers_.emplace_back(std::make_shared<InMemoryLogBuffer>(id));
    return buffers_.back();
}


bool FileBasedWal::appendLogInternal(LogID id,
                                     TermID term,
                                     ClusterID cluster,
                                     std::string msg) {
    if (stopped_) {
        LOG(ERROR) << idStr_ << "WAL has stopped. Do not accept logs any more";
        return false;
    }

    if (lastLogId_ != 0 && firstLogId_ != 0 && id != lastLogId_ + 1) {
        LOG(ERROR) << idStr_ << "There is a gap in the log id. The last log id is "
                   << lastLogId_
                   << ", and the id being appended is " << id;
        return false;
    }

    if (!preProcessor_(id, term, cluster, msg)) {
        LOG(ERROR) << idStr_ << "Pre process failed for log " << id;
        return false;
    }

    // Write to the WAL file first
    std::string strBuf;
    strBuf.reserve(sizeof(LogID)
                    + sizeof(TermID)
                    + sizeof(ClusterID)
                    + msg.size()
                    + 2 * sizeof(int32_t));
    strBuf.append(reinterpret_cast<char*>(&id), sizeof(LogID));
    strBuf.append(reinterpret_cast<char*>(&term), sizeof(TermID));
    int32_t len = msg.size();
    strBuf.append(reinterpret_cast<char*>(&len), sizeof(int32_t));
    strBuf.append(reinterpret_cast<char*>(&cluster), sizeof(ClusterID));
    strBuf.append(reinterpret_cast<const char*>(msg.data()), msg.size());
    strBuf.append(reinterpret_cast<char*>(&len), sizeof(int32_t));

    // Prepare the WAL file if it's not opened
    if (currFd_ < 0) {
        prepareNewFile(id);
    } else if (currInfo_->size() + strBuf.size() > maxFileSize_) {
        // Need to roll over
        closeCurrFile();

        std::lock_guard<std::mutex> g(walFilesMutex_);
        prepareNewFile(id);
    }

    ssize_t bytesWritten = write(currFd_, strBuf.data(), strBuf.size());
    CHECK_EQ(bytesWritten, strBuf.size());
    currInfo_->setSize(currInfo_->size() + strBuf.size());
    currInfo_->setLastId(id);
    currInfo_->setLastTerm(term);

    lastLogId_ = id;
    lastLogTerm_ = term;
    if (firstLogId_ == 0) {
        firstLogId_ = id;
    }

    // Append to the in-memory buffer
    auto buffer = getLastBuffer(id, strBuf.size());
    DCHECK_EQ(id, static_cast<int64_t>(buffer->firstLogId() + buffer->numLogs()));
    buffer->push(term, cluster, std::move(msg));

    return true;
}


bool FileBasedWal::appendLog(LogID id,
                             TermID term,
                             ClusterID cluster,
                             std::string msg) {
    if (!appendLogInternal(id, term, cluster, std::move(msg))) {
        LOG(ERROR) << "Failed to append log for logId " << id;
        return false;
    }
    return true;
}


bool FileBasedWal::appendLogs(LogIterator& iter) {
    for (; iter.valid(); ++iter) {
        if (!appendLogInternal(iter.logId(),
                               iter.logTerm(),
                               iter.logSource(),
                               iter.logMsg().toString())) {
            LOG(ERROR) << idStr_ << "Failed to append log for logId "
                       << iter.logId();
            return false;
        }
    }

    return true;
}


std::unique_ptr<LogIterator> FileBasedWal::iterator(LogID firstLogId,
                                                    LogID lastLogId) {
    return std::make_unique<FileBasedWalIterator>(shared_from_this(), firstLogId, lastLogId);
}


bool FileBasedWal::rollbackToLog(LogID id) {
    if (id < firstLogId_ - 1 || id > lastLogId_) {
        LOG(ERROR) << idStr_ << "Rollback target id " << id
                   << " is not in the range of ["
                   << firstLogId_ << ","
                   << lastLogId_ << "] of WAL";
        return false;
    }

    //-----------------------
    // 1. Roll back WAL files
    //-----------------------

    // First close the current file
    closeCurrFile();

    {
        std::lock_guard<std::mutex> g(walFilesMutex_);

        if (!walFiles_.empty()) {
            auto it = walFiles_.upper_bound(id);
            // We need to remove wal files whose entire log range
            // are rolled back
            while (it != walFiles_.end()) {
                // Need to remove the file
                VLOG(1) << "Removing file " << it->second->path();
                unlink(it->second->path());
                it = walFiles_.erase(it);
            }
        }

        if (walFiles_.empty()) {
            // All WAL files are gone
            CHECK(id == firstLogId_ - 1 || id == 0);
            firstLogId_ = 0;
            lastLogId_ = 0;
            lastLogTerm_ = 0;
        } else {
            VLOG(1) << "Roll back to log " << id
                    << ", the last WAL file is now \""
                    << walFiles_.rbegin()->second->path() << "\"";
            lastLogId_ = id;
            lastLogTerm_ = readTermId(walFiles_.rbegin()->second->path(), lastLogId_);
        }

        // Create the next WAL file
        prepareNewFile(lastLogId_ + 1);
    }

    //------------------------------
    // 2. Roll back in-memory buffers
    //------------------------------
    {
        // First rollback from buffers
        std::unique_lock<std::mutex> g(buffersMutex_);

        // Remove all buffers that are rolled back
        auto it = buffers_.begin();
        while (it != buffers_.end() && (*it)->firstLogId() <= id) {
            it++;
        }
        while (it != buffers_.end()) {
            it = buffers_.erase(it);
        }

        // Need to rollover to a new buffer
        if (buffers_.size() == policy_.numBuffers) {
            // Need to pop the first one
            buffers_.pop_front();
        }
        buffers_.emplace_back(std::make_shared<InMemoryLogBuffer>(id + 1));
    }

    return true;
}


bool FileBasedWal::reset() {
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

void FileBasedWal::cleanWAL(int32_t ttl) {
    std::lock_guard<std::mutex> g(walFilesMutex_);
    if (walFiles_.empty()) {
        return;
    }
    auto now = time::WallClock::fastNowInSec();
    // We skip the latest wal file because it is beging written now.
    size_t index = 0;
    auto it = walFiles_.begin();
    auto size = walFiles_.size();
    int count = 0;
    int walTTL = ttl == 0 ? policy_.ttl : ttl;
    while (it != walFiles_.end()) {
        if (index++ < size - 1 &&  (now - it->second->mtime() > walTTL)) {
            VLOG(1) << "Clean wals, Remove " << it->second->path();
            unlink(it->second->path());
            it = walFiles_.erase(it);
            count++;
        } else {
            ++it;
        }
    }
    if (count > 0) {
        LOG(INFO) << idStr_ << "Clean wals number " << count;
    }
    firstLogId_ = walFiles_.begin()->second->firstId();
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
