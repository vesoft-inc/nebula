/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/FileUtils.h"
#include "common/time/WallClock.h"
#include "kvstore/wal/FileBasedWal.h"
#include "kvstore/wal/WalFileIterator.h"
#include <utime.h>

DEFINE_int32(wal_ttl, 14400, "Default wal ttl");
DEFINE_int64(wal_file_size, 16 * 1024 * 1024, "Default wal file size");
DEFINE_int32(wal_buffer_size, 8 * 1024 * 1024, "Default wal buffer size");
DEFINE_bool(wal_sync, false, "Whether fsync needs to be called every write");

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
        FileBasedWalInfo info,
        FileBasedWalPolicy policy,
        PreProcessor preProcessor,
        std::shared_ptr<kvstore::DiskManager> diskMan) {
    return std::shared_ptr<FileBasedWal>(new FileBasedWal(
        dir, std::move(info), std::move(policy), std::move(preProcessor), diskMan));
}


FileBasedWal::FileBasedWal(const folly::StringPiece dir,
                           FileBasedWalInfo walInfo,
                           FileBasedWalPolicy policy,
                           PreProcessor preProcessor,
                           std::shared_ptr<kvstore::DiskManager> diskMan)
        : dir_(dir.toString())
        , idStr_(walInfo.idStr_)
        , spaceId_(walInfo.spaceId_)
        , partId_(walInfo.partId_)
        , policy_(std::move(policy))
        , preProcessor_(std::move(preProcessor))
        , diskMan_(diskMan) {
    // Make sure WAL directory exist
    if (FileUtils::fileType(dir_.c_str()) == fs::FileType::NOTEXIST) {
        if (!FileUtils::makeDir(dir_)) {
            LOG(FATAL) << "MakeDIR " << dir_ << " failed";
        }
    }

    logBuffer_ = AtomicLogBuffer::instance(policy_.bufferSize);
    scanAllWalFiles();
    if (!walFiles_.empty()) {
        firstLogId_ = walFiles_.begin()->second->firstId();
        auto& info = walFiles_.rbegin()->second;
        lastLogId_ = info->lastId();
        lastLogTerm_ = info->lastTerm();
        LOG(INFO) << idStr_ << "lastLogId in wal is " << lastLogId_
                  << ", lastLogTerm is " << lastLogTerm_
                  << ", path is " << info->path();
        currFd_ = open(info->path(), O_WRONLY | O_APPEND);
        currInfo_ = info;
        if (currFd_ < 0) {
            LOG(FATAL) << "Failed to open the file \"" << info->path() << "\" ("
                       << errno << "): " << strerror(errno);
        }
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
    std::vector<std::string> files = FileUtils::listAllFilesInDir(dir_.c_str(), false, "*.wal");
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
        walFiles_.insert(std::make_pair(startIdFromName, info));

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
    }

    if (!walFiles_.empty()) {
        auto it = walFiles_.rbegin();
        // Try to scan last wal, if it is invalid or empty, scan the privous one
        scanLastWal(it->second, it->second->firstId());
        if (it->second->lastId() <= 0) {
            unlink(it->second->path());
            walFiles_.erase(it->first);
        }
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

    if (!policy_.sync) {
        if (::fsync(currFd_) == -1) {
            LOG(WARNING) << "sync wal \"" << currInfo_->path()
                         << "\" failed, error: " << strerror(errno);
        }
    }

    // Close the file
    if (::close(currFd_) == -1) {
        LOG(WARNING) << "close wal \"" << currInfo_->path()
                     << "\" failed, error: " << strerror(errno);
    }
    currFd_ = -1;

    auto now = time::WallClock::fastNowInSec();
    currInfo_->setMTime(now);
    struct utimbuf timebuf;
    timebuf.modtime = currInfo_->mtime();
    timebuf.actime = currInfo_->mtime();
    VLOG(1) << "Close cur file " << currInfo_->path() << ", mtime: " << currInfo_->mtime();
    utime(currInfo_->path(), &timebuf);
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


void FileBasedWal::rollbackInFile(WalFileInfoPtr info, LogID logId) {
    auto path = info->path();
    int32_t fd = open(path, O_RDWR);
    if (fd < 0) {
        LOG(FATAL) << "Failed to open file \"" << path
                   << "\" (errno: " << errno << "): "
                   << strerror(errno);
    }

    size_t pos = 0;
    LogID id = 0;
    TermID term = 0;
    while (true) {
        // Read the log Id
        if (pread(fd, &id, sizeof(LogID), pos) != sizeof(LogID)) {
            LOG(ERROR) << "Failed to read the log id (errno "
                       << errno << "): " << strerror(errno);
            break;
        }

        // Read the term Id
        if (pread(fd, &term, sizeof(TermID), pos + sizeof(LogID)) != sizeof(TermID)) {
            LOG(ERROR) << "Failed to read the term id (errno "
                       << errno << "): " << strerror(errno);
            break;
        }

        // Read the message length
        int32_t len;
        if (pread(fd, &len, sizeof(int32_t), pos + sizeof(LogID) + sizeof(TermID))
                != sizeof(int32_t)) {
            LOG(ERROR) << "Failed to read the message length (errno "
                       << errno << "): " << strerror(errno);
            break;
        }

        // Move to the next log
        pos += sizeof(LogID)
               + sizeof(TermID)
               + sizeof(ClusterID)
               + 2 * sizeof(int32_t)
               + len;

        if (id == logId) {
            break;
        }
    }

    if (id != logId) {
        LOG(FATAL) << idStr_ << "Didn't found log " << logId << " in " << path;
    }
    lastLogId_ = logId;
    lastLogTerm_ = term;
    LOG(INFO) << idStr_ << "Rollback to log " << logId;

    CHECK_GT(pos, 0) << "This wal should have been deleted";
    if (pos < FileUtils::fileSize(path)) {
        LOG(INFO) << idStr_ << "Need to truncate from offset " << pos;
        if (ftruncate(fd, pos) < 0) {
            LOG(FATAL) << "Failed to truncate file \"" << path
                       << "\" (errno: " << errno << "): "
                       << strerror(errno);
        }
        info->setSize(pos);
    }
    info->setLastId(id);
    info->setLastTerm(term);
    close(fd);
}


void FileBasedWal::scanLastWal(WalFileInfoPtr info, LogID firstId) {
    auto* path = info->path();
    int32_t fd = open(path, O_RDWR);
    if (fd < 0) {
        LOG(FATAL) << "Failed to open file \"" << path
                   << "\" (errno: " << errno << "): "
                   << strerror(errno);
    }

    LogID curLogId = firstId;
    size_t pos = 0;
    LogID id = 0;
    TermID term = 0;
    int32_t head = 0;
    int32_t foot = 0;
    while (true) {
        // Read the log Id
        if (pread(fd, &id, sizeof(LogID), pos) != sizeof(LogID)) {
            break;
        }

        if (id != curLogId) {
            LOG(ERROR) << "LogId is not consistent" << id << " " << curLogId;
            break;
        }

        // Read the term Id
        if (pread(fd, &term, sizeof(TermID), pos + sizeof(LogID)) != sizeof(TermID)) {
            break;
        }

        // Read the message length
        if (pread(fd, &head, sizeof(int32_t), pos + sizeof(LogID) + sizeof(TermID))
                != sizeof(int32_t)) {
            break;
        }

        if (pread(fd, &foot, sizeof(int32_t),
                  pos + sizeof(LogID) + sizeof(TermID) + sizeof(int32_t) + sizeof(ClusterID) + head)
                != sizeof(int32_t)) {
            break;
        }

        if (head != foot) {
            LOG(ERROR) << "Message size doen't match: " << head << " != " << foot;
            break;
        }

        info->setLastTerm(term);
        info->setLastId(id);

        // Move to the next log
        pos += sizeof(LogID)
               + sizeof(TermID)
               + sizeof(ClusterID)
               + sizeof(int32_t)
               + head
               + sizeof(int32_t);

        ++curLogId;
    }

    if (0 < pos && pos < FileUtils::fileSize(path)) {
        LOG(WARNING) << "Invalid wal " << path << ", truncate from offset " << pos;
        if (ftruncate(fd, pos) < 0) {
            LOG(FATAL) << "Failed to truncate file \"" << path
                       << "\" (errno: " << errno << "): "
                       << strerror(errno);
        }
        info->setSize(pos);
    }

    close(fd);
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
    } else if (currInfo_->size() + strBuf.size() > policy_.fileSize) {
        // Need to roll over
        closeCurrFile();

        std::lock_guard<std::mutex> g(walFilesMutex_);
        prepareNewFile(id);
    }

    ssize_t bytesWritten = write(currFd_, strBuf.data(), strBuf.size());
    if (bytesWritten != (ssize_t)strBuf.size()) {
        LOG(FATAL) << idStr_ << "bytesWritten:" << bytesWritten << ", expected:" << strBuf.size()
                   << ", error:" << strerror(errno);
    }

    if (policy_.sync && ::fsync(currFd_) == -1) {
        LOG(WARNING) << "sync wal \"" << currInfo_->path()
                     << "\" failed, error: " << strerror(errno);
    }
    currInfo_->setSize(currInfo_->size() + strBuf.size());
    currInfo_->setLastId(id);
    currInfo_->setLastTerm(term);

    lastLogId_ = id;
    lastLogTerm_ = term;
    if (firstLogId_ == 0) {
        firstLogId_ = id;
    }

    logBuffer_->push(id, term, cluster, std::move(msg));
    return true;
}


bool FileBasedWal::appendLog(LogID id,
                             TermID term,
                             ClusterID cluster,
                             std::string msg) {
    if (diskMan_ && !diskMan_->hasEnoughSpace(spaceId_, partId_)) {
        LOG_EVERY_N(WARNING, 100) << idStr_ << "Failed to appendLogs because of no more space";
        return false;
    }
    if (!appendLogInternal(id, term, cluster, std::move(msg))) {
        LOG(ERROR) << "Failed to append log for logId " << id;
        return false;
    }
    return true;
}


bool FileBasedWal::appendLogs(LogIterator& iter) {
    if (diskMan_ && !diskMan_->hasEnoughSpace(spaceId_, partId_)) {
        LOG_EVERY_N(WARNING, 100) << idStr_ << "Failed to appendLogs because of no more space";
        return false;
    }
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
    auto iter = logBuffer_->iterator(firstLogId, lastLogId);
    if (iter->valid()) {
        return iter;
    }
    return std::make_unique<WalFileIterator>(shared_from_this(), firstLogId, lastLogId);
}

bool FileBasedWal::linkCurrentWAL(const char* newPath) {
    closeCurrFile();
    std::lock_guard<std::mutex> g(walFilesMutex_);
    if (walFiles_.empty()) {
        LOG(INFO) << idStr_ << "No wal files found, skip link";
        return true;
    }
    if (fs::FileUtils::exist(newPath) &&
        !fs::FileUtils::remove(newPath, true)) {
        LOG(ERROR) << "Remove exist dir failed of wal : " << newPath;
        return false;
     }
    if (!fs::FileUtils::makeDir(newPath)) {
        LOG(INFO) << idStr_ << "Link file parent dir make failed : " << newPath;
        return false;
    }

    for (const auto& f : walFiles_) {
        // Using the original wal file name.
        auto targetFile =
            fs::FileUtils::joinPath(newPath, folly::stringPrintf("%019ld.wal", f.first));

        if (link(f.second->path(), targetFile.data()) != 0) {
            LOG(INFO) << idStr_ << "Create link failed for " << f.second->path() << " on "
                      << newPath << ", error:" << strerror(errno);
            return false;
        }
        LOG(INFO) << idStr_ << "Create link success for " << f.second->path() << " on " << newPath;
    }

    return true;
}

bool FileBasedWal::rollbackToLog(LogID id) {
    if (id < firstLogId_ - 1 || id > lastLogId_) {
        LOG(ERROR) << idStr_ << "Rollback target id " << id
                   << " is not in the range of ["
                   << firstLogId_ << ","
                   << lastLogId_ << "] of WAL";
        return false;
    }

    folly::RWSpinLock::WriteHolder holder(rollbackLock_);
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
            rollbackInFile(walFiles_.rbegin()->second, id);
        }
    }

    //------------------------------
    // 2. Roll back in-memory buffers
    //------------------------------
    logBuffer_->reset();

    return true;
}


bool FileBasedWal::reset() {
    closeCurrFile();
    logBuffer_->reset();
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

void FileBasedWal::cleanWAL() {
    std::lock_guard<std::mutex> g(walFilesMutex_);
    if (walFiles_.empty()) {
        return;
    }
    auto now = time::WallClock::fastNowInSec();
    // In theory we only need to keep the latest wal file because it is beging written now.
    // However, sometimes will trigger raft snapshot even only a small amount of logs is missing,
    // especially when we reboot all storage, so se keep one more wal.
    size_t index = 0;
    auto it = walFiles_.begin();
    auto size = walFiles_.size();
    if (size < 2) {
        return;
    }
    int count = 0;
    int walTTL = FLAGS_wal_ttl;
    while (it != walFiles_.end()) {
        // keep at least two wal
        if (index++ < size - 2 && (now - it->second->mtime() > walTTL)) {
            VLOG(1) << "Clean wals, Remove " << it->second->path() << ", now: " << now
                    << ", mtime: " << it->second->mtime();
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

void FileBasedWal::cleanWAL(LogID id) {
    std::lock_guard<std::mutex> g(walFilesMutex_);
    if (walFiles_.empty()) {
        return;
    }

    if (walFiles_.rbegin()->second->lastId() < id) {
        LOG(WARNING) << "Try to clean wal not existed " << id
                     << ", lastWal is " << walFiles_.rbegin()->second->lastId();
        return;
    }

    // remove wal file that lastId is less than target
    auto iter = walFiles_.begin();
    while (iter != walFiles_.end()) {
        if (iter->second->lastId() < id) {
            VLOG(1) << "Clean wals, Remove " << iter->second->path();
            unlink(iter->second->path());
            iter = walFiles_.erase(iter);
        } else {
            break;
        }
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

}  // namespace wal
}  // namespace nebula
