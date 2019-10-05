/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "kvstore/wal/FileBasedWalIterator.h"
#include "kvstore/wal/FileBasedWal.h"
#include "kvstore/wal/WalFileInfo.h"

namespace nebula {
namespace wal {

FileBasedWalIterator::FileBasedWalIterator(
    std::shared_ptr<FileBasedWal> wal,
    LogID startId,
    LogID lastId)
        : wal_(std::move(wal))
        , currId_(startId) {
    if (lastId >= 0) {
        lastId_ = lastId;
    } else {
        lastId_ = wal_->lastLogId();
    }

    if (currId_ > lastId_) {
        LOG(ERROR) << wal_->idStr_ << "The log " << currId_
                   << " is out of range, the lastLogId is " << lastId_;
        return;
    }

    if (startId < wal_->firstLogId()) {
        LOG(ERROR) << wal_->idStr_ << "The given log id " << startId
                   << " is out of the range, the wal firstLogId is " << wal_->firstLogId();
        currId_ = lastId_ + 1;
        return;
    }

    ringBuffer_ = wal_->ringBuffer_;
}


FileBasedWalIterator::~FileBasedWalIterator() {
    for (auto& fd : fds_) {
        close(fd);
    }
}

LogIterator& FileBasedWalIterator::operator++() {
    ++currId_;
    if (hasReadWalFile_) {
        if (currId_ >= nextFirstId_) {
            // Need to roll over to next file
            VLOG(2) << "Current ID is " << currId_
                    << ", and the first ID in the next file is "
                    << nextFirstId_
                    << ", so need to move to the next file";
            // Close the current file
            CHECK_EQ(close(fds_.front()), 0);
            fds_.pop_front();
            idRanges_.pop_front();

            if (idRanges_.empty()) {
                // Reached the end of wal files, only happens
                // when there is no buffer to read
                currId_ = lastId_ + 1;
                return *this;
            }

            nextFirstId_ = getFirstIdInNextFile();
            CHECK_EQ(currId_, idRanges_.front().first);
            currPos_ = 0;
        } else {
            // Move to the next log
            currPos_ += sizeof(LogID)
                        + sizeof(TermID)
                        + sizeof(int32_t) * 2
                        + currMsgLen_
                        + sizeof(ClusterID);
        }

        if (idRanges_.front().second <= 0) {
            // empty file
            currId_ = lastId_ + 1;
            return *this;
        } else {
            int fd = fds_.front();
            // Read the log length
            CHECK_EQ(pread(fd,
                           reinterpret_cast<char*>(&currMsgLen_),
                           sizeof(int32_t),
                           currPos_ + sizeof(TermID) + sizeof(LogID)),
                     static_cast<ssize_t>(sizeof(int32_t)));
        }
    }

    return *this;
}

void FileBasedWalIterator::loadWalFiles() {
    // We need to read from the WAL files
    wal_->accessAllWalInfo([this] (WalFileInfoPtr info) {
        if (info->firstId() > lastId_) {
            // Skip this file
            return true;
        }
        int fd = open(info->path(), O_RDONLY);
        if (fd < 0) {
            LOG(ERROR) << "Failed to open wal file \""
                       << info->path()
                       << "\" (" << errno << "): "
                       << strerror(errno);
            currId_ = lastId_ + 1;
            return false;
        }
        fds_.push_front(fd);
        idRanges_.push_front(std::make_pair(info->firstId(), info->lastId()));

        if (info->firstId() <= currId_) {
            // Go no further
            return false;
        } else {
            return true;
        }
    });

    if (idRanges_.empty() || idRanges_.front().first > currId_) {
        LOG(ERROR) << "LogID " << currId_
                   << " is out of the wal files range";
        currId_ = lastId_ + 1;
        return;
    }

    nextFirstId_ = getFirstIdInNextFile();
    CHECK_LE(currId_, idRanges_.front().second);

    // Find the correct position in the first WAL file
    currPos_ = 0;
    while (true) {
        LogID logId;
        // Read the logID
        int fd = fds_.front();
        CHECK_EQ(pread(fd,
                       reinterpret_cast<char*>(&logId),
                       sizeof(LogID),
                       currPos_),
                 static_cast<ssize_t>(sizeof(LogID)));
        // Read the log length
        CHECK_EQ(pread(fd,
                       reinterpret_cast<char*>(&currMsgLen_),
                       sizeof(int32_t),
                       currPos_ + sizeof(LogID) + sizeof(TermID)),
                 static_cast<ssize_t>(sizeof(int32_t)));
        if (logId == currId_) {
            break;
        }
        currPos_ += sizeof(LogID)
                    + sizeof(TermID)
                    + sizeof(int32_t) * 2
                    + currMsgLen_
                    + sizeof(ClusterID);
    }
}



bool FileBasedWalIterator::valid() const {
    return currId_ <= lastId_;
}


LogID FileBasedWalIterator::logId() const {
    return currId_;
}

// use logEntry to retrieve wal log
TermID FileBasedWalIterator::logTerm() const {
    LOG(FATAL) << "Should not use this method";
}


ClusterID FileBasedWalIterator::logSource() const {
    LOG(FATAL) << "Should not use this method";
}


folly::StringPiece FileBasedWalIterator::logMsg() const {
    LOG(FATAL) << "Should not use this method";
}

LogEntry FileBasedWalIterator::logEntry() {
    // Try to get log from ring buffer first, if failed to read from buffer,
    // try to read from wal files
    // TODO: need to figure out when can we stop reading from wal file anymore once hit ring buffer
    LogEntry logEntry;
    if (ringBuffer_->getLogEntry(currId_, logEntry)) {
        return logEntry;
    }

    if (!hasReadWalFile_) {
        loadWalFiles();
        hasReadWalFile_ = true;
    }

    LogID logId;
    int fd = fds_.front();
    // Read the logID
    CHECK_EQ(pread(fd,
                   reinterpret_cast<char*>(&logId),
                   sizeof(LogID),
                   currPos_),
             static_cast<ssize_t>(sizeof(LogID))) << "currPos = " << currPos_;

    TermID term;
    CHECK_EQ(pread(fd,
                   reinterpret_cast<char*>(&term),
                   sizeof(TermID),
                   currPos_ + sizeof(LogID)),
             static_cast<ssize_t>(sizeof(TermID)));

    ClusterID cluster = 0;
    CHECK_EQ(pread(fd,
                   &(cluster),
                   sizeof(ClusterID),
                   currPos_ + sizeof(LogID) + sizeof(TermID) + sizeof(int32_t)),
             static_cast<ssize_t>(sizeof(ClusterID)));

    std::string logMsg;
    logMsg.resize(currMsgLen_);
    CHECK_EQ(pread(fd,
                   &(logMsg[0]),
                   currMsgLen_,
                   currPos_
                    + sizeof(LogID)
                    + sizeof(TermID)
                    + sizeof(int32_t)
                    + sizeof(ClusterID)),
             static_cast<ssize_t>(currMsgLen_));
    return LogEntry(logId, term, cluster, std::move(logMsg));
}


LogID FileBasedWalIterator::getFirstIdInNextFile() const {
    auto it = idRanges_.begin();
    ++it;
    if (it == idRanges_.end()) {
        return idRanges_.front().second + 1;
    } else {
        return it->first;
    }
}

}  // namespace wal
}  // namespace nebula


