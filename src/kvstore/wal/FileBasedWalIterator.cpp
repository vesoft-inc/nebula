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
        : wal_(wal)
        , currId_(startId) {
    holder_ = std::make_unique<folly::RWSpinLock::ReadHolder>(wal_->rollbackLock_);
    if (lastId >= 0 && lastId <= wal_->lastLogId()) {
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
    } else {
        // Pick all buffers that match the range [currId_, lastId_]
        wal_->accessAllBuffers([this] (BufferPtr buffer) {
            if (buffer->empty()) {
                // Skip th empty one.
                return true;
            }
            if (lastId_ >= buffer->firstLogId()) {
                buffers_.push_front(buffer);
                firstIdInBuffer_ = buffer->firstLogId();
            }
            if (firstIdInBuffer_ <= currId_) {
                // Go no futher
                currIdx_ = currId_ - firstIdInBuffer_;
                currTerm_ = buffers_.front()->getTerm(currIdx_);
                nextFirstId_ = getFirstIdInNextBuffer();
                return false;
            } else {
                return true;
            }
        });
    }

    if (firstIdInBuffer_ > currId_) {
        // We need to read from the WAL files
        wal_->accessAllWalInfo([this] (WalFileInfoPtr info) {
            if (info->firstId() >= firstIdInBuffer_) {
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
        if (currId_ > idRanges_.front().second) {
            LOG(FATAL) << wal_->idStr_ << "currId_ " << currId_
                       << ", idRanges.front firstLogId " << idRanges_.front().first
                       << ", idRanges.front lastLogId " << idRanges_.front().second
                       << ", idRanges size " << idRanges_.size()
                       << ", lastId_ " << lastId_
                       << ", nextFirstId_ " << nextFirstId_;
        }
    }

    if (!idRanges_.empty()) {
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
            // Read the termID
            CHECK_EQ(pread(fd,
                           reinterpret_cast<char*>(&currTerm_),
                           sizeof(TermID),
                           currPos_ + sizeof(LogID)),
                     static_cast<ssize_t>(sizeof(TermID)));
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
}


FileBasedWalIterator::~FileBasedWalIterator() {
    for (auto& fd : fds_) {
        close(fd);
    }
}


LogIterator& FileBasedWalIterator::operator++() {
    ++currId_;
    if (currId_ < firstIdInBuffer_) {
        // Still in the WAL file
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
                CHECK_EQ(std::numeric_limits<LogID>::max(),
                         firstIdInBuffer_);
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
            LogID logId;
            int fd = fds_.front();
            // Read the logID
            CHECK_EQ(pread(fd,
                           reinterpret_cast<char*>(&logId),
                           sizeof(LogID),
                           currPos_),
                     static_cast<ssize_t>(sizeof(LogID))) << "currPos = " << currPos_;
            CHECK_EQ(currId_, logId);
            // Read the termID
            CHECK_EQ(pread(fd,
                           reinterpret_cast<char*>(&currTerm_),
                           sizeof(TermID),
                           currPos_ + sizeof(LogID)),
                     static_cast<ssize_t>(sizeof(TermID)));
            // Read the log length
            CHECK_EQ(pread(fd,
                           reinterpret_cast<char*>(&currMsgLen_),
                           sizeof(int32_t),
                           currPos_ + sizeof(TermID) + sizeof(LogID)),
                     static_cast<ssize_t>(sizeof(int32_t)));
        }
    } else if (currId_ <= lastId_) {
        // Need to adjust nextFirstId_, in case we just start
        // reading buffers
        if (currId_ == firstIdInBuffer_) {
            nextFirstId_ = getFirstIdInNextBuffer();
            CHECK_LT(firstIdInBuffer_, nextFirstId_);
            currIdx_ = -1;
        }

        // Read from buffer
        if (currId_ >= nextFirstId_) {
            // Roll over to next buffer
            if (buffers_.size() == 1) {
                LOG(FATAL) << wal_->idStr_
                           << ", currId_ " << currId_
                           << ", nextFirstId_ " << nextFirstId_
                           << ", firstIdInBuffer_ " << firstIdInBuffer_
                           << ", lastId_ " << lastId_
                           << ", firstLogId in buffer " << buffers_.front()->firstLogId()
                           << ", lastLogId in buffer " << buffers_.front()->lastLogId()
                           << ", numLogs in buffer " <<  buffers_.front()->size();
            }
            buffers_.pop_front();
            CHECK(!buffers_.empty());
            CHECK_EQ(currId_, buffers_.front()->firstLogId());

            nextFirstId_ = getFirstIdInNextBuffer();
            currIdx_ = 0;
        } else {
            ++currIdx_;
        }
        currTerm_ = buffers_.front()->getTerm(currIdx_);
    }

    return *this;
}


bool FileBasedWalIterator::valid() const {
    return currId_ <= lastId_;
}


LogID FileBasedWalIterator::logId() const {
    return currId_;
}


TermID FileBasedWalIterator::logTerm() const {
    return currTerm_;
}


ClusterID FileBasedWalIterator::logSource() const {
    if (currId_ >= firstIdInBuffer_) {
        // Retrieve from the buffer
        DCHECK(!buffers_.empty());
        return buffers_.front()->getCluster(currIdx_);
    } else {
        // Retrieve from the file
        DCHECK(!fds_.empty());

        ClusterID cluster = 0;
        CHECK_EQ(pread(fds_.front(),
                       &(cluster),
                       sizeof(ClusterID),
                       currPos_
                        + sizeof(LogID)
                        + sizeof(TermID)
                        + sizeof(int32_t)),
                 static_cast<ssize_t>(sizeof(ClusterID)))
            << "Failed to read. Curr position is " << currPos_
            << ", expected read length is " << sizeof(ClusterID)
            << " (errno: " << errno << "): " << strerror(errno);

        return cluster;
    }
}


folly::StringPiece FileBasedWalIterator::logMsg() const {
    if (currId_ >= firstIdInBuffer_) {
        // Retrieve from the buffer
        DCHECK(!buffers_.empty());
        return buffers_.front()->getLog(currIdx_);
    } else {
        // Retrieve from the file
        DCHECK(!fds_.empty());

        currLog_.resize(currMsgLen_);
        CHECK_EQ(pread(fds_.front(),
                       &(currLog_[0]),
                       currMsgLen_,
                       currPos_
                        + sizeof(LogID)
                        + sizeof(TermID)
                        + sizeof(int32_t)
                        + sizeof(ClusterID)),
                 static_cast<ssize_t>(currMsgLen_))
            << "Failed to read. Curr position is " << currPos_
            << ", expected read length is " << currMsgLen_
            << " (errno: " << errno << "): " << strerror(errno);

        return currLog_;
    }
}


LogID FileBasedWalIterator::getFirstIdInNextBuffer() const {
    auto it = buffers_.begin();
    ++it;
    if (it == buffers_.end()) {
        return buffers_.front()->lastLogId() + 1;
    } else {
        return (*it)->firstLogId();
    }
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


