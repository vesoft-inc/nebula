/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/wal/WalFileIterator.h"

#include "common/base/Base.h"
#include "kvstore/wal/FileBasedWal.h"
#include "kvstore/wal/WalFileInfo.h"

namespace nebula {
namespace wal {

WalFileIterator::WalFileIterator(std::shared_ptr<FileBasedWal> wal, LogID startId, LogID lastId)
    : wal_(wal), lastId_(lastId), currId_(startId) {
  if (currId_ > lastId_) {
    VLOG(3) << wal_->idStr_ << "The log " << currId_ << " is out of range, the lastLogId is "
            << lastId_;
    return;
  }

  if (startId < wal_->firstLogId()) {
    VLOG(3) << wal_->idStr_ << "The given log id " << startId
            << " is out of the range, the wal firstLogId is " << wal_->firstLogId();
    currId_ = lastId_ + 1;
    return;
  }

  // We need to read from the WAL files
  wal_->accessAllWalInfo([this](WalFileInfoPtr info) {
    int fd = open(info->path(), O_RDONLY);
    if (fd < 0) {
      LOG(WARNING) << "Failed to open wal file \"" << info->path() << "\" (" << errno
                   << "): " << strerror(errno);
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
    VLOG(3) << "LogID " << currId_ << " is out of the wal files range";
    currId_ = lastId_ + 1;
    return;
  }

  nextFirstId_ = getFirstIdInNextFile();
  // log in range [startId, lastId] is located in last wal, however, the wal is rollbacked during
  // building the iterator
  if (currId_ > idRanges_.front().second) {
    currId_ = lastId_ + 1;
    return;
  }

  if (!idRanges_.empty()) {
    // Find the correct position in the first WAL file
    currPos_ = 0;
    while (true) {
      LogID logId;
      // Read the logID
      int fd = fds_.front();
      if (pread(fd, reinterpret_cast<char*>(&logId), sizeof(LogID), currPos_) !=
          static_cast<ssize_t>(sizeof(LogID))) {
        eof_ = true;
        break;
      }
      // Read the termID
      if (pread(
              fd, reinterpret_cast<char*>(&currTerm_), sizeof(TermID), currPos_ + sizeof(LogID)) !=
          static_cast<ssize_t>(sizeof(TermID))) {
        eof_ = true;
        break;
      }
      // Read the log length
      if (pread(fd,
                reinterpret_cast<char*>(&currMsgLen_),
                sizeof(int32_t),
                currPos_ + sizeof(LogID) + sizeof(TermID)) !=
          static_cast<ssize_t>(sizeof(int32_t))) {
        eof_ = true;
        break;
      }
      if (logId == currId_) {
        break;
      }
      currPos_ +=
          sizeof(LogID) + sizeof(TermID) + sizeof(int32_t) * 2 + currMsgLen_ + sizeof(ClusterID);
    }
  }
}

WalFileIterator::~WalFileIterator() {
  for (auto& fd : fds_) {
    close(fd);
  }
}

LogIterator& WalFileIterator::operator++() {
  ++currId_;
  if (currId_ >= nextFirstId_) {
    // Need to roll over to next file
    VLOG(4) << "Current ID is " << currId_ << ", and the first ID in the next file is "
            << nextFirstId_ << ", so need to move to the next file";
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
    currPos_ +=
        sizeof(LogID) + sizeof(TermID) + sizeof(int32_t) * 2 + currMsgLen_ + sizeof(ClusterID);
  }

  if (idRanges_.front().second <= 0) {
    // empty file
    currId_ = lastId_ + 1;
    return *this;
  } else {
    LogID logId;
    int fd = fds_.front();
    do {
      // Read the logID
      if (pread(fd, reinterpret_cast<char*>(&logId), sizeof(LogID), currPos_) !=
          static_cast<ssize_t>(sizeof(LogID))) {
        VLOG(3) << "Failed to read logId currPos = " << currPos_;
        eof_ = true;
        break;
      }
      CHECK_EQ(currId_, logId);
      // Read the termID
      if (pread(
              fd, reinterpret_cast<char*>(&currTerm_), sizeof(TermID), currPos_ + sizeof(LogID)) !=
          static_cast<ssize_t>(sizeof(TermID))) {
        VLOG(3) << "Failed to read term currPos = " << currPos_;
        eof_ = true;
        break;
      }
      // Read the log length
      if (pread(fd,
                reinterpret_cast<char*>(&currMsgLen_),
                sizeof(int32_t),
                currPos_ + sizeof(TermID) + sizeof(LogID)) !=
          static_cast<ssize_t>(sizeof(int32_t))) {
        VLOG(3) << "Failed to read log length currPos = " << currPos_;
        eof_ = true;
        break;
      }
    } while (false);
  }

  return *this;
}

bool WalFileIterator::valid() const {
  return !eof_ && currId_ <= lastId_;
}

LogID WalFileIterator::logId() const {
  return currId_;
}

TermID WalFileIterator::logTerm() const {
  return currTerm_;
}

ClusterID WalFileIterator::logSource() const {
  // Retrieve from the file
  DCHECK(!fds_.empty());
  ClusterID cluster = 0;
  CHECK_EQ(pread(fds_.front(),
                 &(cluster),
                 sizeof(ClusterID),
                 currPos_ + sizeof(LogID) + sizeof(TermID) + sizeof(int32_t)),
           static_cast<ssize_t>(sizeof(ClusterID)))
      << "Failed to read. Curr position is " << currPos_ << ", expected read length is "
      << sizeof(ClusterID) << " (errno: " << errno << "): " << strerror(errno);

  return cluster;
}

folly::StringPiece WalFileIterator::logMsg() const {
  // Retrieve from the file
  DCHECK(!fds_.empty());

  currLog_.resize(currMsgLen_);
  CHECK_EQ(pread(fds_.front(),
                 &(currLog_[0]),
                 currMsgLen_,
                 currPos_ + sizeof(LogID) + sizeof(TermID) + sizeof(int32_t) + sizeof(ClusterID)),
           static_cast<ssize_t>(currMsgLen_))
      << "Failed to read. Curr position is " << currPos_ << ", expected read length is "
      << currMsgLen_ << " (errno: " << errno << "): " << strerror(errno);

  return currLog_;
}

LogID WalFileIterator::getFirstIdInNextFile() const {
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
