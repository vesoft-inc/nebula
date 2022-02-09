/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef WAL_WALFILEINFO_H_
#define WAL_WALFILEINFO_H_

#include "common/base/Base.h"
#include "common/base/Cord.h"
#include "kvstore/wal/Wal.h"

namespace nebula {
namespace wal {

/**
 * @brief File info of wal file
 */
class WalFileInfo final {
 public:
  /**
   * @brief Construct a new wal file info
   *
   * @param path Wal path
   * @param firstId First log in wal file
   */
  WalFileInfo(std::string path, LogID firstId)
      : fullpath_(std::move(path)),
        firstLogId_(firstId),
        lastLogId_(-1),
        lastLogTerm_(-1),
        mtime_(0),
        size_(0) {}

  /**
   * @brief The wal file full path
   *
   * @return const char*
   */
  const char* path() const {
    return fullpath_.c_str();
  }

  /**
   * @brief Return first log id of wal file
   */
  LogID firstId() const {
    return firstLogId_;
  }

  /**
   * @brief Return last log id of wal file
   */
  LogID lastId() const {
    return lastLogId_;
  }

  /**
   * @brief Set the last log id
   *
   * @param id
   */
  void setLastId(LogID id) {
    lastLogId_ = id;
  }

  /**
   * @brief Return last log term of wal file
   *
   * @return TermID
   */
  TermID lastTerm() const {
    return lastLogTerm_;
  }

  /**
   * @brief Set the last log term
   *
   * @param term
   */
  void setLastTerm(TermID term) {
    lastLogTerm_ = term;
  }

  /**
   * @brief Get the last modify time of wal file
   *
   * @return time_t
   */
  time_t mtime() const {
    return mtime_;
  }

  /**
   * @brief Set the last modify time of wal file
   *
   * @param time UTC time
   */
  void setMTime(time_t time) {
    mtime_ = time;
  }

  /**
   * @brief Get the wal file size
   *
   * @return size_t File size in bytes
   */
  size_t size() const {
    return size_;
  }

  /**
   * @brief Set the wal file size
   *
   * @param size File size in bytes
   */
  void setSize(size_t size) {
    size_ = size;
  }

 private:
  const std::string fullpath_;
  const LogID firstLogId_;
  LogID lastLogId_;
  TermID lastLogTerm_;
  time_t mtime_;
  size_t size_;
};

using WalFileInfoPtr = std::shared_ptr<WalFileInfo>;

}  // namespace wal
}  // namespace nebula

#endif  // WAL_WALFILEINFO_H_
