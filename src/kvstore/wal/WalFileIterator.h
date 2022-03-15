/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef WAL_WALFILEITERATOR_H_
#define WAL_WALFILEITERATOR_H_

#include "common/base/Base.h"
#include "common/utils/LogIterator.h"

namespace nebula {
namespace wal {

class FileBasedWal;

/**
 * @brief The log iterator used in AtomicLogBuffer, all logs are in wal file.
 */
class WalFileIterator final : public LogIterator {
 public:
  /**
   * @brief Construct a new wal iterator in range [start, end]
   *
   * @param wal Related wal file
   * @param start Start log id, inclusive
   * @param end End log id, inclusive
   */
  WalFileIterator(std::shared_ptr<FileBasedWal> wal, LogID startId, LogID lastId = -1);

  /**
   * @brief Destroy the wal file iterator
   */
  virtual ~WalFileIterator();

  /**
   * @brief Move forward iterator to next wal record
   *
   * @return LogIterator&
   */
  LogIterator& operator++() override;

  /**
   * @brief Return whether log iterator is valid
   */
  bool valid() const override;

  /**
   * @brief Return the log id pointed by current iterator
   */
  LogID logId() const override;

  /**
   * @brief Return the log term pointed by current iterator
   */
  TermID logTerm() const override;

  /**
   * @brief Return the log source pointed by current iterator
   */
  ClusterID logSource() const override;

  /**
   * @brief Return the log message pointed by current iterator
   */
  folly::StringPiece logMsg() const override;

 private:
  /**
   * @brief Return the first log id in next wal file
   */
  LogID getFirstIdInNextFile() const;

 private:
  // Holds the Wal object, so that it will not be destroyed before the iterator
  std::shared_ptr<FileBasedWal> wal_;

  LogID lastId_;
  LogID currId_;
  TermID currTerm_;

  // When there are more wals, nextFirstId_ is the firstLogId in next wal.
  // When there are not more wals, nextFirstId_ is the current wal's lastLogId + 1
  LogID nextFirstId_;

  // [firstId, lastId]
  std::list<std::pair<LogID, LogID>> idRanges_;
  std::list<int> fds_;
  int64_t currPos_{0};
  int32_t currMsgLen_{0};
  // Whether we have encounter end of wal file during building iterator or iterating
  bool eof_{false};
  mutable std::string currLog_;
};

}  // namespace wal
}  // namespace nebula

#endif  // WAL_WALFILEITERATOR_H_
