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

class WalFileIterator final : public LogIterator {
 public:
  // The range is [startId, lastId]
  // if the lastId < 0, the wal_->lastId_ will be used
  WalFileIterator(std::shared_ptr<FileBasedWal> wal, LogID startId, LogID lastId = -1);

  virtual ~WalFileIterator();

  LogIterator& operator++() override;

  bool valid() const override;

  LogID logId() const override;

  TermID logTerm() const override;

  ClusterID logSource() const override;

  folly::StringPiece logMsg() const override;

 private:
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
