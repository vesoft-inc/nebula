/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef RAFTEX_WALFILEINFO_H_
#define RAFTEX_WALFILEINFO_H_

#include "base/Base.h"
#include "base/Cord.h"
#include "raftex/Wal.h"
#include "raftex/InMemoryLogBuffer.h"

namespace vesoft {
namespace raftex {

class WalFileInfo final {
public:
    WalFileInfo(std::string path, LogID firstId)
        : fullpath_(std::move(path))
        , firstLogId_(firstId)
        , lastLogId_(-1)
        , lastLogTerm_(-1)
        , mtime_(0)
        , size_(0) {}

    const char* path() const {
        return fullpath_.c_str();;
    }

    LogID firstId() const {
        return firstLogId_;
    }

    LogID lastId() const {
        return lastLogId_;
    }
    void setLastId(LogID id) {
        lastLogId_ = id;
    }

    TermID lastTerm() const {
        return lastLogTerm_;
    }
    void setLastTerm(TermID term) {
        lastLogTerm_ = term;
    }

    time_t mtime() const {
        return mtime_;
    }
    void setMTime(time_t time) {
        mtime_ = time;
    }

    size_t size() const {
        return size_;
    }
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

}  // namespace raftex
}  // namespace vesoft

#endif  // RAFTEX_WALFILEINFO_H_

