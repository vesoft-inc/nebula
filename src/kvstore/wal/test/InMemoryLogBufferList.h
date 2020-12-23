/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "utils/LogIterator.h"
#include "kvstore/wal/test/InMemoryLogBuffer.h"

namespace nebula {
namespace wal {

class InMemoryBufferList : public std::enable_shared_from_this<InMemoryBufferList> {
public:
    class Iterator : public LogIterator {
        friend class InMemoryBufferList;
    public:
        Iterator(std::shared_ptr<InMemoryBufferList> imBufferList, LogID start, LogID end)
            : imBufferList_(imBufferList)
            , currId_(start)
            , end_(std::min(end, imBufferList->latestLogId_.load())) {
            if (currId_ > end_
                    || imBufferList_->buffers_.empty()
                    || currId_ < imBufferList_->buffers_.front()->firstLogId()) {
                return;
            }
            imBufferList_->accessAllBuffers([this] (BufferPtr buffer) {
                if (buffer->empty()) {
                    // Skip th empty one.
                    return true;
                }
                if (end_ >= buffer->firstLogId()) {
                    buffers_.push_front(buffer);
                    firstIdInBuffer_ = buffer->firstLogId();
                }
                if (firstIdInBuffer_ <= currId_) {
                    // Go no futher
                    currIdx_ = currId_ - firstIdInBuffer_;
                    nextFirstId_ = getFirstIdInNextBuffer();
                    return false;
                } else {
                    return true;
                }
            });
            if (!buffers_.empty()) {
                valid_ = true;
            }
        }

        bool valid() const override {
            return valid_;
        }

        LogIterator& operator++() override {
            ++currId_;
            if (currId_ > end_) {
                valid_ = false;
                return *this;
            }
            if (currId_ >= nextFirstId_) {
                // Roll over to next buffer
                if (buffers_.size() == 1) {
                    LOG(FATAL) << "currId_ " << currId_
                               << ", nextFirstId_ " << nextFirstId_
                               << ", firstIdInBuffer_ " << firstIdInBuffer_
                               << ", lastId_ " << end_
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
            return *this;
        }

        LogID logId() const override {
            CHECK(valid_);
            return currId_;
        }

        TermID logTerm() const override {
            CHECK(valid_);
            return buffers_.front()->getTerm(currIdx_);
        }

        ClusterID logSource() const override {
            CHECK(valid_);
            return buffers_.front()->getCluster(currIdx_);
        }

        folly::StringPiece logMsg() const override {
            CHECK(valid_);
            return buffers_.front()->getLog(currIdx_);
        }

    private:
        LogID getFirstIdInNextBuffer() const {
            auto it = buffers_.begin();
            ++it;
            if (it == buffers_.end()) {
                return buffers_.front()->lastLogId() + 1;
            } else {
                return (*it)->firstLogId();
            }
        }

    private:
        std::shared_ptr<InMemoryBufferList> imBufferList_;
        LogID                               currId_{0};
        LogID                               end_{0};
        bool                                valid_{false};
        BufferList                          buffers_;
        size_t                              currIdx_{0};
        LogID                               firstIdInBuffer_{std::numeric_limits<LogID>::max()};
        LogID                               nextFirstId_{0};
    };

    static std::shared_ptr<InMemoryBufferList> instance() {
        return std::shared_ptr<InMemoryBufferList>(new InMemoryBufferList());
    }

    void push(LogID logId, TermID termId, ClusterID clusterId, std::string&& msg) {
        auto buffer = getLastBuffer(logId, msg.size());
        buffer->push(termId, clusterId, std::move(msg));
        latestLogId_ = logId;
    }

    std::unique_ptr<Iterator> iterator(LogID start, LogID end) {
        std::unique_ptr<Iterator> iter(new Iterator(shared_from_this(), start, end));
        return iter;
    }

private:
    InMemoryBufferList() = default;

    BufferPtr getLastBuffer(LogID id, size_t expectedToWrite) {
        std::unique_lock<std::mutex> g(buffersMutex_);
        if (!buffers_.empty()) {
            if (buffers_.back()->size() + expectedToWrite <= maxBufferSize_) {
                return buffers_.back();
            }
            // Need to rollover to a new buffer
            if (buffers_.size() == numBuffers_) {
                // Need to pop the first one
                buffers_.pop_front();
            }
            CHECK_LT(buffers_.size(), numBuffers_);
        }
        buffers_.emplace_back(std::make_shared<InMemoryLogBuffer>(id, ""));
        return buffers_.back();
    }

    size_t accessAllBuffers(std::function<bool(BufferPtr buffer)> fn) const {
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

private:
    BufferList  buffers_;
    mutable std::mutex  buffersMutex_;
    size_t      maxBufferSize_{4 * 1024 * 1024};
    size_t      numBuffers_{2};
    std::atomic<LogID>      latestLogId_{0};
};

}   // namespace wal
}   // namespace nebula

