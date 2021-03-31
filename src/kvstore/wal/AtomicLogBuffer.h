/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef WAL_ATOMICLOGBUFFER_H_
#define WAL_ATOMICLOGBUFFER_H_

#include "common/thrift/ThriftTypes.h"
#include "utils/LogIterator.h"
#include <gtest/gtest_prod.h>
#include <folly/lang/Aligned.h>

namespace nebula {
namespace wal {

constexpr int32_t kMaxLength = 64;

struct Record {
    Record() = default;
    Record(const Record&) = default;
    Record(Record&& record) noexcept = default;
    Record& operator=(Record&& record) noexcept = default;

    Record(ClusterID clusterId, TermID termId, std::string msg)
        : clusterId_(clusterId)
        , termId_(termId)
        , msg_(std::move(msg)) {}

    int32_t size() const {
        return sizeof(ClusterID) + sizeof(TermID) + msg_.size();
    }

    ClusterID       clusterId_;
    TermID          termId_;
    std::string     msg_;
};

struct Node {
    Node() = default;

    bool isFull() {
        return pos_.load(std::memory_order_acquire) == kMaxLength;
    }

    bool push_back(Record&& rec) {
        if (isFull()) {
            return false;
        }
        size_ += rec.size();
        auto pos = pos_.load(std::memory_order_acquire);
        (*records_)[pos] = std::move(rec);
        pos_.fetch_add(1, std::memory_order_release);
        return true;
    }

    Record* rec(int32_t index) {
        CHECK_GE(index, 0);
        auto pos = pos_.load(std::memory_order_acquire);
        CHECK_LE(index, pos);
        CHECK(index != kMaxLength);
        return &(*records_)[index];
    }

    LogID lastLogId() const {
        return firstLogId_ + pos_.load(std::memory_order_relaxed);
    }

    LogID                             firstLogId_{0};
    // total size for current Node.
    int32_t                           size_{0};
    Node*                             next_{nullptr};

    /******* readers maybe access the fields below ******************/

    // We should ensure the records appended happens-before pos_ increment.
    folly::cacheline_aligned<std::array<Record, kMaxLength>>    records_;
    // current valid position for the next record.
    std::atomic<int32_t>              pos_{0};
    // The field only be accessed when the refs count down to zero
    std::atomic<bool>                 markDeleted_{false};
    // We should ensure the records appended happens-before the prev inited.
    std::atomic<Node*>                prev_{nullptr};
};

/**
 * A wait-free log buffer for single writer, multi readers
 * When deleting the extra node, to avoid read the dangling one,
 * we just mark it to be deleted, and delete it when no readers using it.
 *
 * For write, most of time, it is o(1)
 * For seek, it is o(n), n is the number of nodes inside current list, but in most
 * cases, the seeking log is in the head node, so it equals o(1)
 * */
class AtomicLogBuffer : public std::enable_shared_from_this<AtomicLogBuffer> {
    FRIEND_TEST(AtomicLogBufferTest, ResetThenPushExceedLimit);

public:
    /**
     * The iterator once created, it could just see the snapshot of current list.
     * In other words, the new records inserted during scanning are invisible.
     * */
    class Iterator : public LogIterator{
        friend class AtomicLogBuffer;
        FRIEND_TEST(AtomicLogBufferTest, SingleWriterMultiReadersTest);

    public:
        ~Iterator() {
            logBuffer_->releaseRef();
        }

        LogIterator& operator++() override {
            currIndex_++;
            currLogId_++;
            if (currLogId_ > end_) {
                valid_ = false;
                currRec_ = nullptr;
                return *this;
            }
            // Operations after load SHOULD NOT reorder before it.
            auto pos = currNode_->pos_.load(std::memory_order_acquire);
            VLOG(3) << "currNode firstLogId = " << currNode_->firstLogId_
                    << ", currIndex = " << currIndex_
                    << ", currNode pos " << pos;
            if (currIndex_ >= pos) {
                currNode_ = currNode_->prev_.load(std::memory_order_relaxed);
                if (currNode_ == nullptr) {
                    valid_ = false;
                    currRec_ = nullptr;
                    return *this;
                } else {
                    currIndex_ = 0;
                }
            }
            DCHECK_LT(currIndex_, kMaxLength);
            currRec_ = DCHECK_NOTNULL(currNode_)->rec(currIndex_);
            return *this;
        }

        bool valid() const override {
            return valid_;
        }

        LogID logId() const override {
            DCHECK(valid_);
            return currLogId_;
        }

        TermID logTerm() const override {
            return record()->termId_;
        }

        ClusterID logSource() const override {
            return record()->clusterId_;
        }

        folly::StringPiece logMsg() const override {
            return record()->msg_;
        }

    private:
        // Iterator could only be acquired by AtomicLogBuffer::iterator interface.
        Iterator(std::shared_ptr<AtomicLogBuffer> logBuffer, LogID start, LogID end)
            : logBuffer_(logBuffer)
            , currLogId_(start) {
            logBuffer_->addRef();
            end_ = std::min(end, logBuffer->lastLogId());
            seek(currLogId_);
        }

        const Record* record() const {
            if (!valid_) {
                return nullptr;
            }
            return DCHECK_NOTNULL(currRec_);
        }

        void seek(LogID logId) {
            currNode_ = logBuffer_->seek(logId);
            if (currNode_ != nullptr) {
                currIndex_ = logId - currNode_->firstLogId_;
                currRec_ = currNode_->rec(currIndex_);
                valid_ = true;
            } else {
                valid_ = false;
            }
        }

        Node* currNode() const {
            return currNode_;
        }

        int32_t currIndex() const {
            return currIndex_;
        }

    private:
        std::shared_ptr<AtomicLogBuffer> logBuffer_;
        LogID                            currLogId_{0};
        LogID                            end_{0};
        Node*                            currNode_{nullptr};
        int32_t                          currIndex_{0};
        bool                             valid_{true};
        Record*                          currRec_{nullptr};
    };

    static std::shared_ptr<AtomicLogBuffer> instance(int32_t capacity = 8 * 1024 * 1024) {
        return std::shared_ptr<AtomicLogBuffer>(new AtomicLogBuffer(capacity));
    }

    /**
     * Users should ensure there are no readers when releasing it.
     * */
    ~AtomicLogBuffer() {
        auto refs = refs_.load(std::memory_order_acquire);
        CHECK_EQ(0, refs);
        auto* curr = head_.load(std::memory_order_relaxed);
        auto* prev = curr;
        while (curr != nullptr) {
            curr = curr->next_;
            delete prev;
            prev = curr;
        }
        if (prev != nullptr) {
            delete prev;
        }
    }

    void push(LogID logId, TermID termId, ClusterID clusterId, std::string&& msg) {
        push(logId, Record(clusterId, termId, std::move(msg)));
    }

    void push(LogID logId, Record&& record) {
        auto* head = head_.load(std::memory_order_relaxed);
        auto recSize = record.size();
        if (head == nullptr
                || head->isFull()
                || head->markDeleted_.load(std::memory_order_relaxed)) {
            auto* newNode = new Node();
            newNode->firstLogId_ = logId;
            newNode->next_ = head;
            newNode->push_back(std::move(record));
            if (head == nullptr || head->markDeleted_.load(std::memory_order_relaxed)) {
                // It is the first Node in current list, or head has been marked as deleted
                firstLogId_.store(logId, std::memory_order_relaxed);
                tail_.store(newNode, std::memory_order_relaxed);
            } else if (head != nullptr) {
                head->prev_.store(newNode, std::memory_order_release);
            }
            size_.fetch_add(recSize, std::memory_order_relaxed);
            head_.store(newNode, std::memory_order_relaxed);
            return;
        }
        if (size_ + recSize > capacity_) {
            auto* tail = tail_.load(std::memory_order_relaxed);
            // todo(doodle): there is a potential problem is that: since Node::isFull is judeged by
            // log count, we can only add new node when previous node has enough logs. So when tail
            // is equal to head, we need to wait tail is full, after head moves forward, at then
            // tail can be marked as deleted. So the log buffer would takes up more memory than its
            // capacity. Since it does not affect correctness, we could fix it later if necessary.
            if (tail != head) {
                // We have more than one nodes in current list.
                // So we mark the tail to be deleted.
                bool expected = false;
                VLOG(3) << "Mark node " << tail->firstLogId_ << " to be deleted!";
                auto marked = tail->markDeleted_.compare_exchange_strong(
                    expected, true, std::memory_order_relaxed);
                auto* prev = tail->prev_.load(std::memory_order_relaxed);
                firstLogId_.store(prev->firstLogId_, std::memory_order_relaxed);
                // All operations above SHOULD NOT be reordered.
                tail_.store(tail->prev_, std::memory_order_release);
                if (marked) {
                    size_.fetch_sub(tail->size_, std::memory_order_relaxed);
                    // dirtyNodes_ changes SHOULD after the tail move.
                    dirtyNodes_.fetch_add(1, std::memory_order_release);
                }
            }
        }
        size_.fetch_add(recSize, std::memory_order_relaxed);
        head->push_back(std::move(record));
    }

    LogID firstLogId() const {
        return firstLogId_.load(std::memory_order_relaxed);
    }

    LogID lastLogId() const {
        auto* p = head_.load(std::memory_order_relaxed);
        if (p == nullptr) {
            return 0;
        }
        return p->lastLogId();
    }

    /**
     * For reset operation, users should keep it thread-safe with push operation.
     * Just mark all nodes to be deleted.
     *
     * Actually, we don't follow the invariant strictly (node in range [head, tail] are valid),
     * head and tail are not modified. But once an log is pushed after reset, everything will obey
     * the invariant.
     * */
    void reset() {
        auto* p = head_.load(std::memory_order_relaxed);
        int32_t count = 0;
        while (p != nullptr) {
            bool expected = false;
            if (!p->markDeleted_.compare_exchange_strong(
                    expected, true, std::memory_order_relaxed)) {
                // The rest nodes has been mark deleted.
                break;
            }
            p = p->next_;
            ++count;
        }
        size_.store(0, std::memory_order_relaxed);
        firstLogId_.store(0, std::memory_order_relaxed);
        dirtyNodes_.fetch_add(count, std::memory_order_release);
    }

    std::unique_ptr<Iterator> iterator(LogID start, LogID end) {
        std::unique_ptr<Iterator> iter(new Iterator(shared_from_this(), start, end));
        return iter;
    }

private:
    explicit AtomicLogBuffer(int32_t capacity)
        : capacity_(capacity) {}

    /*
     * Find the non-deleted node contains the logId.
     * */
    Node* seek(LogID logId) {
        auto* head = head_.load(std::memory_order_relaxed);
        if (head != nullptr && logId > head->lastLogId()) {
            VLOG(3) << "Bad seek, the seeking logId " << logId
                    << " is greater than the latest logId in buffer "
                    << head->lastLogId();
            return nullptr;
        }
        auto* p = head;
        if (p == nullptr) {
            return nullptr;
        }
        auto* tail = tail_.load(std::memory_order_relaxed);
        CHECK_NOTNULL(tail);
        // The scan range is [head, tail]
        // And we should ensure the nodes inside the range SHOULD NOT be deleted.
        // We could ensure the tail during gc is older than current one.
        while (p != tail->next_ && !p->markDeleted_) {
            VLOG(3) << "current node firstLogId = " << p->firstLogId_
                    << ", the seeking logId = " << logId;
            if (logId >= p->firstLogId_) {
                break;
            }
            p = p->next_;
        }
        if (p == nullptr) {
            return nullptr;
        }
        return p->markDeleted_ ? nullptr : p;
    }

    int32_t addRef() {
        return refs_.fetch_add(1, std::memory_order_relaxed);
    }

    void releaseRef() {
        // All operations following SHOULD NOT reordered before tail.load()
        // so we could ensure the tail used in GC is older than new coming readers.
        auto* tail = tail_.load(std::memory_order_acquire);
        auto readers = refs_.fetch_sub(1, std::memory_order_relaxed);
        VLOG(3) << "Release ref, readers = " << readers;
        // todo(doodle): https://github.com/vesoft-inc/nebula-storage/issues/390
        if (readers > 1) {
            return;
        }
        // In this position, maybe there are some new readers coming in
        // So we should load tail before refs count down to ensure the tail current thread
        // got is older than the new readers see.

        CHECK_EQ(1, readers);

        auto dirtyNodes = dirtyNodes_.load(std::memory_order_relaxed);
        bool gcRunning = false;

        if (dirtyNodes > dirtyNodesLimit_) {
            if (gcOnGoing_.compare_exchange_strong(gcRunning, true, std::memory_order_acquire)) {
                VLOG(1) << "GC begins!";
                // It means no readers on the deleted nodes.
                // Cut-off the list.
                CHECK_NOTNULL(tail);
                auto* dirtyHead = tail->next_;
                tail->next_ = nullptr;

                // Now we begin to delete the nodes.
                auto* curr = dirtyHead;
                while (curr != nullptr) {
                    CHECK(curr->markDeleted_.load(std::memory_order_relaxed));
                    VLOG(1) << "Delete node " << curr->firstLogId_;
                    auto* del = curr;
                    curr = curr->next_;
                    delete del;
                    dirtyNodes_.fetch_sub(1, std::memory_order_release);
                    CHECK_GE(dirtyNodes_, 0);
                }

                gcOnGoing_.store(false, std::memory_order_release);
                VLOG(1) << "GC finished!";
            } else {
                VLOG(1) << "Current list is in gc now!";
            }
        }
    }


private:
    std::atomic<Node*>           head_{nullptr};
    // The tail is the last valid Node in current list
    // After tail_, all nodes should be marked deleted.
    std::atomic<Node*>           tail_{nullptr};

    std::atomic_int              refs_{0};
    // current size for the buffer.
    std::atomic_int              size_{0};
    std::atomic<LogID>           firstLogId_{0};
    // The max size limit.
    int32_t                      capacity_{8 * 1024 * 1024};
    std::atomic<bool>            gcOnGoing_{false};
    std::atomic<int32_t>         dirtyNodes_{0};
    int32_t                      dirtyNodesLimit_{5};
};

}  // namespace wal
}  // namespace nebula

#endif  // WAL_ATOMICLOGBUFFER_H_
