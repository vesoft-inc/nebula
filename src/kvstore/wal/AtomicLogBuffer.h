/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef WAL_ATOMICLOGBUFFER_H_
#define WAL_ATOMICLOGBUFFER_H_

#include <folly/lang/Aligned.h>
#include <gtest/gtest_prod.h>

#include "common/thrift/ThriftTypes.h"
#include "common/utils/LogIterator.h"

namespace nebula {
namespace wal {

constexpr int32_t kMaxLength = 64;

/**
 * @brief Wal record in each Node, it is wrapper calls of wal log
 */
struct Record {
  Record() = default;
  Record(const Record&) = default;
  Record(Record&& record) noexcept = default;
  Record& operator=(Record&& record) noexcept = default;

  Record(ClusterID clusterId, TermID termId, folly::StringPiece msg)
      : clusterId_(clusterId), termId_(termId), msg_(msg.toString()) {}

  int32_t size() const {
    return sizeof(ClusterID) + sizeof(TermID) + msg_.size();
  }

  ClusterID clusterId_;
  TermID termId_;
  std::string msg_;
};

/**
 * @brief A node contains fix count of wal
 */
struct Node {
  Node() = default;

  /**
   * @brief Return current node is full or not
   */
  bool isFull() {
    return pos_.load(std::memory_order_acquire) == kMaxLength;
  }

  /**
   * @brief Add a record to current node
   *
   * @param rec Record to add
   * @return Whether operation succeed or not
   */
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

  /**
   * @brief Fetch a record by index
   *
   * @param index Wal index in node
   * @return Record* Wal record if exists
   */
  Record* rec(int32_t index) {
    if (UNLIKELY(index >= kMaxLength)) {
      return nullptr;
    }
    CHECK_GE(index, 0);
    auto pos = pos_.load(std::memory_order_acquire);
    CHECK_LE(index, pos);
    return &(*records_)[index];
  }

  /**
   * @brief The last wal log id by counting how many wal in current node
   *
   * @return LogID The last wal log id
   */
  LogID lastLogId() const {
    return firstLogId_ + pos_.load(std::memory_order_relaxed);
  }

  LogID firstLogId_{0};
  // total size for current Node.
  int32_t size_{0};
  Node* next_{nullptr};

  /******* readers maybe access the fields below ******************/

  // We should ensure the records appended happens-before pos_ increment.
  folly::cacheline_aligned<std::array<Record, kMaxLength>> records_;
  // current valid position for the next record.
  std::atomic<int32_t> pos_{0};
  // The field only be accessed when the refs count down to zero
  std::atomic<bool> markDeleted_{false};
  // We should ensure the records appended happens-before the prev inited.
  std::atomic<Node*> prev_{nullptr};
};

/**
 * @brief A wait-free log buffer for single writer, multi readers When deleting the extra node, to
 * avoid read the dangling one, we just mark it to be deleted, and delete it when no readers using
 * it. For write, most of time, it is o(1) For seek, it is o(n), n is the number of nodes inside
 * current list, but in most cases, the seeking log is in the head node, so it equals o(1)
 */
class AtomicLogBuffer : public std::enable_shared_from_this<AtomicLogBuffer> {
  FRIEND_TEST(AtomicLogBufferTest, ResetThenPushExceedLimit);

 public:
  /**
   * @brief The log iterator used in AtomicLogBuffer, all logs are in memory. Once the iterator is
   * created, it could just see the snapshot of current list. In other words, the new records
   * inserted during scanning are invisible.
   */
  class Iterator : public LogIterator {
    friend class AtomicLogBuffer;
    FRIEND_TEST(AtomicLogBufferTest, SingleWriterMultiReadersTest);

   public:
    /**
     * @brief Destroy the iterator, which would trigger gc if necessary
     */
    ~Iterator() {
      logBuffer_->releaseRef();
    }

    /**
     * @brief Move forward iterator to next wal record
     *
     * @return LogIterator&
     */
    LogIterator& operator++() override;

    /**
     * @brief Return whether log iterator is valid
     */
    bool valid() const override {
      return valid_;
    }

    /**
     * @brief Return the log id pointed by current iterator
     */
    LogID logId() const override {
      DCHECK(valid_);
      return currLogId_;
    }

    /**
     * @brief Return the log term pointed by current iterator
     */
    TermID logTerm() const override {
      return record()->termId_;
    }

    /**
     * @brief Return the log source pointed by current iterator
     */
    ClusterID logSource() const override {
      return record()->clusterId_;
    }

    /**
     * @brief Return the log message pointed by current iterator
     */
    folly::StringPiece logMsg() const override {
      return record()->msg_;
    }

   private:
    /**
     * @brief Construct a new wal iterator in range [start, end]
     *
     * @param logBuffer Related log buffer
     * @param start Start log id, inclusive
     * @param end End log id, inclusive
     */
    Iterator(std::shared_ptr<AtomicLogBuffer> logBuffer, LogID start, LogID end)
        : logBuffer_(logBuffer), currLogId_(start) {
      logBuffer_->addRef();
      end_ = std::min(end, logBuffer->lastLogId());
      seek(currLogId_);
    }

    /**
     * @brief Return the wal record if valid
     *
     * @return const Record* The wal record
     */
    const Record* record() const {
      if (!valid_) {
        return nullptr;
      }
      return DCHECK_NOTNULL(currRec_);
    }

    /**
     * @brief Seek the wal by log id, and initialize the iterator if valid
     *
     * @param logId The wal log id to seek
     */
    void seek(LogID logId);

    /**
     * @brief Return the current node of pointed by iterator
     *
     * @return Node*
     */
    Node* currNode() const {
      return currNode_;
    }

    /**
     * @brief Current index in wal node
     *
     * @return int32_t
     */
    int32_t currIndex() const {
      return currIndex_;
    }

   private:
    std::shared_ptr<AtomicLogBuffer> logBuffer_;
    LogID currLogId_{0};
    LogID end_{0};
    Node* currNode_{nullptr};
    int32_t currIndex_{0};
    bool valid_{true};
    Record* currRec_{nullptr};
  };

  /**
   * @brief Build the AtomicLogBuffer instance
   *
   * @param capacity Max capacity in bytes, when size exceeds capacity, which would trigger garbage
   * collection
   * @return std::shared_ptr<AtomicLogBuffer>
   */
  static std::shared_ptr<AtomicLogBuffer> instance(int32_t capacity = 8 * 1024 * 1024) {
    return std::shared_ptr<AtomicLogBuffer>(new AtomicLogBuffer(capacity));
  }

  /**
   * @brief Destroy the atomic log buffer, users should ensure there are no readers when
   * releasing it.
   */
  ~AtomicLogBuffer();

  /**
   * @brief Add a wal log to current buffer
   *
   * @param logId Log id
   * @param termId Log term
   * @param clusterId Cluster id of log
   * @param msg Log message
   */
  void push(LogID logId, TermID termId, ClusterID clusterId, folly::StringPiece msg) {
    push(logId, Record(clusterId, termId, msg));
  }

  /**
   * @brief Add the wal record into current buffer
   *
   * @param logId Log id
   * @param record Log record
   */
  void push(LogID logId, Record&& record);

  /**
   * @brief Return the first log id in buffer
   */
  LogID firstLogId() const {
    return firstLogId_.load(std::memory_order_relaxed);
  }

  /**
   * @brief Return the last log id in buffer
   */
  LogID lastLogId() const {
    auto* p = head_.load(std::memory_order_relaxed);
    if (p == nullptr) {
      return 0;
    }
    return p->lastLogId();
  }

  /**
   * @brief For reset operation, users should keep it thread-safe with push operation. Just mark all
   * nodes to be deleted. Actually, we don't follow the invariant strictly (node in range [head,
   * tail] are valid), head and tail are not modified. But once an log is pushed after reset,
   * everything will obey the invariant.
   */
  void reset();

  /**
   * @brief Return the log iterator in range [start, end]
   *
   * @param start Start log id, inclusive
   * @param end End log id, inclusive
   * @return std::unique_ptr<Iterator> Log iterator
   */
  std::unique_ptr<Iterator> iterator(LogID start, LogID end) {
    std::unique_ptr<Iterator> iter(new Iterator(shared_from_this(), start, end));
    return iter;
  }

 private:
  /**
   * @brief Construct a new Atomic Log Buffer object
   *
   * @param capacity Max capacity in bytes, when size exceeds capacity, which would trigger garbage
   * collection
   */
  explicit AtomicLogBuffer(int32_t capacity) : capacity_(capacity) {}

  /**
   * @brief Find the node which contains the log with given id
   *
   * @param logId Log it to seek
   * @return Node* Return the node contains the log, return nullptr if not found
   */
  Node* seek(LogID logId);

  /**
   * @brief Add a reference count of how many iterator exists
   *
   * @return int32_t Reference count
   */
  int32_t addRef() {
    return refs_.fetch_add(1, std::memory_order_relaxed);
  }

  /**
   * @brief Release the node if there are two many dirty nodes
   *
   */
  void releaseRef();

 private:
  std::atomic<Node*> head_{nullptr};
  // The tail is the last valid Node in current list
  // After tail_, all nodes should be marked deleted.
  std::atomic<Node*> tail_{nullptr};

  std::atomic_int refs_{0};
  // current size for the buffer.
  std::atomic_int size_{0};
  std::atomic<LogID> firstLogId_{0};
  // The max size limit.
  int32_t capacity_{8 * 1024 * 1024};
  std::atomic<bool> gcOnGoing_{false};
  std::atomic<int32_t> dirtyNodes_{0};
  int32_t dirtyNodesLimit_{5};
};

}  // namespace wal
}  // namespace nebula

#endif  // WAL_ATOMICLOGBUFFER_H_
