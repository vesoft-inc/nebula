/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Random.h>     // for Random
#include <folly/String.h>     // for stringPrintf
#include <folly/init/Init.h>  // for init
#include <glog/logging.h>     // for INFO
#include <gtest/gtest.h>      // for TestPartResult
#include <gtest/gtest.h>      // for Message
#include <gtest/gtest.h>      // for TestPartResult
#include <unistd.h>           // for usleep, size_t

#include <atomic>   // for atomic, memory_order_acquire
#include <cstdint>  // for int32_t
#include <memory>   // for shared_ptr, __shared_ptr_ac...
#include <ostream>  // for operator<<, basic_ostream::...
#include <string>   // for string, basic_string
#include <thread>   // for thread
#include <utility>  // for move
#include <vector>   // for vector

#include "common/base/Logging.h"          // for CHECK, COMPACT_GOOGLE_LOG_F...
#include "common/thrift/ThriftTypes.h"    // for LogID, ClusterID, TermID
#include "kvstore/wal/AtomicLogBuffer.h"  // for AtomicLogBuffer, AtomicLogB...

namespace nebula {
namespace wal {

void checkIterator(std::shared_ptr<AtomicLogBuffer> logBuffer,
                   LogID from,
                   LogID to,
                   LogID expected) {
  auto iter = logBuffer->iterator(from, to);
  for (; iter->valid(); ++(*iter)) {
    auto log = iter->logMsg();
    ASSERT_EQ(folly::stringPrintf("str_%ld", from), log);
    from++;
  }
  EXPECT_EQ(expected, from);
}

TEST(AtomicLogBufferTest, ReadWriteTest) {
  auto logBuffer = AtomicLogBuffer::instance();
  for (LogID logId = 0; logId < 1000L; logId++) {
    logBuffer->push(logId, Record(0, 0, folly::stringPrintf("str_%ld", logId)));
  }
  checkIterator(logBuffer, 200, 1000, 1000);
  checkIterator(logBuffer, 200, 1500, 1000);
  checkIterator(logBuffer, 200, 800, 801);
  {
    LogID from = 1200;
    auto iter = logBuffer->iterator(from, 1800);
    CHECK(!iter->valid());
  }
}

TEST(AtomicLogBufferTest, OverflowTest) {
  auto logBuffer = AtomicLogBuffer::instance(128);
  for (LogID logId = 0; logId < 1000L; logId++) {
    logBuffer->push(logId, Record(0, 0, folly::stringPrintf("str_%ld", logId)));
  }
  {
    LogID from = 100;
    auto iter = logBuffer->iterator(from, 1800);
    CHECK(!iter->valid());
  }
}

TEST(AtomicLogBufferTest, SingleWriterMultiReadersTest) {
  // The default size is 100K
  auto logBuffer = AtomicLogBuffer::instance(100 * 1024);
  std::atomic<LogID> writePoint{0};
  std::thread writer([logBuffer, &writePoint] {
    LOG(INFO) << "Begin write 1M records";
    for (LogID logId = 0; logId < 1000000L; logId++) {
      logBuffer->push(logId, Record(0, 0, folly::stringPrintf("str_%ld", logId)));
      writePoint.store(logId, std::memory_order_release);
    }
    LOG(INFO) << "Finish writer";
  });

  std::vector<std::thread> readers;
  for (int i = 0; i < 5; i++) {
    readers.emplace_back([i, logBuffer, &writePoint] {
      usleep(10);
      LOG(INFO) << "Start reader " << i;
      int times = 10000;
      int validSeek = 0;
      while (times-- > 0) {
        auto wp = writePoint.load(std::memory_order_acquire) - 1;
        auto start = folly::Random::rand32(logBuffer->firstLogId(), wp);
        auto end = start + folly::Random::rand32(1000);
        auto iter = logBuffer->iterator(start, end);
        if (!iter->valid()) {
          continue;
        }
        validSeek++;
        size_t num = start;
        for (; iter->valid(); ++(*iter)) {
          const auto* rec = iter->record();
          auto logId = iter->logId();
          auto* node = iter->currNode();
          CHECK_NOTNULL(rec);
          auto expected = folly::stringPrintf("str_%ld", num);
          EXPECT_EQ(num, logId);
          EXPECT_EQ(expected.size(), rec->msg_.size())
              << "wp " << wp << ", start " << start << ", logId " << logId << ", end " << end
              << ", curr node " << node->firstLogId_ << ", pos " << node->pos_ << ", curr index "
              << iter->currIndex() << ", head lastLodId " << logBuffer->lastLogId();
          EXPECT_EQ(expected, rec->msg_)
              << "expected size " << expected.size() << ", actual size " << rec->msg_.size();
          num++;
        }
      }
      LOG(INFO) << "End reader " << i << ", valid seek times " << validSeek;
    });
  }

  writer.join();
  for (auto& r : readers) {
    r.join();
  }
}

TEST(AtomicLogBufferTest, ResetThenPushExceedLimit) {
  int32_t capacity = 24 * (kMaxLength + 1);
  auto logBuffer = AtomicLogBuffer::instance(capacity);
  // One node would save at most kMaxLength logs, so there will be 2 node.
  LogID logId = 0;
  for (; logId <= kMaxLength; logId++) {
    // The actual size of one record would be sizeof(ClusterID) + sizeof(TermID)
    // + msg_.size(), in this case, it would be 8 + 8 + 8 = 24, total size would
    // be 24 * (kMaxLength + 1)
    logBuffer->push(logId, Record(0, 0, std::string(8, 'a')));
  }

  // Mark all two node as deleted, log buffer size would be reset to 0
  logBuffer->reset();
  CHECK(logBuffer->seek(0) == nullptr);
  CHECK(logBuffer->seek(kMaxLength) == nullptr);

  // Because head has been marked as deleted, this would save in a new node.
  // The record size will be exactly same with capacity of log buffer.
  std::string logMakeBufferFull(capacity - sizeof(ClusterID) - sizeof(TermID), 'a');
  logBuffer->push(logId, Record(0, 0, std::move(logMakeBufferFull)));

  // Before this PR, the logId will be 0 when buffer has been reset, and push a
  // new log
  CHECK_EQ(logId, logBuffer->firstLogId_);
  CHECK_EQ(capacity, logBuffer->size_);
  CHECK(logBuffer->seek(logId) != nullptr);

  logId++;

  // At this point, buffer will have three node, head contain the
  // logMakeBufferFull, others are marked as deleted, tail != head. Let's push
  // another log
  logBuffer->push(logId, Record(0, 0, std::string(8, 'a')));
  CHECK(logBuffer->seek(logId) != nullptr);
}

}  // namespace wal
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);

  return RUN_ALL_TESTS();
}
