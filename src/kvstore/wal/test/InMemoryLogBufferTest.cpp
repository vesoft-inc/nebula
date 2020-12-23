/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include <utility>
#include <gtest/gtest.h>
#include "kvstore/wal/test/InMemoryLogBuffer.h"
#include "kvstore/wal/test/InMemoryLogBufferList.h"

namespace nebula {
namespace wal {

/// Check empty status
TEST(InMemoryLogBuffer, Empty) {
    constexpr LogID i = 0;
    constexpr std::pair<LogID, TermID> p(-1, -1);
    InMemoryLogBuffer b(i);
    EXPECT_EQ(p, b.accessAllLogs([](
        LogID id,
        TermID j,
        ClusterID k,
        const std::string& s){
        // Nothing
        UNUSED(id);
        UNUSED(j);
        UNUSED(k);
        UNUSED(s);
    }));
    EXPECT_EQ(b.size(), 0UL);
    EXPECT_TRUE(b.empty());
    EXPECT_EQ(i, b.firstLogId());
    EXPECT_EQ(i - 1, b.lastLogId());
    EXPECT_EQ(0, b.numLogs());
    EXPECT_EQ(0, b.size());
}

// Check simple append operation
TEST(InMemoryLogBuffer, Simple) {
    constexpr LogID i = 0;
    constexpr TermID t = 0;
    constexpr ClusterID c = 0;
    constexpr std::size_t inc = 10;
    constexpr auto log = "Log";
    constexpr std::pair<LogID, TermID> p(inc-1, inc-1);
    InMemoryLogBuffer b(i);
    for (std::size_t j = 0; j < inc; j++) {
        b.push(t + j, c + j, log);
    }
    EXPECT_EQ(p, b.accessAllLogs([](
        LogID id,
        TermID j,
        ClusterID k,
        const std::string& s){
        // Nothing
        UNUSED(id);
        UNUSED(j);
        UNUSED(k);
        UNUSED(s);
    }));
    EXPECT_FALSE(b.empty());
    EXPECT_EQ(i, b.firstLogId());
    EXPECT_EQ(inc - 3, b.getCluster(inc -3));
    EXPECT_EQ(log,  b.getLog(inc - 2));
    EXPECT_EQ(inc - 4,  b.getTerm(inc - 4));
    EXPECT_EQ(inc - 1, b.lastLogId());
    EXPECT_EQ(inc - 1, b.lastLogTerm());
    EXPECT_EQ(inc, b.numLogs());
    EXPECT_EQ(inc * (std::strlen(log) + sizeof(LogID) + sizeof(TermID) + sizeof(ClusterID)),
        b.size());
}

void checkIterator(std::shared_ptr<InMemoryBufferList> buffers,
                   LogID from,
                   LogID to,
                   LogID expectedEnd) {
    auto iter = buffers->iterator(from, to);
    for (; iter->valid(); ++(*iter)) {
        auto log = iter->logMsg();
        ASSERT_EQ(folly::stringPrintf("str_%ld", from), log);
        from++;
    }
    EXPECT_EQ(from, expectedEnd);
}

TEST(InMemoryLogBufferList, RWTest) {
    auto buffers = InMemoryBufferList::instance();
    for (size_t i = 0; i < 1000; i++) {
        buffers->push(i, 0, 0, folly::stringPrintf("str_%ld", i));
    }
    checkIterator(buffers, 200, 1000, 1000);
    checkIterator(buffers, 200, 1200, 1000);
    checkIterator(buffers, 200, 800, 801);
    {
        LogID from = 1300;
        auto iter = buffers->iterator(from, 1600);
        EXPECT_FALSE(iter->valid());
    }
}

}  // namespace wal
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
