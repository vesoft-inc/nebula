/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSACTION_TRANSACTIONUTILS_H_
#define STORAGE_TRANSACTION_TRANSACTIONUTILS_H_

#include <folly/FBVector.h>
#include <folly/container/Enumerate.h>
#include "common/interface/gen-cpp2/storage_types.h"
#include "kvstore/Common.h"

namespace nebula {
namespace storage {

class TransactionUtils {
public:
    static std::string dumpKey(const cpp2::EdgeKey& key);

    // join hex of src & dst as en edge id
    static std::string hexEdgeId(size_t vIdLen, folly::StringPiece key);

    static std::string reverseRawKey(size_t vIdLen, PartitionID partId, const std::string& rawKey);

    static int64_t getSnowFlakeUUID();

    /**
     * @brief a simple wrapper for NebulaKeyUtils::edgeKey
     *        allow cpp2::EdgeKey& as a parameter
     */
    static std::string edgeKey(size_t vIdLen,
                               PartitionID partId,
                               const cpp2::EdgeKey& key);
};

/**
 * @brief a simple time recoder used for perf optimize
 */
class Timer {
public:
    Timer() : start_(std::chrono::high_resolution_clock::now()) {}

    void reset() {
        start_ = std::chrono::high_resolution_clock::now();
    }

    int64_t elapsed_ms() const {
        auto tpNow = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(tpNow - start_).count();
    }

    int64_t elapsed_micro() const {
        auto tpNow = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(tpNow - start_).count();
    }

private:
    std::chrono::time_point<std::chrono::high_resolution_clock> start_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TRANSACTION_TRANSACTIONUTILS_H_
