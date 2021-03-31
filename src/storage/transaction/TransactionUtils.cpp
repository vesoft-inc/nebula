/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/transaction/TransactionUtils.h"
#include "common/time/WallClock.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

std::string TransactionUtils::dumpKey(const cpp2::EdgeKey& key) {
    return folly::sformat("dumpKey(): src={}, dst={}",
            (*key.src_ref()).toString(), (*key.dst_ref()).toString());
}

std::string TransactionUtils::edgeKey(size_t vIdLen,
                                      PartitionID partId,
                                      const cpp2::EdgeKey& key) {
    return NebulaKeyUtils::edgeKey(
        vIdLen,
        partId,
        (*key.src_ref()).getStr(),
        *key.edge_type_ref(),
        *key.ranking_ref(),
        (*key.dst_ref()).getStr());
}

std::string TransactionUtils::reverseRawKey(size_t vIdLen,
                                            PartitionID partId,
                                            const std::string& rawKey) {
    return NebulaKeyUtils::edgeKey(vIdLen,
                                   partId,
                                   NebulaKeyUtils::getDstId(vIdLen, rawKey).str(),
                                   0 - NebulaKeyUtils::getEdgeType(vIdLen, rawKey),
                                   NebulaKeyUtils::getRank(vIdLen, rawKey),
                                   NebulaKeyUtils::getSrcId(vIdLen, rawKey).str());
}

int64_t TransactionUtils::getSnowFlakeUUID() {
    auto ver = std::numeric_limits<int64_t>::max() - time::WallClock::slowNowInMicroSec();
    // Switch ver to big-endian, make sure the key is in ordered.
    return folly::Endian::big(ver);
}

std::string TransactionUtils::hexEdgeId(size_t vIdLen, folly::StringPiece key) {
    return folly::hexlify(NebulaKeyUtils::getSrcId(vIdLen, key)) +
           folly::hexlify(NebulaKeyUtils::getDstId(vIdLen, key));
}

}  // namespace storage
}  // namespace nebula
