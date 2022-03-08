/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_TRANSACTION_CONSISTUTIL_H
#define STORAGE_TRANSACTION_CONSISTUTIL_H

#include "common/time/WallClock.h"
#include "common/utils/MemoryLockWrapper.h"
#include "common/utils/NebulaKeyUtils.h"
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/KVStore.h"
#include "storage/CommonUtils.h"
#include "storage/transaction/ConsistTypes.h"

namespace nebula {
namespace storage {
class ConsistUtil final {
 public:
  static std::string primeTable(PartitionID partId);

  static std::string doublePrimeTable(PartitionID partId);

  static std::string edgeKey(size_t vIdLen, PartitionID partId, const cpp2::EdgeKey& key);

  static std::string primeKey(size_t vIdLen, PartitionID partId, const cpp2::EdgeKey& edgeKey);

  static std::string doublePrime(size_t vIdLen, PartitionID partId, const cpp2::EdgeKey& edgeKey);

  static std::string primePrefix(PartitionID partId);

  static std::string doublePrimePrefix(PartitionID partId);

  static std::string primeKey(size_t vIdLen,
                              PartitionID partId,
                              const VertexID& srcId,
                              EdgeType type,
                              EdgeRanking rank,
                              const VertexID& dstId);

  static folly::StringPiece edgeKeyFromPrime(const folly::StringPiece& key);

  static folly::StringPiece edgeKeyFromDoublePrime(const folly::StringPiece& key);

  static std::string doublePrime(size_t vIdLen,
                                 PartitionID partId,
                                 const VertexID& srcId,
                                 EdgeType type,
                                 EdgeRanking rank,
                                 const VertexID& dstId);

  static RequestType parseType(folly::StringPiece val);

  static cpp2::UpdateEdgeRequest parseUpdateRequest(folly::StringPiece val);

  static cpp2::AddEdgesRequest parseAddRequest(folly::StringPiece val);

  static std::string strUUID();

  static cpp2::AddEdgesRequest toAddEdgesRequest(const cpp2::ChainAddEdgesRequest& req);

  static cpp2::EdgeKey reverseEdgeKey(const cpp2::EdgeKey& edgeKey);

  static void reverseEdgeKeyInplace(cpp2::EdgeKey& edgeKey);

  static std::string insertIdentifier() noexcept {
    return "a";
  }

  static std::string updateIdentifier() noexcept {
    return "u";
  }

  static std::string deleteIdentifier() noexcept {
    return "d";
  }

  /**
   * @brief if the vid of space is created as "Fixed string"
   * when trying to print this vid, it will show a hex string
   * This function trying to transform it to human readable format.
   * @return -1 if failed
   */
  static int64_t toInt(const ::nebula::Value& val);

  static std::string readableKey(size_t vidLen, bool isIntId, const std::string& rawKey);

  static std::vector<std::string> toStrKeys(const cpp2::DeleteEdgesRequest& req, int vidLen);

  static ::nebula::cpp2::ErrorCode getErrorCode(const cpp2::ExecResponse& resp);
};

struct DeleteEdgesRequestHelper final {
  static cpp2::DeleteEdgesRequest toDeleteEdgesRequest(const cpp2::ChainDeleteEdgesRequest& req);

  static cpp2::ChainDeleteEdgesRequest toChainDeleteEdgesRequest(
      const cpp2::DeleteEdgesRequest& req);

  static cpp2::DeleteEdgesRequest parseDeleteEdgesRequest(const std::string& val);

  static std::string explain(const cpp2::DeleteEdgesRequest& req, bool isIntVid);
};

}  // namespace storage
}  // namespace nebula
#endif
