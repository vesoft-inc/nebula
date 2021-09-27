/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

#include "common/time/WallClock.h"
#include "common/utils/MemoryLockWrapper.h"
#include "common/utils/NebulaKeyUtils.h"
#include "interface/gen-cpp2/storage_types.h"
#include "kvstore/KVStore.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

enum class RequestType {
  UNKNOWN,
  INSERT,
  UPDATE,
};

enum class ResumeType {
  UNKNOWN = 0,
  RESUME_CHAIN,
  RESUME_REMOTE,
};

struct ResumeOptions {
  ResumeOptions(ResumeType tp, std::string val) : resumeType(tp), primeValue(std::move(val)) {}
  ResumeType resumeType;
  std::string primeValue;
};

class ConsistUtil final {
 public:
  static std::string primeTable();

  static std::string doublePrimeTable();

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

  static std::string tempRequestTable();

  static std::vector<int64_t> getMultiEdgeVers(kvstore::KVStore* store,
                                               GraphSpaceID spaceId,
                                               PartitionID partId,
                                               const std::vector<std::string>& keys);

  // return -1 if edge version not exist
  static int64_t getSingleEdgeVer(kvstore::KVStore* store,
                                  GraphSpaceID spaceId,
                                  PartitionID partId,
                                  const std::string& key);

  static int64_t getTimestamp(const std::string& val) noexcept;

  static cpp2::AddEdgesRequest makeDirectAddReq(const cpp2::ChainAddEdgesRequest& req);

  static cpp2::EdgeKey reverseEdgeKey(const cpp2::EdgeKey& edgeKey);

  static void reverseEdgeKeyInplace(cpp2::EdgeKey& edgeKey);

  static std::string insertIdentifier() noexcept { return "a"; }

  static std::string updateIdentifier() noexcept { return "u"; }

  static std::pair<int64_t, nebula::cpp2::ErrorCode> versionOfUpdateReq(
      StorageEnv* env, const cpp2::UpdateEdgeRequest& req);

  static std::string dumpAddEdgeReq(const cpp2::AddEdgesRequest& req);

  using Parts = std::unordered_map<PartitionID, std::vector<cpp2::NewEdge>>;
  static std::string dumpParts(const Parts& parts);
};

}  // namespace storage
}  // namespace nebula
