/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_KVENGINE_H_
#define KVSTORE_KVENGINE_H_

#include "common/base/Base.h"
#include "common/base/ErrorOr.h"
#include "common/base/Status.h"
#include "kvstore/Common.h"
#include "kvstore/KVIterator.h"

namespace nebula {
namespace kvstore {

/**
 * @brief wrapper of batch write
 */
class WriteBatch {
 public:
  virtual ~WriteBatch() = default;

  /**
   * @brief Encode the operation of put key/value into write batch
   *
   * @param key Key to put
   * @param value Value to put
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode put(folly::StringPiece key, folly::StringPiece value) = 0;

  /**
   * @brief Encode the operation of remove key into write batch
   *
   * @param key Key to remove
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode remove(folly::StringPiece key) = 0;

  /**
   * @brief Encode the operation of remove keys in range [start, end) into write batch
   *
   * @param start Start key to be removed, inclusive
   * @param end End key to be removed, exclusive
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode removeRange(folly::StringPiece start, folly::StringPiece end) = 0;
};

/**
 * @brief Key-value engine object
 */
class KVEngine {
 public:
  explicit KVEngine(GraphSpaceID spaceId) : spaceId_(spaceId) {}

  virtual ~KVEngine() = default;

  virtual void stop() = 0;

  /**
   * @brief Retrieve the data path of kv engine
   *
   * @return const char* Data path of kv engine
   */
  virtual const char* getDataRoot() const = 0;

  /**
   * @brief Retrieve the wal path of kv engine
   *
   * @return const char* Wal path of kv engine
   */
  virtual const char* getWalRoot() const = 0;

  /**
   * @brief return a WriteBatch object to do batch operation
   *
   * @return std::unique_ptr<WriteBatch>
   */
  virtual std::unique_ptr<WriteBatch> startBatchWrite() = 0;

  /**
   * @brief write the batch operation into kv engine
   *
   * @param batch WriteBatch object
   * @param disableWAL Whether wal is disabled, only used in rocksdb
   * @param sync Whether need to sync when write, only used in rocksdb
   * @param wait Whether wait until write result, rocksdb would return incompelete if wait is false
   * in certain scenario
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode commitBatchWrite(std::unique_ptr<WriteBatch> batch,
                                                   bool disableWAL,
                                                   bool sync,
                                                   bool wait) = 0;

  /**
   * @brief Get the Snapshot from kv engine.
   *
   * @return const void* snapshot pointer.
   */
  virtual const void* GetSnapshot() = 0;

  /**
   * @brief Release snapshot from kv engine.
   *
   * @param snapshot
   */
  virtual void ReleaseSnapshot(const void* snapshot) = 0;

  /**
   * @brief Read a single key
   *
   * @param key Key to read
   * @param value Pointer of value
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode get(const std::string& key,
                                      std::string* value,
                                      const void* snapshot = nullptr) = 0;

  /**
   * @brief Read a list of keys
   *
   * @param keys Keys to read
   * @param values Pointers of value
   * @return std::vector<Status> Result status of each key, if key[i] does not exist, the i-th value
   * in return value would be Status::KeyNotFound
   */
  virtual std::vector<Status> multiGet(const std::vector<std::string>& keys,
                                       std::vector<std::string>* values) = 0;

  /**
   * @brief Get all results in range [start, end)
   *
   * @param start Start key, inclusive
   * @param end End key, exclusive
   * @param iter Iterator in range [start, end), returns by kv engine
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode range(const std::string& start,
                                        const std::string& end,
                                        std::unique_ptr<KVIterator>* iter) = 0;

  /**
   * @brief Get all results with 'prefix' str as prefix.
   *
   * @param prefix The prefix of keys to iterate
   * @param iter Iterator of keys starts with 'prefix', returns by kv engine
   * @param snapshot Snapshot from kv engine. nullptr means no snapshot.
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode prefix(const std::string& prefix,
                                         std::unique_ptr<KVIterator>* iter,
                                         const void* snapshot = nullptr) = 0;

  /**
   * @brief Get all results with 'prefix' str as prefix starting form 'start'
   *
   * @param start Start key, inclusive
   * @param prefix The prefix of keys to iterate
   * @param iter Iterator of keys starts with 'prefix' beginning from 'start', returns by kv engine
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode rangeWithPrefix(const std::string& start,
                                                  const std::string& prefix,
                                                  std::unique_ptr<KVIterator>* iter) = 0;

  /**
   * @brief Scan all keys in kv engine
   *
   * @param storageIter Iterator returns by kv engine
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode scan(std::unique_ptr<KVIterator>* storageIter) = 0;

  /**
   * @brief Write a single record
   *
   * @param key Key to write
   * @param value Value to write
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode put(std::string key, std::string value) = 0;

  /**
   * @brief Write a batch of records
   *
   * @param keyValues Key-value pairs to write
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode multiPut(std::vector<KV> keyValues) = 0;

  /**
   * @brief Remove a single key
   *
   * @param key Key to remove
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode remove(const std::string& key) = 0;

  /**
   * @brief Remove a batch of keys
   *
   * @param keys Keys to remove
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode multiRemove(std::vector<std::string> keys) = 0;

  /**
   * @brief Remove key in range [start, end)
   *
   * @param start Start key, inclusive
   * @param end End key, exclusive
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode removeRange(const std::string& start, const std::string& end) = 0;

  /**
   * @brief Add partId into current storage engine.
   *
   * @param partId
   * @param raftPeers partition raft peers, including peers created during balance which are not in
   * meta
   */
  virtual void addPart(PartitionID partId, const Peers& raftPeers) = 0;

  /**
   * @brief Update part info. Could only update the peers' persist info now.
   *
   * @param partId
   * @param raftPeer 1. if raftPeer.status is kDeleted, delete this peer.
   *                 2. if raftPeer.status is others, add or update this peer
   */
  virtual void updatePart(PartitionID partId, const Peer& raftPeer) = 0;

  /**
   * @brief Remove a partition from kv engine
   *
   * @param partId Partition id to add
   */
  virtual void removePart(PartitionID partId) = 0;

  /**
   * @brief Return all parts current engine holds.
   *
   * @return std::vector<PartitionID> Partition ids
   */
  virtual std::vector<PartitionID> allParts() = 0;

  /**
   * @brief Return all partId->raft peers that current storage engine holds.
   *
   * @return std::map<PartitionID, Peers> partId-> raft peers for each part, including learners
   */
  virtual std::map<PartitionID, Peers> allPartPeers() = 0;

  /**
   * @brief Return total parts num
   *
   * @return int32_t Count of partition num
   */
  virtual int32_t totalPartsNum() = 0;

  /**
   * @brief Ingest external sst files, only used in rocksdb
   *
   * @param files SST file path
   * @param verifyFileChecksum  Whether verify sst check-sum during ingestion
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode ingest(const std::vector<std::string>& files,
                                         bool verifyFileChecksum = false) = 0;

  /**
   * @brief Set config option, only used in rocksdb
   *
   * @param configKey Config name
   * @param configValue Config value
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode setOption(const std::string& configKey,
                                            const std::string& configValue) = 0;

  /**
   * @brief Set DB config option, only used in rocksdb
   *
   * @param configKey Config name
   * @param configValue Config value
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode setDBOption(const std::string& configKey,
                                              const std::string& configValue) = 0;

  /**
   * @brief Get engine property, only used in rocksdb
   *
   * @param property Config name
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::string>
   */
  virtual ErrorOr<nebula::cpp2::ErrorCode, std::string> getProperty(
      const std::string& property) = 0;

  /**
   * @brief Do data compation in lsm tree
   *
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode compact() = 0;

  /**
   * @brief Flush data in memtable into sst
   *
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode flush() = 0;

  /**
   * @brief Create a rocksdb check point
   *
   * @param checkpointPath
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode createCheckpoint(const std::string& checkpointPath) = 0;

  // For meta
  /**
   * @brief Backup the data of a table prefix, for meta backup
   *
   * @param path KV engine path
   * @param tablePrefix Table prefix
   * @param filter Data filter when iterate the table
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> Return the sst file path if
   * succeed, else return ErrorCode
   */
  virtual ErrorOr<nebula::cpp2::ErrorCode, std::string> backupTable(
      const std::string& path,
      const std::string& tablePrefix,
      std::function<bool(const folly::StringPiece& key)> filter) = 0;

  /**
   * @brief Call rocksdb backup, mainly for rocksdb PlainTable mounted on tmpfs/ramfs
   *
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode backup() = 0;

 protected:
  GraphSpaceID spaceId_;
};

}  // namespace kvstore
}  // namespace nebula

#endif  // KVSTORE_KVENGINE_H_
