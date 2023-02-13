/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_KVSTORE_H_
#define KVSTORE_KVSTORE_H_

#include <rocksdb/compaction_filter.h>
#include <rocksdb/merge_operator.h>

#include "common/base/Base.h"
#include "common/base/ErrorOr.h"
#include "common/base/Status.h"
#include "common/meta/SchemaManager.h"
#include "kvstore/Common.h"
#include "kvstore/CompactionFilter.h"
#include "kvstore/KVEngine.h"
#include "kvstore/KVIterator.h"
#include "kvstore/PartManager.h"
#include "kvstore/raftex/RaftPart.h"

namespace nebula {
namespace kvstore {

struct KVOptions {
  // HBase thrift server address.
  HostAddr hbaseServer_;

  // SchemaManager instance, help the hbasestore to encode/decode data.
  meta::SchemaManager* schemaMan_{nullptr};

  // Paths for data. It would be used by rocksdb engine.
  // Be careful! We should ensure each "paths" has only one instance,
  // otherwise it would mix up the data on disk.
  std::vector<std::string> dataPaths_;

  std::string walPath_;

  // Path for listener, only wal is stored, the structure would be
  // spaceId/partId/wal
  std::string listenerPath_;

  // PartManager instance for kvstore.
  std::unique_ptr<PartManager> partMan_{nullptr};

  // Custom MergeOperator used in rocksdb.merge method.
  std::shared_ptr<rocksdb::MergeOperator> mergeOp_{nullptr};

  // Custom CompactionFilter used in compaction.
  std::unique_ptr<CompactionFilterFactoryBuilder> cffBuilder_{nullptr};
};

struct StoreCapability {
  static const uint32_t SC_FILTERING = 1;
  static const uint32_t SC_ASYNC = 2;
};
#define SUPPORT_FILTERING(store) (store.capability() & StoreCapability::SC_FILTERING)

class Part;
/**
 * @brief Interfaces of kvstore
 */
class KVStore {
 public:
  virtual ~KVStore() = default;

  /**
   * @brief Return bit-OR of StoreCapability values;
   *
   * @return uint32_t Bitwise capability
   */
  virtual uint32_t capability() const = 0;

  /**
   * @brief Stop the kvstore
   */
  virtual void stop() = 0;

  /**
   * @brief Retrieve the current leader for the given partition. This is usually called when
   * E_LEADER_CHANGED error code is returned.
   *
   * @param spaceId
   * @param partID
   * @return ErrorOr<nebula::cpp2::ErrorCode, HostAddr> Return HostAddr when succeeded, return
   * ErrorCode when failed
   */
  virtual ErrorOr<nebula::cpp2::ErrorCode, HostAddr> partLeader(GraphSpaceID spaceId,
                                                                PartitionID partID) = 0;

  /**
   * @brief Return pointer of part manager
   *
   * @return PartManager*
   */
  virtual PartManager* partManager() const {
    return nullptr;
  }

  /**
   * @brief Get the Snapshot object
   *
   * @param spaceId Space id
   * @param partID Partition id
   * @param canReadFromFollower
   * @return const void* Snapshot.
   */
  virtual const void* GetSnapshot(GraphSpaceID spaceId,
                                  PartitionID partID,
                                  bool canReadFromFollower = false) = 0;

  /**
   * @brief Release snapshot.
   *
   * @param spaceId Space id.
   * @param partId Partition id.
   * @param snapshot Snapshot to release.
   */
  virtual void ReleaseSnapshot(GraphSpaceID spaceId, PartitionID partId, const void* snapshot) = 0;

  /**
   * @brief Read a single key
   *
   * @param spaceId
   * @param partId
   * @param key
   * @param value
   * @param canReadFromFollower
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode get(GraphSpaceID spaceId,
                                      PartitionID partId,
                                      const std::string& key,
                                      std::string* value,
                                      bool canReadFromFollower = false,
                                      const void* snapshot = nullptr) = 0;

  /**
   * @brief Read a list of keys
   *
   * @param spaceId
   * @param partId
   * @param keys Keys to read
   * @param values Pointers of value
   * @param canReadFromFollower
   * @return Return std::vector<Status> when succeeded: Result status of each key, if key[i] does
   * not exist, the i-th value in return value would be Status::KeyNotFound. Return ErrorCode when
   * failed
   */
  virtual std::pair<nebula::cpp2::ErrorCode, std::vector<Status>> multiGet(
      GraphSpaceID spaceId,
      PartitionID partId,
      const std::vector<std::string>& keys,
      std::vector<std::string>* values,
      bool canReadFromFollower = false) = 0;

  /**
   * @brief Get all results in range [start, end)
   *
   * @param spaceId
   * @param partId
   * @param start Start key, inclusive
   * @param end End key, exclusive
   * @param iter Iterator in range [start, end), returns by kv engine
   * @param canReadFromFollower
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode range(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        const std::string& start,
                                        const std::string& end,
                                        std::unique_ptr<KVIterator>* iter,
                                        bool canReadFromFollower = false) = 0;

  /**
   * @brief To forbid to pass rvalue via the 'range' parameter.
   */
  virtual nebula::cpp2::ErrorCode range(GraphSpaceID spaceId,
                                        PartitionID partId,
                                        std::string&& start,
                                        std::string&& end,
                                        std::unique_ptr<KVIterator>* iter,
                                        bool canReadFromFollower = false) = delete;

  /**
   * @brief Get all results with 'prefix' str as prefix.
   *
   * @param spaceId
   * @param partId
   * @param prefix Key of prefix to seek
   * @param iter Iterator of keys starts with 'prefix', returns by kv engine
   * @param canReadFromFollower
   * @param snapshot If set, read from snapshot.
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode prefix(GraphSpaceID spaceId,
                                         PartitionID partId,
                                         const std::string& prefix,
                                         std::unique_ptr<KVIterator>* iter,
                                         bool canReadFromFollower = false,
                                         const void* snapshot = nullptr) = 0;

  /**
   * @brief To forbid to pass rvalue via the 'prefix' parameter.
   */
  virtual nebula::cpp2::ErrorCode prefix(GraphSpaceID spaceId,
                                         PartitionID partId,
                                         std::string&& prefix,
                                         std::unique_ptr<KVIterator>* iter,
                                         bool canReadFromFollower = false,
                                         const void* snapshot = nullptr) = delete;

  /**
   * @brief Get all results with 'prefix' str as prefix starting form 'start'
   *
   * @param spaceId
   * @param partId
   * @param start Start key, inclusive
   * @param prefix The prefix of keys to iterate
   * @param iter Iterator of keys starts with 'prefix' beginning from 'start', returns by kv engine
   * @param canReadFromFollower
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode rangeWithPrefix(GraphSpaceID spaceId,
                                                  PartitionID partId,
                                                  const std::string& start,
                                                  const std::string& prefix,
                                                  std::unique_ptr<KVIterator>* iter,
                                                  bool canReadFromFollower = false) = 0;

  /**
   * @brief To forbid to pass rvalue via the 'rangeWithPrefix' parameter.
   */
  virtual nebula::cpp2::ErrorCode rangeWithPrefix(GraphSpaceID spaceId,
                                                  PartitionID partId,
                                                  std::string&& start,
                                                  std::string&& prefix,
                                                  std::unique_ptr<KVIterator>* iter,
                                                  bool canReadFromFollower = false) = delete;

  /**
   * @brief Synchronize the kvstore across multiple replica
   *
   * @param spaceId
   * @param partId
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode sync(GraphSpaceID spaceId, PartitionID partId) = 0;

  /**
   * @brief Write multiple key/values to kvstore asynchronously
   *
   * @param spaceId
   * @param partId
   * @param keyValues Key/values to put
   * @param cb Callback when has a result
   */
  virtual void asyncMultiPut(GraphSpaceID spaceId,
                             PartitionID partId,
                             std::vector<KV>&& keyValues,
                             KVCallback cb) = 0;

  /**
   * @brief Remove a key from kvstore asynchronously
   *
   * @param spaceId
   * @param partId
   * @param key Key to remove
   * @param cb Callback when has a result
   */
  virtual void asyncRemove(GraphSpaceID spaceId,
                           PartitionID partId,
                           const std::string& key,
                           KVCallback cb) = 0;

  /**
   * @brief Remove multiple keys from kvstore asynchronously
   *
   * @param spaceId
   * @param partId
   * @param key Keys to remove
   * @param cb Callback when has a result
   */
  virtual void asyncMultiRemove(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::vector<std::string>&& keys,
                                KVCallback cb) = 0;

  /**
   * @brief Remove keys in range [start, end) asynchronously
   *
   * @param spaceId
   * @param partId
   * @param start Start key
   * @param end End key
   * @param cb Callback when has a result
   */
  virtual void asyncRemoveRange(GraphSpaceID spaceId,
                                PartitionID partId,
                                const std::string& start,
                                const std::string& end,
                                KVCallback cb) = 0;

  /**
   * @brief Do some atomic operation on kvstore
   *
   * @param spaceId
   * @param partId
   * @param op Atomic operation
   * @param cb Callback when has a result
   */
  virtual void asyncAtomicOp(GraphSpaceID spaceId,
                             PartitionID partId,
                             MergeableAtomicOp op,
                             KVCallback cb) = 0;

  /**
   * @brief Brief async commit multi operation, difference between asyncMultiPut or asyncMultiRemove
   * is this func allow contains both put and remove together, difference between asyncAtomicOp is
   * asyncAtomicOp may have CAS
   *
   * @param spaceId
   * @param partId
   * @param batch Encoded write batch
   * @param cb Callback when has a result
   */
  virtual void asyncAppendBatch(GraphSpaceID spaceId,
                                PartitionID partId,
                                std::string&& batch,
                                KVCallback cb) = 0;

  /**
   * @brief Ingest the sst file under download directory
   *
   * @param spaceId
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode ingest(GraphSpaceID spaceId) = 0;

  /**
   * @brief Retrieve the leader distribution
   *
   * @param leaderIds The leader address of all partitions
   * @return int32_t The leader count of all spaces
   */
  virtual int32_t allLeader(
      std::unordered_map<GraphSpaceID, std::vector<meta::cpp2::LeaderInfo>>& leaderIds) = 0;

  /**
   * @brief Get the part object of given spaceId and partId
   *
   * @param spaceId
   * @param partId
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<Part>> Return the part if succeeded,
   * else return ErrorCode
   */
  virtual ErrorOr<nebula::cpp2::ErrorCode, std::shared_ptr<Part>> part(GraphSpaceID spaceId,
                                                                       PartitionID partId) = 0;

  /**
   * @brief Trigger comapction, only used in rocksdb
   *
   * @param spaceId
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode compact(GraphSpaceID spaceId) = 0;

  /**
   * @brief Trigger flush, only used in rocksdb
   *
   * @param spaceId
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode flush(GraphSpaceID spaceId) = 0;

  /**
   * @brief Create a Checkpoint, only used in rocksdb
   *
   * @param spaceId
   * @param name Checkpoint name
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::CheckpointInfo>> Return the
   * checkpoint info if succeed, else return ErrorCode
   */
  virtual ErrorOr<nebula::cpp2::ErrorCode, std::vector<cpp2::CheckpointInfo>> createCheckpoint(
      GraphSpaceID spaceId, const std::string& name) = 0;

  /**
   * @brief Drop a Checkpoint, only used in rocksdb
   *
   * @param spaceId
   * @param name Checkpoint name
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode dropCheckpoint(GraphSpaceID spaceId, const std::string& name) = 0;

  /**
   * @brief Set the write blocking flag
   *
   * @param spaceId
   * @param sign True to block. Falst to unblock
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode setWriteBlocking(GraphSpaceID spaceId, bool sign) = 0;

  /**
   * @brief Backup the data of a table prefix, for meta backup
   *
   * @param spaceId
   * @param name
   * @param tablePrefix Table prefix
   * @param filter Data filter when iterate the table
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> Return the sst file path if
   * succeed, else return ErrorCode
   */
  virtual ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> backupTable(
      GraphSpaceID spaceId,
      const std::string& name,
      const std::string& tablePrefix,
      std::function<bool(const folly::StringPiece& key)> filter) = 0;

  /**
   * @brief Restore from sst files
   *
   * @param spaceId
   * @param files SST file path
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode restoreFromFiles(GraphSpaceID spaceId,
                                                   const std::vector<std::string>& files) = 0;

  /**
   * @brief return a WriteBatch object to do batch operation
   *
   * @return std::unique_ptr<WriteBatch>
   */
  virtual std::unique_ptr<WriteBatch> startBatchWrite() = 0;

  /**
   * @brief Write batch data to local storage only
   *
   * @param spaceId
   * @param batch
   * @return nebula::cpp2::ErrorCode
   */
  virtual nebula::cpp2::ErrorCode batchWriteWithoutReplicator(
      GraphSpaceID spaceId, std::unique_ptr<WriteBatch> batch) = 0;

  /**
   * @brief Get the data paths
   *
   * @return std::vector<std::string> Data paths
   */
  virtual std::vector<std::string> getDataRoot() const = 0;

  /**
   * @brief Get the kvstore property, only used in rocksdb
   *
   * @param spaceId
   * @param property Property name
   * @return ErrorOr<nebula::cpp2::ErrorCode, std::string> Return the property value in string if
   * succeed, else return ErrorCode
   */
  virtual ErrorOr<nebula::cpp2::ErrorCode, std::string> getProperty(
      GraphSpaceID spaceId, const std::string& property) = 0;

 protected:
  KVStore() = default;
};

}  // namespace kvstore
}  // namespace nebula

#endif  // KVSTORE_KVSTORE_H_
