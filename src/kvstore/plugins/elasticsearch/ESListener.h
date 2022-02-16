/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_PLUGINS_ES_LISTENER_H_
#define KVSTORE_PLUGINS_ES_LISTENER_H_

#include <folly/String.h>  // for stringPrintf
#include <stdint.h>        // for int32_t

#include <memory>   // for shared_ptr, make_unique
#include <string>   // for string, basic_string
#include <utility>  // for pair, move
#include <vector>   // for vector

#include "codec/RowReaderWrapper.h"
#include "common/base/Logging.h"        // for CHECK, COMPACT_GOOGLE_LO...
#include "common/datatypes/HostAddr.h"  // for HostAddr
#include "common/plugin/fulltext/FTStorageAdapter.h"
#include "common/plugin/fulltext/FTUtils.h"  // for DocItem (ptr only), Http...
#include "common/thrift/ThriftTypes.h"       // for LogID, TermID, GraphSpaceID
#include "kvstore/Common.h"                  // for KV
#include "kvstore/Listener.h"                // for Listener, RaftClient

namespace nebula {
class RowReader;
namespace kvstore {
class DiskManager;
}  // namespace kvstore
namespace meta {
class SchemaManager;
namespace cpp2 {
class FTIndex;
}  // namespace cpp2
}  // namespace meta
namespace raftex {
class SnapshotManager;
}  // namespace raftex
namespace thread {
class GenericThreadPool;
}  // namespace thread
}  // namespace nebula

namespace folly {
class Executor;
class IOThreadPoolExecutor;

class Executor;
class IOThreadPoolExecutor;
}  // namespace folly

namespace nebula {
class RowReader;
namespace meta {
class SchemaManager;
namespace cpp2 {
class FTIndex;
}  // namespace cpp2
}  // namespace meta
namespace raftex {
class SnapshotManager;
}  // namespace raftex
namespace thread {
class GenericThreadPool;
}  // namespace thread

namespace kvstore {
class DiskManager;

using nebula::plugin::DocItem;

class ESListener : public Listener {
 public:
  /**
   * @brief Construct a new ES Listener, it is a derived class of Listener
   *
   * @param spaceId
   * @param partId
   * @param localAddr Listener ip/addr
   * @param walPath Listener's wal path
   * @param ioPool IOThreadPool for listener
   * @param workers Background thread for listener
   * @param handlers Worker thread for listener
   * @param snapshotMan Snapshot manager
   * @param clientMan Client manager
   * @param diskMan Disk manager
   * @param schemaMan Schema manager
   */
  ESListener(GraphSpaceID spaceId,
             PartitionID partId,
             HostAddr localAddr,
             const std::string& walPath,
             std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
             std::shared_ptr<thread::GenericThreadPool> workers,
             std::shared_ptr<folly::Executor> handlers,
             std::shared_ptr<raftex::SnapshotManager> snapshotMan,
             std::shared_ptr<RaftClient> clientMan,
             std::shared_ptr<DiskManager> diskMan,
             meta::SchemaManager* schemaMan)
      : Listener(spaceId,
                 partId,
                 std::move(localAddr),
                 walPath,
                 ioPool,
                 workers,
                 handlers,
                 snapshotMan,
                 clientMan,
                 diskMan,
                 schemaMan) {
    CHECK(!!schemaMan);
    lastApplyLogFile_ = std::make_unique<std::string>(
        folly::stringPrintf("%s/last_apply_log_%d", walPath.c_str(), partId));
  }

 protected:
  /**
   * @brief Init work: get vid length, get es client
   */
  void init() override;

  /**
   * @brief Send data by es client
   *
   * @param data Key/value to apply
   * @return True if succeed. False if failed.
   */
  bool apply(const std::vector<KV>& data) override;

  /**
   * @brief Persist commitLogId commitLogTerm and lastApplyLogId
   */
  bool persist(LogID lastId, TermID lastTerm, LogID lastApplyLogId) override;

  /**
   * @brief Get commit log id and commit log term from persistance storage, called in start()
   *
   * @return std::pair<LogID, TermID>
   */
  std::pair<LogID, TermID> lastCommittedLogId() override;

  /**
   * @brief Get last apply id from persistance storage, used in initialization
   *
   * @return LogID Last apply log id
   */
  LogID lastApplyLogId() override;

 private:
  /**
   * @brief Write last commit id, last commit term, last apply id to a file
   *
   * @param lastId Last commit id
   * @param lastTerm Last commit term
   * @param lastApplyLogId Last apply id
   * @return Whether persist succeed
   */
  bool writeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId);

  /**
   * @brief Encode last commit id, last commit term, last apply id to a file
   *
   * @param lastId Last commit id
   * @param lastTerm Last commit term
   * @param lastApplyLogId Last apply id
   * @return Encoded string
   */
  std::string encodeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId) const noexcept;

  /**
   * @brief Convert key value to DocItem
   *
   * @param items DocItems to send
   * @param kv Key/value to encode into DocItems
   * @return Whether append DocItem succeed
   */
  bool appendDocItem(std::vector<DocItem>& items, const KV& kv) const;

  /**
   * @brief Convert edge key value to DocItem
   *
   * @param items DocItems to send
   * @param kv Edge key/value to encode into DocItems
   * @return Whether append DocItem succeed
   */
  bool appendEdgeDocItem(std::vector<DocItem>& items, const KV& kv) const;

  /**
   * @brief Convert tag key value to DocItem
   *
   * @param items DocItems to send
   * @param kv Edge key/value to encode into DocItems
   * @return Whether append DocItem succeed
   */
  bool appendTagDocItem(std::vector<DocItem>& items, const KV& kv) const;

  /**
   * @brief Add the fulltext index field to DocItem
   *
   * @param items DocItems to send
   * @param reader Key/value's reader
   * @param fti Fulltext index schema
   * @return Whether append DocItem succeed
   */
  bool appendDocs(std::vector<DocItem>& items,
                  RowReader* reader,
                  const std::pair<std::string, nebula::meta::cpp2::FTIndex>& fti) const;

  /**
   * @brief Bulk DocItem to es
   *
   * @param items DocItems to send
   * @return Whether send succeed
   */
  bool writeData(const std::vector<nebula::plugin::DocItem>& items) const;

  /**
   * @brief Put DocItem to es
   *
   * @param items DocItems to send
   * @return Whether send succeed
   */
  bool writeDatum(const std::vector<nebula::plugin::DocItem>& items) const;

 private:
  std::unique_ptr<std::string> lastApplyLogFile_{nullptr};
  std::unique_ptr<std::string> spaceName_{nullptr};
  std::vector<nebula::plugin::HttpClient> esClients_;
  int32_t vIdLen_;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PLUGINS_ES_LISTENER_H_
