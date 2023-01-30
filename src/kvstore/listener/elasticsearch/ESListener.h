/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_LISTENER_ES_LISTENER_H_
#define KVSTORE_LISTENER_ES_LISTENER_H_

#include "codec/RowReaderWrapper.h"
#include "common/plugin/fulltext/elasticsearch/ESAdapter.h"
#include "kvstore/listener/Listener.h"

namespace nebula {
namespace kvstore {

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
   * @param schemaMan Schema manager
   */
  ESListener(GraphSpaceID spaceId,
             PartitionID partId,
             HostAddr localAddr,
             const std::string& walPath,
             std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
             std::shared_ptr<thread::GenericThreadPool> workers,
             std::shared_ptr<folly::Executor> handlers,
             meta::SchemaManager* schemaMan)
      : Listener(spaceId, partId, std::move(localAddr), walPath, ioPool, workers, handlers),
        schemaMan_(schemaMan) {
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
  bool apply(const BatchHolder& batch);

  /**
   * @brief Persist commitLogId commitLogTerm and lastApplyLogId
   */
  bool persist(LogID lastId, TermID lastTerm, LogID lastApplyLogId) override;

  /**
   * @brief Get commit log id and commit log term from persistence storage, called in start()
   *
   * @return std::pair<LogID, TermID>
   */
  std::pair<LogID, TermID> lastCommittedLogId() override;

  /**
   * @brief Get last apply id from persistence storage, used in initialization
   *
   * @return LogID Last apply log id
   */
  LogID lastApplyLogId() override;

  void processLogs() override;

  std::tuple<nebula::cpp2::ErrorCode, int64_t, int64_t> commitSnapshot(
      const std::vector<std::string>& data,
      LogID committedLogId,
      TermID committedLogTerm,
      bool finished) override;

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
  std::string encodeAppliedId(LogID lastId, TermID lastTerm, LogID lastApplyLogId) const;

 private:
  meta::SchemaManager* schemaMan_{nullptr};
  using PickFunc = std::function<void(BatchLogType type,
                                      const std::string& index,
                                      const std::string& vid,
                                      const std::string& src,
                                      const std::string& dst,
                                      int64_t rank,
                                      const std::string& text)>;
  void pickTagAndEdgeData(BatchLogType type,
                          const std::string& key,
                          const std::string& value,
                          const PickFunc& func);

  std::string truncateVid(const std::string& vid);

  StatusOr<::nebula::plugin::ESAdapter> getESAdapter();

  std::unique_ptr<std::string> lastApplyLogFile_{nullptr};
  std::unique_ptr<std::string> spaceName_{nullptr};
  int32_t vIdLen_;
  bool isIntVid_{false};
};

}  // namespace kvstore
}  // namespace nebula
#endif
