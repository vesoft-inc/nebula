/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef RAFTEX_SNAPSHOTMANAGER_H_
#define RAFTEX_SNAPSHOTMANAGER_H_

#include <folly/Function.h>                        // for Function
#include <folly/executors/IOThreadPoolExecutor.h>  // for IOThreadPoolExecutor
#include <folly/futures/Future.h>                  // for Future
#include <stdint.h>                                // for int64_t

#include <memory>   // for shared_ptr, unique...
#include <string>   // for string
#include <utility>  // for pair
#include <vector>   // for vector

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/thrift/ThriftClientManager.h"  // for ThriftClientManager
#include "common/thrift/ThriftTypes.h"          // for TermID, GraphSpaceID
#include "interface/gen-cpp2/RaftexServiceAsyncClient.h"
#include "interface/gen-cpp2/raftex_types.h"

namespace nebula {
namespace raftex {
namespace cpp2 {
class RaftexServiceAsyncClient;
class SendSnapshotResponse;
}  // namespace cpp2
}  // namespace raftex
struct HostAddr;
template <typename T>
class StatusOr;

struct HostAddr;
template <typename T>
class StatusOr;

namespace raftex {
namespace cpp2 {
class RaftexServiceAsyncClient;
class SendSnapshotResponse;
}  // namespace cpp2

enum SnapshotStatus {
  IN_PROGRESS,
  DONE,
  FAILED,
};

// Return false if send snapshot failed, will not send the rest of it.
using SnapshotCallback = folly::Function<bool(const std::vector<std::string>& rows,
                                              int64_t totalCount,
                                              int64_t totalSize,
                                              SnapshotStatus status)>;
class RaftPart;

class SnapshotManager {
 public:
  SnapshotManager();
  virtual ~SnapshotManager() = default;

  /**
   * @brief Send snapshot for spaceId, partId to host dst.
   *
   * @param part The RaftPart
   * @param dst The address of target peer
   * @return folly::Future<StatusOr<std::pair<LogID, TermID>>> Future of snapshot result, return the
   * commit log id and commit log term if succeed
   */
  folly::Future<StatusOr<std::pair<LogID, TermID>>> sendSnapshot(std::shared_ptr<RaftPart> part,
                                                                 const HostAddr& dst);

 private:
  /**
   * @brief Send the snapshot in batch
   *
   * @param spaceId
   * @param partId
   * @param termId Current term of RaftPart
   * @param committedLogId The commit log id of snapshot
   * @param committedLogTerm The commit log term of snapshot
   * @param localhost Local address
   * @param data The key/value to send
   * @param totalSize The key/value has been sent in bytes
   * @param totalCount Count of key/value has been sent
   * @param addr Address of target peer
   * @param finished Whether this is the last batch of snapshot
   * @return folly::Future<raftex::cpp2::SendSnapshotResponse>
   */
  folly::Future<raftex::cpp2::SendSnapshotResponse> send(GraphSpaceID spaceId,
                                                         PartitionID partId,
                                                         TermID termId,
                                                         LogID committedLogId,
                                                         TermID committedLogTerm,
                                                         const HostAddr& localhost,
                                                         const std::vector<std::string>& data,
                                                         int64_t totalSize,
                                                         int64_t totalCount,
                                                         const HostAddr& addr,
                                                         bool finished);

  /**
   * @brief Interface to scan data, and trigger callback to send them
   *
   * @param spaceId
   * @param partId
   * @param cb Callback to send data
   */
  virtual void accessAllRowsInSnapshot(GraphSpaceID spaceId,
                                       PartitionID partId,
                                       SnapshotCallback cb) = 0;

 private:
  std::unique_ptr<folly::IOThreadPoolExecutor> executor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
  thrift::ThriftClientManager<raftex::cpp2::RaftexServiceAsyncClient> connManager_;
};

}  // namespace raftex
}  // namespace nebula

#endif  // RAFTEX_SNAPSHOTMANAGER_H_
