/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef RAFTEX_SNAPSHOTMANAGER_H_
#define RAFTEX_SNAPSHOTMANAGER_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "gen-cpp2/raftex_types.h"
#include "gen-cpp2/RaftexServiceAsyncClient.h"
#include "thrift/ThriftClientManager.h"
#include <folly/futures/Future.h>
#include <folly/Function.h>
#include <folly/executors/IOThreadPoolExecutor.h>

namespace nebula {
namespace raftex {


enum SnapshotStatus {
    IN_PROGRESS,
    DONE,
    FAILED,
};

using SnapshotCallback = folly::Function<bool(const std::vector<std::string>& rows,
                                              int64_t totalCount,
                                              int64_t totalSize,
                                              SnapshotStatus status)>;
class RaftPart;

class SnapshotManager {
public:
    SnapshotManager();
    virtual ~SnapshotManager() = default;

    // Send snapshot for spaceId, partId to host dst.
    folly::Future<Status> sendSnapshot(std::shared_ptr<RaftPart> part,
                                       const HostAddr& dst,
                                       std::pair<LogID, TermID> commitLogIdAndTerm);

private:
    folly::Future<raftex::cpp2::SendSnapshotResponse> send(
                                                   GraphSpaceID spaceId,
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

