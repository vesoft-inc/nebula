/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/raftex/SnapshotManager.h"

#include <folly/Try.h>                                          // for Try::...
#include <folly/executors/thread_factory/NamedThreadFactory.h>  // for Named...
#include <folly/futures/Future.h>                               // for Futur...
#include <folly/futures/Promise.h>                              // for Promi...
#include <folly/futures/Promise.h>                              // for Promise
#include <folly/futures/Promise.h>                              // for Promi...
#include <folly/futures/Promise.h>                              // for Promise
#include <folly/io/async/EventBase.h>                           // for Event...
#include <gflags/gflags.h>                                      // for DEFIN...
#include <thrift/lib/cpp/util/EnumUtils.h>                      // for enumN...
#include <thrift/lib/cpp2/FieldRef.h>                           // for field...
#include <unistd.h>                                             // for sleep

#include <exception>      // for excep...
#include <ostream>        // for opera...
#include <type_traits>    // for remov...
#include <unordered_map>  // for opera...

#include "common/base/Logging.h"                          // for COMPA...
#include "common/base/Status.h"                           // for Status
#include "common/base/StatusOr.h"                         // for StatusOr
#include "common/datatypes/HostAddr.h"                    // for opera...
#include "common/thrift/ThriftClientManager.h"            // for Thrif...
#include "interface/gen-cpp2/RaftexServiceAsyncClient.h"  // for Rafte...
#include "interface/gen-cpp2/common_types.h"              // for Error...
#include "interface/gen-cpp2/raftex_types.h"              // for SendS...
#include "kvstore/raftex/RaftPart.h"                      // for RaftPart

DEFINE_int32(snapshot_worker_threads, 4, "Threads number for snapshot");
DEFINE_int32(snapshot_io_threads, 4, "Threads number for snapshot");
DEFINE_int32(snapshot_send_retry_times, 3, "Retry times if send failed");
DEFINE_int32(snapshot_send_timeout_ms, 60000, "Rpc timeout for sending snapshot");

namespace nebula {
namespace raftex {

SnapshotManager::SnapshotManager() {
  executor_.reset(new folly::IOThreadPoolExecutor(
      FLAGS_snapshot_worker_threads,
      std::make_shared<folly::NamedThreadFactory>("snapshot-worker")));
  ioThreadPool_.reset(new folly::IOThreadPoolExecutor(
      FLAGS_snapshot_io_threads,
      std::make_shared<folly::NamedThreadFactory>("snapshot-ioexecutor")));
}

folly::Future<StatusOr<std::pair<LogID, TermID>>> SnapshotManager::sendSnapshot(
    std::shared_ptr<RaftPart> part, const HostAddr& dst) {
  folly::Promise<StatusOr<std::pair<LogID, TermID>>> p;
  auto fut = p.getFuture();
  executor_->add([this, p = std::move(p), part, dst]() mutable {
    auto spaceId = part->spaceId_;
    auto partId = part->partId_;
    auto termId = part->term_;
    // TODO(heng):  maybe the committedLogId is less than the real one in the
    // snapshot. It will not loss the data, but maybe some record will be
    // committed twice.
    auto commitLogIdAndTerm = part->lastCommittedLogId();
    const auto& localhost = part->address();
    std::vector<folly::Future<raftex::cpp2::SendSnapshotResponse>> results;
    VLOG(1) << part->idStr_ << "Begin to send the snapshot to the host " << dst
            << ", commitLogId = " << commitLogIdAndTerm.first
            << ", commitLogTerm = " << commitLogIdAndTerm.second;
    accessAllRowsInSnapshot(
        spaceId,
        partId,
        [&, this, p = std::move(p)](const std::vector<std::string>& data,
                                    int64_t totalCount,
                                    int64_t totalSize,
                                    SnapshotStatus status) mutable -> bool {
          if (status == SnapshotStatus::FAILED) {
            VLOG(1) << part->idStr_ << "Snapshot send failed, the leader changed?";
            p.setValue(Status::Error("Send snapshot failed!"));
            return false;
          }
          int retry = FLAGS_snapshot_send_retry_times;
          while (retry-- > 0) {
            auto f = send(spaceId,
                          partId,
                          termId,
                          commitLogIdAndTerm.first,
                          commitLogIdAndTerm.second,
                          localhost,
                          data,
                          totalSize,
                          totalCount,
                          dst,
                          status == SnapshotStatus::DONE);
            // TODO(heng): we send request one by one to avoid too large memory
            // occupied.
            try {
              auto resp = std::move(f).get();
              if (resp.get_error_code() == nebula::cpp2::ErrorCode::SUCCEEDED) {
                VLOG(3) << part->idStr_ << "has sended count " << totalCount;
                if (status == SnapshotStatus::DONE) {
                  VLOG(1) << part->idStr_ << "Finished, totalCount " << totalCount << ", totalSize "
                          << totalSize;
                  p.setValue(commitLogIdAndTerm);
                }
                return true;
              } else {
                VLOG(2) << part->idStr_ << "Sending snapshot failed, we don't retry anymore! "
                        << "The error code is "
                        << apache::thrift::util::enumNameSafe(resp.get_error_code());
                p.setValue(Status::Error("Send snapshot failed!"));
                return false;
              }
            } catch (const std::exception& e) {
              VLOG(3) << part->idStr_ << "Send snapshot failed, exception " << e.what()
                      << ", retry " << retry << " times";
              sleep(1);
              continue;
            }
          }
          VLOG(2) << part->idStr_ << "Send snapshot failed!";
          p.setValue(Status::Error("Send snapshot failed!"));
          return false;
        });
  });
  return fut;
}

folly::Future<raftex::cpp2::SendSnapshotResponse> SnapshotManager::send(
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
    bool finished) {
  VLOG(4) << "Send snapshot request to " << addr;
  raftex::cpp2::SendSnapshotRequest req;
  req.space_ref() = spaceId;
  req.part_ref() = partId;
  req.term_ref() = termId;
  req.committed_log_id_ref() = committedLogId;
  req.committed_log_term_ref() = committedLogTerm;
  req.leader_addr_ref() = localhost.host;
  req.leader_port_ref() = localhost.port;
  req.rows_ref() = data;
  req.total_size_ref() = totalSize;
  req.total_count_ref() = totalCount;
  req.done_ref() = finished;
  auto* evb = ioThreadPool_->getEventBase();
  return folly::via(evb, [this, addr, evb, req = std::move(req)]() mutable {
    auto client = connManager_.client(addr, evb, false, FLAGS_snapshot_send_timeout_ms);
    return client->future_sendSnapshot(req);
  });
}

}  // namespace raftex
}  // namespace nebula
