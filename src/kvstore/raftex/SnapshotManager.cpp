/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/raftex/SnapshotManager.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "kvstore/raftex/RaftPart.h"

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
      connManager_.reset(
        new thrift::ThriftClientManager<raftex::cpp2::RaftexServiceAsyncClient>(FLAGS_enable_ssl));
}

folly::Future<StatusOr<std::pair<LogID, TermID>>> SnapshotManager::sendSnapshot(
    std::shared_ptr<RaftPart> part, const HostAddr& dst) {
  folly::Promise<StatusOr<std::pair<LogID, TermID>>> p;
  // if use getFuture(), the future's executor is InlineExecutor, and if the promise setValue first,
  // the future's callback will be called directly in thenValue in the same thread, the Host::lock_
  // would be locked twice in one thread, this will cause deadlock
  auto fut = p.getSemiFuture().via(executor_.get());
  executor_->add([this, p = std::move(p), part, dst]() mutable {
    auto spaceId = part->spaceId_;
    auto partId = part->partId_;
    auto tr = part->getTermAndRole();
    if (tr.second != RaftPart::Role::LEADER) {
      VLOG(1) << part->idStr_ << "leader changed, term " << tr.first << ", do not send snapshot to "
              << dst;
      return;
    }
    auto termId = tr.first;
    const auto& localhost = part->address();
    accessAllRowsInSnapshot(
        spaceId,
        partId,
        [&, this, p = std::move(p)](LogID commitLogId,
                                    TermID commitLogTerm,
                                    const std::vector<std::string>& data,
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
                          commitLogId,
                          commitLogTerm,
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
                  p.setValue(std::make_pair(commitLogId, commitLogTerm));
                }
                return true;
              } else {
                VLOG(2) << part->idStr_ << "Sending snapshot failed, the error code is "
                        << apache::thrift::util::enumNameSafe(resp.get_error_code());
                sleep(1);
                continue;
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
  req.current_term_ref() = termId;
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
    auto client = connManager_->client(addr, evb, false, FLAGS_snapshot_send_timeout_ms);
    return client->future_sendSnapshot(req);
  });
}

}  // namespace raftex
}  // namespace nebula
