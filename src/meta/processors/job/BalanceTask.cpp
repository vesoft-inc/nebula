/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/BalanceTask.h"

#include <folly/synchronization/Baton.h>

#include "meta/processors/Common.h"

DEFINE_int32(catch_up_retry, 10, "retry count when catch up failed.");

DEFINE_int32(catch_up_interval_in_secs, 30, "interval between two requests for catching up state");

namespace nebula {
namespace meta {

#define SAVE_STATE()                  \
  if (!saveInStore()) {               \
    ret_ = BalanceTaskResult::FAILED; \
    onError_();                       \
    return;                           \
  }

void BalanceTask::invoke() {
  // All steps in BalanceTask should work even if the task is executed more than
  // once.
  CHECK_NOTNULL(client_);
  if (ret_ == BalanceTaskResult::INVALID) {
    endTimeMs_ = time::WallClock::fastNowInSec();
    saveInStore();
    LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Task invalid, status "
              << static_cast<int32_t>(status_);
    // When a plan is stopped or dst is not alive any more, a task will be
    // marked as INVALID, and the job will be marked as FAILED.
    onError_();
    return;
  } else if (ret_ == BalanceTaskResult::FAILED) {
    endTimeMs_ = time::WallClock::fastNowInSec();
    saveInStore();
    LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Task failed, status "
              << static_cast<int32_t>(status_);
    onError_();
    return;
  } else {
    LOG(INFO) << taskIdStr_ + "," + commandStr_ << " still in processing";
  }

  switch (status_) {
    case BalanceTaskStatus::START: {
      LOG(INFO) << taskIdStr_ << " Start to move part, check the peers firstly!";
      ret_ = BalanceTaskResult::IN_PROGRESS;
      startTimeMs_ = time::WallClock::fastNowInSec();
      SAVE_STATE();
      client_->checkPeers(spaceId_, partId_).thenValue([this](auto&& resp) {
        if (!resp.ok()) {
          LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Check the peers failed, status " << resp;
          ret_ = BalanceTaskResult::FAILED;
        } else {
          status_ = BalanceTaskStatus::CHANGE_LEADER;
        }
        invoke();
      });
      break;
    }
    // fallthrough
    case BalanceTaskStatus::CHANGE_LEADER: {
      LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Ask the src to give up the leadership.";
      SAVE_STATE();
      auto srcLivedRet = ActiveHostsMan::isLived(kv_, src_);
      if (nebula::ok(srcLivedRet) && nebula::value(srcLivedRet)) {
        client_->transLeader(spaceId_, partId_, src_).thenValue([this](auto&& resp) {
          if (!resp.ok()) {
            if (resp == nebula::Status::PartNotFound()) {
              // if the partition has been removed before, regard as
              // succeeded
              LOG(INFO) << "Can't find part " << partId_ << " on " << src_;
              status_ = BalanceTaskStatus::ADD_PART_ON_DST;
            } else {
              LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Transfer leader failed, status "
                        << resp;
              ret_ = BalanceTaskResult::FAILED;
            }
          } else {
            status_ = BalanceTaskStatus::ADD_PART_ON_DST;
          }
          invoke();
        });
        break;
      } else {
        LOG(INFO) << taskIdStr_ + "," + commandStr_
                  << " Src host has been lost, so no need to transfer leader";
        status_ = BalanceTaskStatus::ADD_PART_ON_DST;
      }
    }
    // fallthrough
    case BalanceTaskStatus::ADD_PART_ON_DST: {
      LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Open the part as learner on dst.";
      SAVE_STATE();
      client_->addPart(spaceId_, partId_, dst_, true).thenValue([this](auto&& resp) {
        if (!resp.ok()) {
          LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Open part failed, status " << resp;
          ret_ = BalanceTaskResult::FAILED;
        } else {
          status_ = BalanceTaskStatus::ADD_LEARNER;
        }
        invoke();
      });
      break;
    }
    case BalanceTaskStatus::ADD_LEARNER: {
      LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Add learner dst.";
      SAVE_STATE();
      client_->addLearner(spaceId_, partId_, dst_).thenValue([this](auto&& resp) {
        if (!resp.ok()) {
          LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Add learner failed, status " << resp;
          ret_ = BalanceTaskResult::FAILED;
        } else {
          status_ = BalanceTaskStatus::CATCH_UP_DATA;
        }
        invoke();
      });
      break;
    }
    case BalanceTaskStatus::CATCH_UP_DATA: {
      LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Waiting for the data catch up.";
      SAVE_STATE();
      int32_t retry = FLAGS_catch_up_retry;
      auto retryFunc = [&retry, this](bool ifError, const std::string& messageLog = "") {
        if (ifError) {
          if (!messageLog.empty()) {
            LOG(INFO) << taskIdStr_ + "," + commandStr_ + " " << messageLog << ", retry "
                      << FLAGS_catch_up_retry - retry + 1 << " time";
          }
          retry--;
        } else {
          if (!messageLog.empty()) {
            LOG(INFO) << taskIdStr_ + "," + commandStr_ + " " << messageLog;
          }
        }
        sleep(FLAGS_catch_up_interval_in_secs);
      };
      while (retry) {
        client_->waitingForCatchUpData(spaceId_, partId_, dst_)
            .thenValue([this, &retry, &retryFunc](auto&& statusOrResp) {
              if (!statusOrResp.ok()) {
                Status status = statusOrResp.status();
                retryFunc(true, status.message());
                return;
              }
              auto resp = statusOrResp.value();
              auto status = resp.get_status();
              if (status == nebula::storage::cpp2::CatchUpStatus::CAUGHT_UP) {
                LOG(INFO) << taskIdStr_ + "," + commandStr_ << " has already caught up";
                status_ = BalanceTaskStatus::MEMBER_CHANGE_ADD;
                retry = 0;
              } else if (status == nebula::storage::cpp2::CatchUpStatus::WAITING_FOR_SNAPSHOT) {
                int64_t snapshotRows = resp.get_snapshotRows();
                if (snapshotRows > lastSnapshotRows_) {
                  lastSnapshotRows_ = snapshotRows;
                  retryFunc(false, "snapshotRows increasing, continue waiting...");
                } else if (snapshotRows == lastSnapshotRows_) {
                  retryFunc(true, "snapshotRows did not increase");
                } else if (snapshotRows < lastSnapshotRows_) {
                  retryFunc(true, "snapshotRows has been reset, maybe leader changed");
                }
              } else if (status == nebula::storage::cpp2::CatchUpStatus::STARTING) {
                retryFunc(true, "the part is starting");
              } else if (status == nebula::storage::cpp2::CatchUpStatus::RUNNING) {
                int64_t logId = resp.get_commitLogId();
                if (logId > 0) {
                  LOG(INFO) << taskIdStr_ + "," + commandStr_
                            << " logId > 0 means it has caught up";
                  status_ = BalanceTaskStatus::MEMBER_CHANGE_ADD;
                  retry = 0;
                }
              }
            })
            .wait();
      }
      if (status_ != BalanceTaskStatus::MEMBER_CHANGE_ADD) {
        LOG(INFO) << taskIdStr_ + "," + commandStr_ << " catch up failed";
        ret_ = BalanceTaskResult::FAILED;
      }
      invoke();
      break;
    }
    case BalanceTaskStatus::MEMBER_CHANGE_ADD: {
      LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Send member change request to the leader"
                << ", it will add the new member on dst host";
      SAVE_STATE();
      client_->memberChange(spaceId_, partId_, dst_, true).thenValue([this](auto&& resp) {
        if (!resp.ok()) {
          LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Add peer failed, status " << resp;
          ret_ = BalanceTaskResult::FAILED;
        } else {
          status_ = BalanceTaskStatus::MEMBER_CHANGE_REMOVE;
        }
        invoke();
      });
      break;
    }
    case BalanceTaskStatus::MEMBER_CHANGE_REMOVE: {
      LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Send member change request to the leader"
                << ", it will remove the old member on src host";
      SAVE_STATE();
      client_->memberChange(spaceId_, partId_, src_, false).thenValue([this](auto&& resp) {
        if (!resp.ok()) {
          LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Remove peer failed, status " << resp;
          ret_ = BalanceTaskResult::FAILED;
        } else {
          status_ = BalanceTaskStatus::UPDATE_PART_META;
        }
        invoke();
      });
      break;
    }
    case BalanceTaskStatus::UPDATE_PART_META: {
      LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Update meta for part.";
      SAVE_STATE();
      client_->updateMeta(spaceId_, partId_, src_, dst_).thenValue([this](auto&& resp) {
        // The callback will be called inside raft set value. So don't call
        // invoke directly here.
        if (!resp.ok()) {
          LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Update meta failed, status " << resp;
          ret_ = BalanceTaskResult::FAILED;
        } else {
          LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Update meta succeeded!";
          status_ = BalanceTaskStatus::REMOVE_PART_ON_SRC;
        }
        invoke();
      });
      break;
    }
    case BalanceTaskStatus::REMOVE_PART_ON_SRC: {
      auto srcLivedRet = ActiveHostsMan::isLived(kv_, src_);
      LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Close part on src host, srcLived.";
      SAVE_STATE();
      if (nebula::ok(srcLivedRet) && nebula::value(srcLivedRet)) {
        client_->removePart(spaceId_, partId_, src_).thenValue([this](auto&& resp) {
          if (!resp.ok()) {
            LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Remove part failed, status " << resp;
            ret_ = BalanceTaskResult::FAILED;
          } else {
            status_ = BalanceTaskStatus::CHECK;
          }
          invoke();
        });
        break;
      } else {
        LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Don't remove part on src " << src_;
        status_ = BalanceTaskStatus::CHECK;
      }
    }
    // fallthrough
    case BalanceTaskStatus::CHECK: {
      LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Check the peers...";
      SAVE_STATE();
      client_->checkPeers(spaceId_, partId_).thenValue([this](auto&& resp) {
        if (!resp.ok()) {
          LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Check the peers failed, status " << resp;
          ret_ = BalanceTaskResult::FAILED;
        } else {
          status_ = BalanceTaskStatus::END;
        }
        invoke();
      });
      break;
    }
    case BalanceTaskStatus::END: {
      LOG(INFO) << taskIdStr_ + "," + commandStr_ << " Part has been moved successfully!";
      endTimeMs_ = time::WallClock::fastNowInSec();
      ret_ = BalanceTaskResult::SUCCEEDED;
      SAVE_STATE();
      onFinished_();
      break;
    }
  }
  return;
}

void BalanceTask::rollback() {
  if (status_ < BalanceTaskStatus::UPDATE_PART_META) {
    // TODO(heng): restart the part on its peers.
  } else {
    // TODO(heng): Go on the task.
  }
}

bool BalanceTask::saveInStore() {
  CHECK_NOTNULL(kv_);
  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::balanceTaskKey(jobId_, spaceId_, partId_, src_, dst_),
                    MetaKeyUtils::balanceTaskVal(status_, ret_, startTimeMs_, endTimeMs_));
  folly::Baton<true, std::atomic> baton;
  bool ret = true;
  kv_->asyncMultiPut(kDefaultSpaceId,
                     kDefaultPartId,
                     std::move(data),
                     [this, &ret, &baton](nebula::cpp2::ErrorCode code) {
                       if (nebula::cpp2::ErrorCode::SUCCEEDED != code) {
                         ret = false;
                         LOG(INFO) << taskIdStr_ << " Can't persist task!";
                       }
                       baton.post();
                     });
  baton.wait();
  if (ret_ == BalanceTaskResult::INVALID)
    LOG(INFO) << taskIdStr_ + "," + commandStr_ << " save task: " << static_cast<int32_t>(status_)
              << " " << static_cast<int32_t>(ret_);
  return ret;
}

}  // namespace meta
}  // namespace nebula
