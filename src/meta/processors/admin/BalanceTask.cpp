/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/BalanceTask.h"
#include <folly/synchronization/Baton.h>
#include "meta/processors/Common.h"

namespace nebula {
namespace meta {

#define SAVE_STATE() \
    if (!saveInStore()) { \
        ret_ = BalanceTaskResult::FAILED; \
        onError_(); \
        return; \
    }

void BalanceTask::invoke() {
    // All steps in BalanceTask should work even if the task is executed more than once.
    CHECK_NOTNULL(client_);
    if (ret_ == BalanceTaskResult::INVALID) {
        endTimeMs_ = time::WallClock::fastNowInMilliSec();
        saveInStore();
        LOG(ERROR) << taskIdStr_ << " Task invalid, status "
                   << static_cast<int32_t>(status_);
        // When a plan is stopped or dst is not alive any more, a task will be marked as INVALID,
        // the task will not be executed again. Balancer will start a new plan instead.
        onFinished_();
        return;
    } else if (ret_ == BalanceTaskResult::FAILED) {
        endTimeMs_ = time::WallClock::fastNowInMilliSec();
        saveInStore();
        LOG(ERROR) << taskIdStr_ << " Task failed, status "
                   << static_cast<int32_t>(status_);
        onError_();
        return;
    } else {
        VLOG(3) << taskIdStr_ << " still in processing";
    }

    switch (status_) {
        case BalanceTaskStatus::START: {
            LOG(INFO) << taskIdStr_ << " Start to move part, check the peers firstly!";
            ret_ = BalanceTaskResult::IN_PROGRESS;
            startTimeMs_ = time::WallClock::fastNowInMilliSec();
            SAVE_STATE();
            client_->checkPeers(spaceId_, partId_).thenValue([this] (auto&& resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << taskIdStr_ << " Check the peers failed, status " << resp;
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
            LOG(INFO) << taskIdStr_ << " Ask the src to give up the leadership.";
            SAVE_STATE();
            auto srcLivedRet = ActiveHostsMan::isLived(kv_, src_);
            if (nebula::ok(srcLivedRet) && nebula::value(srcLivedRet)) {
                client_->transLeader(spaceId_, partId_, src_).thenValue([this](auto&& resp) {
                    if (!resp.ok()) {
                        if (resp == nebula::Status::PartNotFound()) {
                            // if the partition has been removed before, regard as succeeded
                            LOG(WARNING) << "Can't find part " << partId_ << " on " << src_;
                            status_ = BalanceTaskStatus::ADD_PART_ON_DST;
                        } else {
                            LOG(ERROR) << taskIdStr_ << " Transfer leader failed, status " << resp;
                            ret_ = BalanceTaskResult::FAILED;
                        }
                    } else {
                        status_ = BalanceTaskStatus::ADD_PART_ON_DST;
                    }
                    invoke();
                });
                break;
            } else {
                LOG(INFO) << taskIdStr_ << " Src host has been lost, so no need to transfer leader";
                status_ = BalanceTaskStatus::ADD_PART_ON_DST;
            }
        }
        // fallthrough
        case BalanceTaskStatus::ADD_PART_ON_DST: {
            LOG(INFO) << taskIdStr_ << " Open the part as learner on dst.";
            SAVE_STATE();
            client_->addPart(spaceId_, partId_, dst_, true).thenValue([this](auto&& resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << taskIdStr_ << " Open part failed, status " << resp;
                    ret_ = BalanceTaskResult::FAILED;
                } else {
                    status_ = BalanceTaskStatus::ADD_LEARNER;
                }
                invoke();
            });
            break;
        }
        case BalanceTaskStatus::ADD_LEARNER: {
            LOG(INFO) << taskIdStr_ << " Add learner dst.";
            SAVE_STATE();
            client_->addLearner(spaceId_, partId_, dst_).thenValue([this](auto&& resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << taskIdStr_ << " Add learner failed, status " << resp;
                    ret_ = BalanceTaskResult::FAILED;
                } else {
                    status_ = BalanceTaskStatus::CATCH_UP_DATA;
                }
                invoke();
            });
            break;
        }
        case BalanceTaskStatus::CATCH_UP_DATA: {
            LOG(INFO) << taskIdStr_ << " Waiting for the data catch up.";
            SAVE_STATE();
            client_->waitingForCatchUpData(spaceId_, partId_, dst_).thenValue([this](auto&& resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << taskIdStr_ << " Catchup data failed, status " << resp;
                    ret_ = BalanceTaskResult::FAILED;
                } else {
                    status_ = BalanceTaskStatus::MEMBER_CHANGE_ADD;
                }
                invoke();
            });
            break;
        }
        case BalanceTaskStatus::MEMBER_CHANGE_ADD: {
            LOG(INFO) << taskIdStr_ << " Send member change request to the leader"
                      << ", it will add the new member on dst host";
            SAVE_STATE();
            client_->memberChange(spaceId_, partId_, dst_, true).thenValue([this](auto&& resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << taskIdStr_ << " Add peer failed, status " << resp;
                    ret_ = BalanceTaskResult::FAILED;
                } else {
                    status_ = BalanceTaskStatus::MEMBER_CHANGE_REMOVE;
                }
                invoke();
            });
            break;
        }
        case BalanceTaskStatus::MEMBER_CHANGE_REMOVE: {
            LOG(INFO) << taskIdStr_ << " Send member change request to the leader"
                      << ", it will remove the old member on src host";
            SAVE_STATE();
            client_->memberChange(spaceId_, partId_, src_, false).thenValue(
                    [this] (auto&& resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << taskIdStr_ << " Remove peer failed, status " << resp;
                    ret_ = BalanceTaskResult::FAILED;
                } else {
                    status_ = BalanceTaskStatus::UPDATE_PART_META;
                }
                invoke();
            });
            break;
        }
        case BalanceTaskStatus::UPDATE_PART_META: {
            LOG(INFO) << taskIdStr_ << " Update meta for part.";
            SAVE_STATE();
            client_->updateMeta(spaceId_, partId_, src_, dst_).thenValue(
                        [this] (auto&& resp) {
                // The callback will be called inside raft set value. So don't call invoke directly
                // here.
                if (!resp.ok()) {
                    LOG(ERROR) << taskIdStr_ << " Update meta failed, status " << resp;
                    ret_ = BalanceTaskResult::FAILED;
                } else {
                    LOG(INFO) << taskIdStr_ << " Update meta succeeded!";
                    status_ = BalanceTaskStatus::REMOVE_PART_ON_SRC;
                }
                invoke();
            });
            break;
        }
        case BalanceTaskStatus::REMOVE_PART_ON_SRC: {
            auto srcLivedRet = ActiveHostsMan::isLived(kv_, src_);
            LOG(INFO) << taskIdStr_ << " Close part on src host, srcLived.";
            SAVE_STATE();
            if (nebula::ok(srcLivedRet) && nebula::value(srcLivedRet)) {
                client_->removePart(spaceId_, partId_, src_).thenValue([this](auto&& resp) {
                    if (!resp.ok()) {
                        LOG(ERROR) << taskIdStr_ << " Remove part failed, status " << resp;
                        ret_ = BalanceTaskResult::FAILED;
                    } else {
                        status_ = BalanceTaskStatus::CHECK;
                    }
                    invoke();
                });
                break;
            } else {
                LOG(INFO) << taskIdStr_ << " Don't remove part on src " << src_;
                status_ = BalanceTaskStatus::CHECK;
            }
        }
        // fallthrough
        case BalanceTaskStatus::CHECK: {
            LOG(INFO) << taskIdStr_ << " Check the peers...";
            SAVE_STATE();
            client_->checkPeers(spaceId_, partId_).thenValue([this] (auto&& resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << taskIdStr_ << " Check the peers failed, status " << resp;
                    ret_ = BalanceTaskResult::FAILED;
                } else {
                    status_ = BalanceTaskStatus::END;
                }
                invoke();
            });
            break;
        }
        case BalanceTaskStatus::END: {
            LOG(INFO) << taskIdStr_ << " Part has been moved successfully!";
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
    data.emplace_back(MetaServiceUtils::balanceTaskKey(balanceId_, spaceId_,
                                                       partId_, src_, dst_),
                      MetaServiceUtils::balanceTaskVal(status_, ret_,
                                                       startTimeMs_,
                                                       endTimeMs_));
    folly::Baton<true, std::atomic> baton;
    bool ret = true;
    kv_->asyncMultiPut(kDefaultSpaceId,
                       kDefaultPartId,
                       std::move(data),
                       [this, &ret, &baton] (nebula::cpp2::ErrorCode code) {
        if (nebula::cpp2::ErrorCode::SUCCEEDED != code) {
            ret = false;
            LOG(ERROR) << taskIdStr_ << " Can't persist task!";
        }
        baton.post();
    });
    baton.wait();
    return ret;
}

}  // namespace meta
}  // namespace nebula

