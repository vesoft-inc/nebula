/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/BalanceTask.h"
#include <folly/synchronization/Baton.h>
#include "meta/processors/Common.h"

DEFINE_int32(wait_time_after_open_part_ms, 3000,
             "The wait time after open part, zero means no wait");
DECLARE_uint32(raft_heartbeat_interval_secs);

namespace nebula {
namespace meta {

#define SAVE_STATE() \
    if (!saveInStore()) { \
        ret_ = Result::FAILED; \
        onError_(); \
        return; \
    }

const std::string kBalanceTaskTable = "__b_task__"; // NOLINT

void BalanceTask::invoke() {
    CHECK_NOTNULL(client_);
    if (ret_ == Result::INVALID) {
        endTimeMs_ = time::WallClock::fastNowInMilliSec();
        saveInStore();
        LOG(ERROR) << taskIdStr_ << "Task invalid!";
        onFinished_();
        return;
    }
    if (ret_ == Result::FAILED) {
        endTimeMs_ = time::WallClock::fastNowInMilliSec();
        saveInStore();
        LOG(ERROR) << taskIdStr_ << "Task failed, status_ " << static_cast<int32_t>(status_);
        onError_();
        return;
    }
    switch (status_) {
        case Status::START: {
            LOG(INFO) << taskIdStr_ << "Start to move part!";
            status_ = Status::CHANGE_LEADER;
            ret_ = Result::IN_PROGRESS;
            startTimeMs_ = time::WallClock::fastNowInMilliSec();
        }
        case Status::CHANGE_LEADER: {
            LOG(INFO) << taskIdStr_ << "Ask the src to give up the leadership.";
            SAVE_STATE();
            if (srcLived_) {
                client_->transLeader(spaceId_, partId_, src_).thenValue([this](auto&& resp) {
                    if (!resp.ok()) {
                        ret_ = Result::FAILED;
                    } else {
                        status_ = Status::ADD_PART_ON_DST;
                    }
                    invoke();
                });
                break;
            } else {
                LOG(INFO) << taskIdStr_ << "Src host has been lost, so no need to transfer leader";
                status_ = Status::ADD_PART_ON_DST;
            }
        }
        case Status::ADD_PART_ON_DST: {
            LOG(INFO) << taskIdStr_ << "Open the part as learner on dst.";
            SAVE_STATE();
            client_->addPart(spaceId_, partId_, dst_, true).thenValue([this](auto&& resp) {
                if (!resp.ok()) {
                    ret_ = Result::FAILED;
                } else {
                    status_ = Status::ADD_LEARNER;
                    if (FLAGS_wait_time_after_open_part_ms > 0) {
                        usleep(FLAGS_wait_time_after_open_part_ms * 1000);
                        sleep(FLAGS_raft_heartbeat_interval_secs);
                    }
                }
                invoke();
            });
            break;
        }
        case Status::ADD_LEARNER: {
            LOG(INFO) << taskIdStr_ << "Add learner dst.";
            SAVE_STATE();
            client_->addLearner(spaceId_, partId_, dst_).thenValue([this](auto&& resp) {
                if (!resp.ok()) {
                    ret_ = Result::FAILED;
                } else {
                    status_ = Status::CATCH_UP_DATA;
                }
                invoke();
            });
            break;
        }
        case Status::CATCH_UP_DATA: {
            LOG(INFO) << taskIdStr_ << "Waiting for the data catch up.";
            SAVE_STATE();
            client_->waitingForCatchUpData(spaceId_, partId_, dst_).thenValue([this](auto&& resp) {
                if (!resp.ok()) {
                    ret_ = Result::FAILED;
                } else {
                    status_ = Status::MEMBER_CHANGE_ADD;
                }
                invoke();
            });
            break;
        }
        case Status::MEMBER_CHANGE_ADD: {
            LOG(INFO) << taskIdStr_ << "Send member change request to the leader"
                      << ", it will add the new member on dst host";
            SAVE_STATE();
            client_->memberChange(spaceId_, partId_, dst_, true).thenValue([this](auto&& resp) {
                if (!resp.ok()) {
                    ret_ = Result::FAILED;
                } else {
                    status_ = Status::MEMBER_CHANGE_REMOVE;
                }
                invoke();
            });
            break;
        }
        case Status::MEMBER_CHANGE_REMOVE: {
            LOG(INFO) << taskIdStr_ << "Send member change request to the leader"
                      << ", it will remove the old member on src host";
            SAVE_STATE();
            client_->memberChange(spaceId_, partId_, src_, false).thenValue(
                    [this] (auto&& resp) {
                if (!resp.ok()) {
                    ret_ = Result::FAILED;
                } else {
                    status_ = Status::UPDATE_PART_META;
                }
                invoke();
            });
            break;
        }
        case Status::UPDATE_PART_META: {
            LOG(INFO) << taskIdStr_ << "Update meta for part.";
            SAVE_STATE();
            client_->updateMeta(spaceId_, partId_, src_, dst_).thenValue(
                        [this] (auto&& resp) {
                // The callback will be called inside raft set value. So don't call invoke directly
                // here.
                LOG(INFO) << "Update meta succeeded!";
                if (!resp.ok()) {
                    ret_ = Result::FAILED;
                } else {
                    status_ = Status::REMOVE_PART_ON_SRC;
                }
                invoke();
            });
            break;
        }
        case Status::REMOVE_PART_ON_SRC: {
            LOG(INFO) << taskIdStr_ << "Close part on src host, srcLived " << srcLived_;
            SAVE_STATE();
            if (srcLived_) {
                client_->removePart(spaceId_, partId_, src_).thenValue([this](auto&& resp) {
                    if (!resp.ok()) {
                        ret_ = Result::FAILED;
                    } else {
                        ret_ = Result::SUCCEEDED;
                        status_ = Status::END;
                    }
                    invoke();
                });
                break;
            } else {
                LOG(INFO) << taskIdStr_ << "Don't remove part on src " << src_;
                ret_ = Result::SUCCEEDED;
                status_ = Status::END;
            }
        }
        case Status::END: {
            LOG(INFO) << taskIdStr_ <<  "Part has been moved successfully!";
            endTimeMs_ = time::WallClock::fastNowInSec();
            SAVE_STATE();
            onFinished_();
            break;
        }
    }
    return;
}

void BalanceTask::rollback() {
    if (status_ < Status::UPDATE_PART_META) {
        // TODO(heng): restart the part on its peers.
    } else {
        // TODO(heng): Go on the task.
    }
}

bool BalanceTask::saveInStore() {
    if (kv_) {
        std::vector<kvstore::KV> data;
        data.emplace_back(taskKey(), taskVal());
        folly::Baton<true, std::atomic> baton;
        bool ret = false;
        kv_->asyncMultiPut(kDefaultSpaceId,
                           kDefaultPartId,
                           std::move(data),
                           [this, &ret, &baton] (kvstore::ResultCode code) {
            if (kvstore::ResultCode::SUCCEEDED == code) {
                ret = true;
            } else {
                LOG(INFO) << taskIdStr_ << "Can't persist task!";
            }
            baton.post();
        });
        baton.wait();
        return ret;
    }
    return true;
}

std::string BalanceTask::taskKey() {
    std::string str;
    str.reserve(64);
    str.append(reinterpret_cast<const char*>(kBalanceTaskTable.data()), kBalanceTaskTable.size());
    str.append(reinterpret_cast<const char*>(&balanceId_), sizeof(balanceId_));
    str.append(reinterpret_cast<const char*>(&spaceId_), sizeof(spaceId_));
    str.append(reinterpret_cast<const char*>(&partId_), sizeof(partId_));
    str.append(reinterpret_cast<const char*>(&src_), sizeof(src_));
    str.append(reinterpret_cast<const char*>(&dst_), sizeof(dst_));
    return str;
}

std::string BalanceTask::taskVal() {
    std::string str;
    str.reserve(32);
    str.append(reinterpret_cast<const char*>(&status_), sizeof(status_));
    str.append(reinterpret_cast<const char*>(&ret_), sizeof(ret_));
    str.append(reinterpret_cast<const char*>(&srcLived_), sizeof(srcLived_));
    str.append(reinterpret_cast<const char*>(&startTimeMs_), sizeof(startTimeMs_));
    str.append(reinterpret_cast<const char*>(&endTimeMs_), sizeof(endTimeMs_));
    return str;
}

std::string BalanceTask::prefix(BalanceID balanceId) {
    std::string str;
    str.reserve(32);
    str.append(reinterpret_cast<const char*>(kBalanceTaskTable.data()), kBalanceTaskTable.size());
    str.append(reinterpret_cast<const char*>(&balanceId), sizeof(balanceId));
    return str;
}

std::tuple<BalanceID, GraphSpaceID, PartitionID, HostAddr, HostAddr>
BalanceTask::parseKey(const folly::StringPiece& rawKey) {
    int32_t offset = kBalanceTaskTable.size();
    auto balanceId = *reinterpret_cast<const BalanceID*>(rawKey.begin() + offset);
    offset += sizeof(balanceId);
    auto spaceId = *reinterpret_cast<const GraphSpaceID*>(rawKey.begin() + offset);
    offset += sizeof(GraphSpaceID);
    auto partId = *reinterpret_cast<const PartitionID*>(rawKey.begin() + offset);
    offset += sizeof(PartitionID);
    auto src = *reinterpret_cast<const HostAddr*>(rawKey.begin() + offset);
    offset += sizeof(HostAddr);
    auto dst = *reinterpret_cast<const HostAddr*>(rawKey.begin() + offset);
    return std::make_tuple(balanceId, spaceId, partId, src, dst);
}

std::tuple<BalanceTask::Status, BalanceTask::Result, bool, int64_t, int64_t>
BalanceTask::parseVal(const folly::StringPiece& rawVal) {
    int32_t offset = 0;
    auto status = *reinterpret_cast<const BalanceTask::Status*>(rawVal.begin() + offset);
    offset += sizeof(BalanceTask::Status);
    auto ret = *reinterpret_cast<const BalanceTask::Result*>(rawVal.begin() + offset);
    offset += sizeof(BalanceTask::Result);
    auto srcLived = *reinterpret_cast<const bool*>(rawVal.begin() + offset);
    offset += sizeof(bool);
    auto start = *reinterpret_cast<const int64_t*>(rawVal.begin() + offset);
    offset += sizeof(int64_t);
    auto end = *reinterpret_cast<const int64_t*>(rawVal.begin() + offset);
    return std::make_tuple(status, ret, srcLived, start, end);
}

}  // namespace meta
}  // namespace nebula

