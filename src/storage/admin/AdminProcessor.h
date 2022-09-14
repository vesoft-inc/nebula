/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef STORAGE_ADMIN_ADMINPROCESSOR_H_
#define STORAGE_ADMIN_ADMINPROCESSOR_H_

#include <folly/executors/Async.h>

#include "common/base/Base.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/Part.h"
#include "kvstore/stats/KVStats.h"
#include "storage/BaseProcessor.h"
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

/**
 * @brief Processor class to handle leader transfer.
 *
 */
class TransLeaderProcessor : public BaseProcessor<cpp2::AdminExecResp> {
 public:
  /**
   * @brief Construct new instance of TransLeaderProcessor.
   *
   * @param env Related environment variables for storage.
   * @return TransLeaderProcessor* TransLeaderProcessor instance.
   */
  static TransLeaderProcessor* instance(StorageEnv* env) {
    return new TransLeaderProcessor(env);
  }

  /**
   * @brief Entry point for trans leader.
   *
   * @param req  Reuqest for trans leader.
   */
  void process(const cpp2::TransLeaderReq& req) {
    CHECK_NOTNULL(env_->kvstore_);
    LOG(INFO) << "Receive transfer leader for space " << req.get_space_id() << ", part "
              << req.get_part_id() << ", to [" << req.get_new_leader().host << ", "
              << req.get_new_leader().port << "]";
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    auto ret = env_->kvstore_->part(spaceId, partId);
    if (!ok(ret)) {
      LOG(INFO) << "Space: " << spaceId << " Part: " << partId << " not found";
      this->pushResultCode(error(ret), partId);
      onFinished();
      return;
    }
    auto part = nebula::value(ret);
    auto host = kvstore::NebulaStore::getRaftAddr(req.get_new_leader());
    if (part->isLeader() && part->address() == host) {
      LOG(INFO) << "I am already leader of space " << spaceId << " part " << partId
                << ", skip transLeader";
      onFinished();
      return;
    }
    auto* partManager = env_->kvstore_->partManager();
    auto status = partManager->partMeta(spaceId, partId);
    if (!status.ok()) {
      LOG(INFO) << "Space: " << spaceId << " Part: " << partId << " not found";
      this->pushResultCode(nebula::cpp2::ErrorCode::E_PART_NOT_FOUND, partId);
      onFinished();
      return;
    }
    auto partMeta = status.value();
    if (partMeta.hosts_.size() == 1) {
      LOG(INFO) << "Skip transfer leader of space " << spaceId << ", part " << partId
                << " because of single replica.";
      onFinished();
      return;
    }

    part->asyncTransferLeader(host, [this, spaceId, partId, part](nebula::cpp2::ErrorCode code) {
      if (code == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
        LOG(INFO) << "I am not the leader of space " << spaceId << " part " << partId;
        handleLeaderChanged(spaceId, partId);
        onFinished();
        return;
      } else if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
        // stats::StatsManager::addValue(kTransferLeaderLatencyUs, part->execTime());
        // To avoid dead lock, we use another ioThreadPool to check the
        // leader information.
        folly::via(folly::getGlobalIOExecutor().get(), [this, part, spaceId, partId] {
          int retry = FLAGS_waiting_new_leader_retry_times;
          while (retry-- > 0) {
            auto leaderRet = env_->kvstore_->partLeader(spaceId, partId);
            if (!ok(leaderRet)) {
              LOG(INFO) << "Space " << spaceId << " Part " << partId << " leader not found";
              this->pushResultCode(error(leaderRet), partId);
              onFinished();
              return;
            }

            auto leader = value(std::move(leaderRet));
            auto* store = static_cast<kvstore::NebulaStore*>(env_->kvstore_);
            if (leader != HostAddr("", 0) && leader != store->address()) {
              LOG(INFO) << "Found new leader of space " << spaceId << " part " << partId << ": "
                        << leader;
              onFinished();
              return;
            } else if (leader != HostAddr("", 0)) {
              LOG(INFO) << "I am chosen as leader of space " << spaceId << " part " << partId
                        << " again!";
              pushResultCode(nebula::cpp2::ErrorCode::E_TRANSFER_LEADER_FAILED, partId);
              onFinished();
              return;
            }
            LOG(INFO) << "Can't find leader for space " << spaceId << " part " << partId << " on "
                      << store->address();
            sleep(FLAGS_waiting_new_leader_interval_in_secs);
          }
          pushResultCode(nebula::cpp2::ErrorCode::E_RETRY_EXHAUSTED, partId);
          onFinished();
        });
      } else {
        LOG(INFO) << "Space " << spaceId << " part " << partId
                  << " failed transfer leader, error: " << static_cast<int32_t>(code);
        this->pushResultCode(code, partId);
        onFinished();
        return;
      }
    });
  }

 private:
  explicit TransLeaderProcessor(StorageEnv* env) : BaseProcessor<cpp2::AdminExecResp>(env) {}
};

/**
 * @brief Processor class to handle adding partitions.
 *
 */
class AddPartProcessor : public BaseProcessor<cpp2::AdminExecResp> {
 public:
  /**
   * @brief Construct new instance of AddPartProcessor
   *
   * @param env Related environment variables for storage.
   * @return AddPartProcessor* AddPartProcessor instance.
   */
  static AddPartProcessor* instance(StorageEnv* env) {
    return new AddPartProcessor(env);
  }

  /**
   * @brief Entry point for adding partitions.
   *
   * @param req Request for adding partitions.
   */
  void process(const cpp2::AddPartReq& req) {
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    if (FLAGS_store_type != "nebula") {
      this->pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_STORE, partId);
      onFinished();
      return;
    }

    LOG(INFO) << "Receive add part for space " << req.get_space_id() << ", part " << partId;
    auto* store = static_cast<kvstore::NebulaStore*>(env_->kvstore_);
    auto ret = store->space(spaceId);
    if (!nebula::ok(ret) && nebula::error(ret) == nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
      LOG(INFO) << "Space " << spaceId << " not exist, create it!";
      store->addSpace(spaceId);
    }
    store->addPart(spaceId, partId, req.get_as_learner(), req.get_peers());
    onFinished();
  }

 private:
  explicit AddPartProcessor(StorageEnv* env) : BaseProcessor<cpp2::AdminExecResp>(env) {}
};

class RemovePartProcessor : public BaseProcessor<cpp2::AdminExecResp> {
 public:
  /**
   * @brief Construct new instance of RemovePartProcess.
   *
   * @param env Related environment variables for storage.
   * @return RemovePartProcessor* RemovePartProcessor instance.
   */
  static RemovePartProcessor* instance(StorageEnv* env) {
    return new RemovePartProcessor(env);
  }

  /**
   * @brief Entry point for removing part.
   *
   * @param req Reuqest for removing part.
   */
  void process(const cpp2::RemovePartReq& req) {
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    if (FLAGS_store_type != "nebula") {
      this->pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_STORE, partId);
      onFinished();
      return;
    }
    auto* store = static_cast<kvstore::NebulaStore*>(env_->kvstore_);
    store->removePart(spaceId, partId);
    onFinished();
  }

 private:
  explicit RemovePartProcessor(StorageEnv* env) : BaseProcessor<cpp2::AdminExecResp>(env) {}
};

/**
 * @brief Processor class to handle member change.
 *
 */
class MemberChangeProcessor : public BaseProcessor<cpp2::AdminExecResp> {
 public:
  /**
   * @brief Construct new instance of MemberChangeProcessor.
   *
   * @param env Related environment variables for storage.
   * @return MemberChangeProcessor* MemberChangeProcessor instance.
   */
  static MemberChangeProcessor* instance(StorageEnv* env) {
    return new MemberChangeProcessor(env);
  }

  /**
   * @brief Entry point to handle member change.
   *
   * @param req Request for member change.
   */
  void process(const cpp2::MemberChangeReq& req) {
    CHECK_NOTNULL(env_->kvstore_);
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    LOG(INFO) << "Receive member change for space " << spaceId << ", part " << partId
              << ", add/remove " << (req.get_add() ? "add" : "remove");
    auto ret = env_->kvstore_->part(spaceId, partId);
    if (!ok(ret)) {
      LOG(INFO) << "Space: " << spaceId << " Part: " << partId << " not found";
      this->pushResultCode(error(ret), partId);
      onFinished();
      return;
    }
    auto part = nebula::value(ret);
    auto peer = kvstore::NebulaStore::getRaftAddr(req.get_peer());
    auto cb = [this, spaceId, partId](nebula::cpp2::ErrorCode code) {
      handleErrorCode(code, spaceId, partId);
      onFinished();
      return;
    };
    if (req.get_add()) {
      LOG(INFO) << "Add peer " << peer;
      part->asyncAddPeer(peer, cb);
    } else {
      LOG(INFO) << "Remove peer " << peer;
      part->asyncRemovePeer(peer, cb);
    }
  }

 private:
  explicit MemberChangeProcessor(StorageEnv* env) : BaseProcessor<cpp2::AdminExecResp>(env) {}
};

/**
 * @brief Processor class to handle adding leader.
 *
 */
class AddLearnerProcessor : public BaseProcessor<cpp2::AdminExecResp> {
 public:
  /**
   * @brief Construct new instance of AddLearnerProcessor.
   *
   * @param env Related environment variables for storage.
   * @return AddLearnerProcessor* AddLearnerProcessor instance.
   */
  static AddLearnerProcessor* instance(StorageEnv* env) {
    return new AddLearnerProcessor(env);
  }

  /**
   * @brief Entry point for adding learner.
   *
   * @param req Reuqest for adding learner.
   */
  void process(const cpp2::AddLearnerReq& req) {
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    LOG(INFO) << "Receive add learner for space " << spaceId << ", part " << partId;
    auto ret = env_->kvstore_->part(spaceId, partId);
    if (!ok(ret)) {
      LOG(INFO) << "Space: " << spaceId << " Part: " << partId << " not found";
      this->pushResultCode(error(ret), partId);
      onFinished();
      return;
    }
    auto part = nebula::value(ret);
    auto learner = kvstore::NebulaStore::getRaftAddr(req.get_learner());
    part->asyncAddLearner(learner, [this, spaceId, partId](nebula::cpp2::ErrorCode code) {
      handleErrorCode(code, spaceId, partId);
      onFinished();
      return;
    });
  }

 private:
  explicit AddLearnerProcessor(StorageEnv* env) : BaseProcessor<cpp2::AdminExecResp>(env) {}
};

/**
 * @brief Processor class to handle waiting data catching up.
 *
 */
class WaitingForCatchUpDataProcessor : public BaseProcessor<cpp2::AdminExecResp> {
 public:
  /**
   * @brief Construct new instance of WaitingForCatchUpDataProcessor.
   *
   * @param env Related environment variables for storage.
   * @return WaitingForCatchUpDataProcessor* WaitingForCatchUpDataProcessor instance.
   */
  static WaitingForCatchUpDataProcessor* instance(StorageEnv* env) {
    return new WaitingForCatchUpDataProcessor(env);
  }

  ~WaitingForCatchUpDataProcessor() {}

  /**
   * @brief Entry point to wait data catching up data for space.
   *
   * @param req Reuqest for waiting data catching up.
   */
  void process(const cpp2::CatchUpDataReq& req) {
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    LOG(INFO) << "Received waiting for catching up data for space " << spaceId << ", part "
              << partId;
    auto ret = env_->kvstore_->part(spaceId, partId);
    if (!ok(ret)) {
      this->pushResultCode(error(ret), partId);
      onFinished();
      return;
    }
    auto part = nebula::value(ret);
    auto peer = kvstore::NebulaStore::getRaftAddr(req.get_target());

    folly::async([this, part, peer, spaceId, partId] {
      int retry = FLAGS_waiting_catch_up_retry_times;
      while (retry-- > 0) {
        auto res = part->isCatchedUp(peer);
        LOG(INFO) << "Waiting for catching up data, peer " << peer << ", space " << spaceId
                  << ", part " << partId << ", remaining " << retry << " retry times"
                  << ", result " << static_cast<int32_t>(res);
        switch (res) {
          case nebula::cpp2::ErrorCode::SUCCEEDED:
            onFinished();
            return;
          case nebula::cpp2::ErrorCode::E_RAFT_INVALID_PEER:
            this->pushResultCode(nebula::cpp2::ErrorCode::E_INVALID_PEER, partId);
            onFinished();
            return;
          case nebula::cpp2::ErrorCode::E_LEADER_CHANGED: {
            handleLeaderChanged(spaceId, partId);
            onFinished();
            return;
          }
          case nebula::cpp2::ErrorCode::E_RAFT_SENDING_SNAPSHOT:
            LOG(INFO) << "Space " << spaceId << ", partId " << partId
                      << " is still sending snapshot, please wait...";
            break;
          default:
            LOG(INFO) << "Unknown error " << static_cast<int32_t>(res);
            break;
        }
        sleep(FLAGS_waiting_catch_up_interval_in_secs);
      }
      this->pushResultCode(nebula::cpp2::ErrorCode::E_RETRY_EXHAUSTED, partId);
      onFinished();
    });
  }

 private:
  explicit WaitingForCatchUpDataProcessor(StorageEnv* env)
      : BaseProcessor<cpp2::AdminExecResp>(env) {}
};

/**
 * @brief Processor to check peer status.
 *
 */
class CheckPeersProcessor : public BaseProcessor<cpp2::AdminExecResp> {
 public:
  /**
   * @brief Construct new instance of CheckPeersProcessor.
   *
   * @param env Related environment variables for storage.
   * @return CheckPeersProcessor* CheckPeersProcessor instance.
   */
  static CheckPeersProcessor* instance(StorageEnv* env) {
    return new CheckPeersProcessor(env);
  }

  /**
   * @brief Entry point for checking peers status.
   *
   * @param req Request for check peers status.
   */
  void process(const cpp2::CheckPeersReq& req) {
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    LOG(INFO) << "Check peers for space " << spaceId << ", part " << partId;
    auto ret = env_->kvstore_->part(spaceId, partId);
    if (!ok(ret)) {
      this->pushResultCode(error(ret), partId);
      onFinished();
      return;
    }
    auto part = nebula::value(ret);
    std::vector<HostAddr> peers;
    for (auto& p : req.get_peers()) {
      peers.emplace_back(kvstore::NebulaStore::getRaftAddr(p));
    }
    part->checkAndResetPeers(peers);

    for (auto p : peers) {
      // change the promoted peer to the normal peer when finish balancing
      part->engine()->updatePart(partId, kvstore::Peer(p, kvstore::Peer::Status::kNormalPeer));
    }
    this->onFinished();
  }

 private:
  explicit CheckPeersProcessor(StorageEnv* env) : BaseProcessor<cpp2::AdminExecResp>(env) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_ADMIN_ADMINPROCESSOR_H_
