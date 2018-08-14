/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef CONSENSUS_REPLICA_H_
#define CONSENSUS_REPLICA_H_

#include "base/Base.h"

namespace vesoft {
namespace vgraph {
namespace consensus {

class Replica {
public:
    Replica(cpp2::PartitionID partId,
            cpp2::IPv4 localIp,
            cpp2::Port localPort,
            const folly::StringPiece walRoot);
    virtual ~Replica();

    void start();
    void stop();

    void addReplica(const folly::StringPiece addr, cpp2::Port port);

    /*****************************************************
     *
     * Methods to process incoming consensus requests
     *
     ****************************************************/
    // Process the incoming leader selection request
    void processAskForVoteRequest(std::unique_ptr<cpp2::AskForVoteRequest> req,
                                  cpp2::AskForVoteResponse& resp);

    // Process appendLog request
    void processAppendLogRequest(std::unique_ptr<cpp2::AppendLogRequest> req,
                                 cpp2::AppendLogResponse& resp);

    /********************************************************
     *
     * Public Log Append API, used by the consensus caller
     *
     *******************************************************/
    // Asynchronously append a log.
    //
    // The method will take the ownership of the log and returns as soon as
    // possible. Internally it will asynchronously try to send the log to
    // all followers. It will keep trying until majority of followers accept
    // the log, then the future will be fulfilled
    folly::Future<AppendLogResult> appendLogAsync(std::string& logMsg,
                                                  uint32_t timeoutInMs = 0,
                                                  bool sendToListenersToo = true);

protected:
    // This method is called when my leader term is finished, either by
    // receiving a new leader election request, or a new leader heartbeat.
    virtual void onLostLeadership(cpp2::TermID term) = 0;

    // This method is called when I'm elected as a new leader.
    virtual void onElected(cpp2::TermID term) = 0;

    // The subclass implements this method to commit a batch of log messages
    virtual bool commitLogs(int64_t firstLogId,
                            std::vector<std::string>&& logMsgs) = 0;

private:
    enum class Role : uint64_t {
        LEADER = 1,     // I'm the leader
        FOLLOWER,       // I'm following a leader
        CANDIDATE       // I'm waiting to be elected
    };
#define GET_MY_ROLE()       \
    static_cast<Role>((myRoleSig_.load() & 0x00000000FFFFFFFF))
#define GET_ROLE(roleSig)   static_cast<Role>((roleSig & 0x00000000FFFFFFFF))
// SET_ROLE should always be protected by consensusLock_
#define SET_MY_ROLE(oldRoleSig, setToRole) \
    myRoleSig_.compare_exchange_strong( \
        oldRoleSig, \
        ((oldRoleSig & 0xFFFFFFFF00000000) + 0x0000000100000000) | \
            static_cast<uint64_t>(setToRole))

    enum class Status {
        STARTING = 0,           // The part is starting, not ready for service
        RUNNING,                // The part is running
        STOPPED                 // The part has been stopped
    };

    // My current role signature
    // The higher 32 bits are ABA, the lower 32 bits are the current role
    //
    // NOTE: The change of the role signature has to be protected by the
    // consensusLock_
    std::atomic<uint64_t> roleSig_{0};

    /*******************************************************************
     *                                                                 *
     * Exclusive access Section                                        *
     *                                                                 *
     * Access to all members defined in this section should be guarded *
     * by ##consensusLock_##                                           *
     *                                                                 *
     *                                                                 */
    mutable std::mutex consensusLock_;

    // All my peers.
    // TODO(sye) For now, we assume the peer list is unchangeable. But this
    // could be changed when the cluster grows or shrinks. We will handle it
    // in the future
    std::unordered_map<std::pair<std::string, uint16_t>,
                       std::unique_ptr<Host<LogType, Codec>>
    > peerHosts_;
    using HostRefType = typename decltype(peerHosts_)::value_type;

    std::unordered_map<std::string, std::shared_ptr<Listener>> listeners_;
    using ListenerRefType = typename decltype(listeners_)::value_type;

    // When I'm the leader, the leaderIp_ and leaderPort_ is same
    // as localIp_ and localPort_
    cpp2::IPv4 leaderIp_{0};
    cpp2::Port leaderPort_{0};

    // The current term id
    //
    // When I become a candidate, termId will be bumped up by 1
    // When I voted for someone, this will be set to the term id proposed
    // by that candidate
    cpp2::TermID term_{0};

    // The id for the last received log
    cpp2::LogID lastLogId_{0};
    // The id for the last globally committed log (synced from the leader)
    cpp2::LogID committedLogId_{0};
    // The id for the last locally committed log
    cpp2::LogID myLastCommittedLogId_{0};
    /*                                                                *
     *                                                                *
     * End of Exclusive access Section                                *
     *                                                                *
     ******************************************************************/
};

}  // namespace consensus
}  // namespace vgraph
}  // namespace vesoft
#endif  // CONSENSUS_REPLICA_H_
