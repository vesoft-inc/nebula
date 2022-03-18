/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

namespace cpp nebula.raftex

include "common.thrift"

enum Role {
    LEADER      = 1, // the leader
    FOLLOWER    = 2; // following a leader
    CANDIDATE   = 3; // Has sent AskForVote request
    LEARNER     = 4; // same with FOLLOWER, except that it does
                     // not vote in leader election
} (cpp.enum_strict)

enum Status {
    STARTING            = 0; // The part is starting, not ready for service
    RUNNING             = 1; // The part is running
    STOPPED             = 2; // The part has been stopped
    WAITING_SNAPSHOT    = 3; // Waiting for the snapshot.
} (cpp.enum_strict)


typedef i64 (cpp.type = "nebula::ClusterID") ClusterID
typedef i32 (cpp.type = "nebula::GraphSpaceID") GraphSpaceID
typedef i32 (cpp.type = "nebula::PartitionID") PartitionID
typedef i64 (cpp.type = "nebula::TermID") TermID
typedef i64 (cpp.type = "nebula::LogID") LogID
typedef i32 (cpp.type = "nebula::Port") Port


// A request to ask for vote
struct AskForVoteRequest {
    1: GraphSpaceID space;              // The graph space id
    2: PartitionID  part;               // The data partition
    3: string       candidate_addr;     // My Address
    4: Port         candidate_port;     // My port
    5: TermID       term;               // Proposed term
    6: LogID        last_log_id;        // The last received log id
    7: TermID       last_log_term;      // The term receiving the last log
    8: bool         is_pre_vote;        // Is pre vote or not
}


// Response message for the vote call
struct AskForVoteResponse {
    1: common.ErrorCode error_code;
    2: TermID           current_term;
}

// Log entries being sent to follower, logId is not included, it could be calculated by
// last_log_id_sent and offset in log_str_list in AppendLogRequest
struct RaftLogEntry {
    1: ClusterID cluster;
    2: binary    log_str;
    3: TermID    log_term;
}

struct AppendLogRequest {
    1: GraphSpaceID        space;               // Graphspace ID
    2: PartitionID         part;                // Partition ID
    3: TermID              current_term;        // Current term
    4: LogID               committed_log_id;    // Last committed Log ID
    5: string              leader_addr;         // The leader's address
    6: Port                leader_port;         // The leader's Port
    7: TermID              last_log_term_sent;  // Term of log entry preceding log_str_list
    8: LogID               last_log_id_sent;    // Id of log entry preceding log_str_list
    9: list<RaftLogEntry>  log_str_list;        // First log id in log_str_list is last_log_id_sent + 1
}

struct AppendLogResponse {
    1: common.ErrorCode error_code;
    2: TermID           current_term;
    3: string           leader_addr;
    4: Port             leader_port;
    5: LogID            committed_log_id;
    6: LogID            last_matched_log_id;
    7: TermID           last_matched_log_term;
}

struct SendSnapshotRequest {
    1: GraphSpaceID space;
    2: PartitionID  part;
    3: TermID       current_term;
    4: LogID        committed_log_id;
    5: TermID       committed_log_term;
    6: string       leader_addr;
    7: Port         leader_port;
    8: list<binary> rows;
    9: i64          total_size;
    10: i64         total_count;
    11: bool        done;
}

struct HeartbeatRequest {
    1: GraphSpaceID space;              // Graphspace ID
    2: PartitionID  part;               // Partition ID
    3: TermID       current_term;       // Current term
    5: LogID        committed_log_id;   // Last committed Log ID
    6: string       leader_addr;        // The leader's address
    7: Port         leader_port;        // The leader's Port
    8: TermID       last_log_term_sent;
    9: LogID        last_log_id_sent;
}

struct HeartbeatResponse {
    1: common.ErrorCode error_code;
    2: TermID           current_term;
    3: string           leader_addr;
    4: Port             leader_port;
    5: LogID            committed_log_id;
    6: LogID            last_log_id;
    7: TermID           last_log_term;
}

struct SendSnapshotResponse {
    1: common.ErrorCode error_code;
    2: TermID           current_term;
}

struct GetStateRequest {
    1: GraphSpaceID space;              // Graphspace ID
    2: PartitionID  part;               // Partition ID
}

struct GetStateResponse {
    1: common.ErrorCode error_code;
    2: Role             role;
    3: TermID           term;
    4: bool             is_leader;
    5: LogID            committed_log_id;
    6: LogID            last_log_id;
    7: TermID           last_log_term;
    8: Status           status;
    9: list<binary>     peers;
}

service RaftexService {
    AskForVoteResponse askForVote(1: AskForVoteRequest req);
    AppendLogResponse appendLog(1: AppendLogRequest req);
    SendSnapshotResponse sendSnapshot(1: SendSnapshotRequest req);
    HeartbeatResponse heartbeat(1: HeartbeatRequest req) (thread = 'eb');
    GetStateResponse getState(1: GetStateRequest req);
}
