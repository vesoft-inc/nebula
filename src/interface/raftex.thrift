/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

namespace cpp nebula.raftex

cpp_include "common/thrift/ThriftTypes.h"

enum ErrorCode {
    SUCCEEDED = 0;

    E_LOG_GAP = -1;
    E_LOG_STALE = -2;
    E_MISSING_COMMIT = -3;
    E_WAITING_SNAPSHOT = -4;    // The follower is waiting a snapshot

    E_UNKNOWN_PART = -5;
    E_TERM_OUT_OF_DATE = -6;
    E_LAST_LOG_TERM_TOO_OLD = -7;
    E_BAD_STATE = -8;
    E_WRONG_LEADER = -9;
    E_WAL_FAIL = -10;
    E_NOT_READY = -11;

    // Local errors
    E_HOST_STOPPED = -12;
    E_NOT_A_LEADER = -13;
    E_HOST_DISCONNECTED = -14;
    E_TOO_MANY_REQUESTS = -15;
    E_PERSIST_SNAPSHOT_FAILED = -16;

    E_BAD_ROLE = -17,

    E_EXCEPTION = -20;          // An thrift internal exception was thrown
}

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
}


// Response message for the vote call
struct AskForVoteResponse {
    1: ErrorCode error_code;
}


struct LogEntry {
    1: ClusterID cluster;
    2: binary log_str;
}


/*
  AppendLogRequest serves two purposes:

  1) Send a log message to followers and listeners
  2) Or, when log_id == 0 and len(log_str) == 0, it serves as a heartbeat
*/
struct AppendLogRequest {
    //
    // Fields 1 - 9 are common for both log appendent and heartbeat
    //
    // last_log_term_sent and last_log_id_sent are the term and log id
    // for the last log being sent
    //
    1: GraphSpaceID space;              // Graphspace ID
    2: PartitionID  part;               // Partition ID
    3: TermID       current_term;       // Current term
    4: LogID        last_log_id;        // Last received log id
    5: LogID        committed_log_id;   // Last committed Log ID
    6: string       leader_addr;        // The leader's address
    7: Port         leader_port;        // The leader's Port
    8: TermID       last_log_term_sent;
    9: LogID        last_log_id_sent;

    // [deprecated]: heartbeat is moved to a separate interface
    //
    // Fields 10 to 11 are used for LogAppend.
    //
    //
    // In the case of LogAppend, the id of the first log is the
    //      last_log_id_sent + 1
    //
    // All logs in the log_str_list must belong to the same term,
    // which specified by log_term
    //
    10: TermID log_term;
    11: list<LogEntry> log_str_list;

    12: bool sending_snapshot;
}


struct AppendLogResponse {
    1: ErrorCode    error_code;
    2: TermID       current_term;
    3: string       leader_addr;
    4: Port         leader_port;
    5: LogID        committed_log_id;
    6: LogID        last_log_id;
    7: TermID       last_log_term;
}

struct SendSnapshotRequest {
    1: GraphSpaceID space;
    2: PartitionID  part;
    3: TermID       term;
    4: LogID        committed_log_id;
    5: TermID       committed_log_term;
    6: string       leader_addr;
    7: Port         leader_port;
    8: list<binary> rows;
    9: i64          total_size;
    10: i64          total_count;
    11: bool         done;
}

struct HeartbeatRequest {
    //
    // Fields 1 - 9 are common for both log appendent and heartbeat
    //
    // last_log_term_sent and last_log_id_sent are the term and log id
    // for the last log being sent
    //
    1: GraphSpaceID space;              // Graphspace ID
    2: PartitionID  part;               // Partition ID
    3: TermID       current_term;       // Current term
    4: LogID        last_log_id;        // Last received log id
    5: LogID        committed_log_id;   // Last committed Log ID
    6: string       leader_addr;        // The leader's address
    7: Port         leader_port;        // The leader's Port
    8: TermID       last_log_term_sent;
    9: LogID        last_log_id_sent;
}

struct HeartbeatResponse {
    1: ErrorCode    error_code;
    2: TermID       current_term;
    3: string       leader_addr;
    4: Port         leader_port;
    5: LogID        committed_log_id;
    6: LogID        last_log_id;
    7: TermID       last_log_term;
}

struct SendSnapshotResponse {
    1: ErrorCode    error_code;
}

service RaftexService {
    AskForVoteResponse askForVote(1: AskForVoteRequest req);
    AppendLogResponse appendLog(1: AppendLogRequest req);
    SendSnapshotResponse sendSnapshot(1: SendSnapshotRequest req);
    HeartbeatResponse heartbeat(1: HeartbeatRequest req) (thread = 'eb');
}
