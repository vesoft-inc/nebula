/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

namespace cpp vesoft.raftex
namespace java vesoft.raftex
namespace go vesoft.raftex

cpp_include "base/ThriftTypes.h"

enum ErrorCode {
    SUCCEEDED = 0;

    E_LOG_GAP = -1;
    E_LOG_STALE = -2;
    E_MISSING_COMMIT = -3;
    E_PULLING_SNAPSHOT = -4;    // The follower is pulling a snapshot

    E_UNKNOWN_PART = -5;
    E_TERM_OUT_OF_DATE = -6;
    E_LAST_LOG_TERM_TOO_OLD = -7;
    E_BAD_STATE = -8;
    E_WRONG_LEADER = -9;
    E_WAL_FAIL = -10;
    E_NOT_READY = -11;

    // Local errors
    E_HOST_STOPPED = -12;
    E_HOST_DISCONNECTED = -13;
    E_REQUEST_ONGOING = -14;

    E_EXCEPTION = -20;          // An thrift internal exception was thrown
}

typedef byte (cpp.type = "vesoft::ClusterID") ClusterID
typedef i32 (cpp.type = "vesoft::GraphSpaceID") GraphSpaceID
typedef i32 (cpp.type = "vesoft::PartitionID") PartitionID
typedef i64 (cpp.type = "vesoft::TermID") TermID
typedef i64 (cpp.type = "vesoft::LogID") LogID
typedef i32 (cpp.type = "vesoft::IPv4") IPv4
typedef i32 (cpp.type = "vesoft::Port") Port


// A request to ask for vote
struct AskForVoteRequest {
    1: GraphSpaceID space;              // The graph space id
    2: PartitionID  part;               // The data partition
    3: IPv4         candidate_ip;       // My IP
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
    // Fields 1 - 8 are common for both log appendent and heartbeat
    //
    1: GraphSpaceID space;              // Graphspace ID
    2: PartitionID  part;               // Partition ID
    3: TermID       term;               // Current term
    4: IPv4         leader_ip;          // The leader's IP
    5: Port         leader_port;        // The leader's Port
    6: LogID        committed_log_id;   // Last committed Log ID
    7: TermID       previous_log_term;  // The previous log term
    8: LogID        previous_log_id;    // The previous log id

    //
    // Fields 9 to 11 are used for LogAppend.
    //
    // In the case of heartbeat, the log_str_list will be empty
    //
    // In the case of LogAppend, the id of the first log in the list is
    //   last_log_id + 1
    //
    // All logs in the list must belong to the same term
    //
    9: TermID           log_term;       // The term to receive these logs
    10: LogID           first_log_id;   // The first log id in the list
    11: list<LogEntry>  log_str_list;   // A list of logs

    12: optional binary snapshot_uri;   // Snapshot URL
}


struct AppendLogResponse {
    1: ErrorCode    error_code;
    2: TermID       term;
    3: IPv4         leader_ip;
    4: Port         leader_port;
    5: LogID        committed_log_id;
    6: LogID        last_log_id;
    7: bool         pulling_snapshot;
}


service RaftexService {
    AskForVoteResponse askForVote(1: AskForVoteRequest req);
    AppendLogResponse appendLog(1: AppendLogRequest req);
}


