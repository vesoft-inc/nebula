/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

namespace cpp vesoft.vgraph.raftex
namespace java vesoft.vgraph.raftex
namespace go vesoft.vgraph.raftex


enum ResultCode {
    SUCCEEDED = 0;
    E_LOG_GAP = -1;
    E_LOG_STALE = -2;
    E_MISSING_COMMIT = -3;
    E_PULLING_SNAPSHOT = -4;    // The follower is pulling a snapshot

    E_BAD_GROUP_ID = -5;
    E_BAD_PART_ID = -6;
    E_TERM_OUT_OF_DATE = -7;
    E_BAD_STATE = -8;
    E_WRONG_LEADER = -9;
    E_WAL_FAIL = -10;
    E_NOT_READY = -11;

    // Local errors
    E_HOST_STOPPED = -12;
    E_HOST_DISCONNECTED = -13;

    E_EXCEPTION = -20;          // An thrift internal exception was thrown
}

typedef i32 GraphSpaceID
typedef i32 PartitionID
typedef i64 TermID
typedef i64 LogID
typedef i32 IPv4
typedef i32 Port


// A request to ask for vote
struct AskForVoteRequest {
    1: GraphSpaceID space;           // My graph space id
    2: PartitionID  partition;       // The data partition
    3: IPv4         candidateIp;     // My IP
    4: Port         candidatePort;   // My port
    5: TermID       term;            // Proposed term (current term + 1)
    6: LogID        committedLogId;  // My last committed log id
    7: LogID        lastLogId;       // My last received log id
}


// Response message for the vote call
struct AskForVoteResponse {
    1: ResultCode result;
}


struct LogEntry {
    1: bool sendToListenersToo;
    2: binary logStr;
}


/*
  AppendLogRequest serves two purposes:

  1) Send a log message to followers and listeners
  2) Or, when logId == 0 and len(logStr) == 0, it serves as a heartbeat
*/
struct AppendLogRequest {
    // Fields 1 - 7 are common for both log appendent and heartbeat
    1: GraphSpaceID space;
    2: PartitionID  partition;
    3: TermID       term;
    4: IPv4         leaderIp;
    5: Port         leaderPort;
    6: LogID        committedLogId;

    // This is the id of the first log in the current term
    7: LogID firstLogInTerm;

    // Fields 8 and 9 are used for LogAppend.
    //
    // In the case of heartbeat, firstLogId will be set zero and
    // the logStrList will be empty
    //
    // In the case of LogAppend, firstLogId is the id for the first log
    // in logStrList
    8: LogID firstLogId;
    9: list<LogEntry> logStrList;

    // URI for snapshot
    10: binary snapshotURI;
}


struct AppendLogResponse {
    1: ResultCode   result;
    2: TermID       term;
    3: IPv4         leaderIp;
    4: Port         leaderPort;
    5: LogID        committedLogId;
    6: LogID        lastLogId;
    7: bool         pullingSnapshot;
}


service RaftexService {
    AskForVoteResponse askForVote(1: AskForVoteRequest req);
    AppendLogResponse appendLog(1: AppendLogRequest req);
}


