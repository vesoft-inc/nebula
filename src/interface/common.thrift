/* vim: ft=proto
 * Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */


namespace cpp nebula
namespace java com.vesoft.nebula
namespace go nebula
namespace js nebula
namespace csharp nebula
namespace py nebula3.common

cpp_include "common/thrift/ThriftTypes.h"
cpp_include "common/datatypes/DateOps-inl.h"
cpp_include "common/datatypes/VertexOps-inl.h"
cpp_include "common/datatypes/EdgeOps-inl.h"
cpp_include "common/datatypes/PathOps-inl.h"
cpp_include "common/datatypes/ValueOps-inl.h"
cpp_include "common/datatypes/MapOps-inl.h"
cpp_include "common/datatypes/ListOps-inl.h"
cpp_include "common/datatypes/SetOps-inl.h"
cpp_include "common/datatypes/DataSetOps-inl.h"
cpp_include "common/datatypes/KeyValueOps-inl.h"
cpp_include "common/datatypes/HostAddrOps-inl.h"
cpp_include "common/datatypes/GeographyOps-inl.h"
cpp_include "common/datatypes/DurationOps-inl.h"

/*
 *
 *  Note: In order to support multiple languages, all strings
 *        have to be defined as **binary** in the thrift file
 *
 */

const binary (cpp.type = "char const *") version = "3.0.0"

typedef i64 (cpp.type = "nebula::ClusterID") ClusterID
typedef i32 (cpp.type = "nebula::GraphSpaceID") GraphSpaceID
typedef i32 (cpp.type = "nebula::PartitionID") PartitionID
typedef i32 (cpp.type = "nebula::TagID") TagID
typedef i32 (cpp.type = "nebula::EdgeType") EdgeType
typedef i64 (cpp.type = "nebula::EdgeRanking") EdgeRanking
typedef i64 (cpp.type = "nebula::LogID") LogID
typedef i64 (cpp.type = "nebula::TermID") TermID

typedef i64 (cpp.type = "nebula::Timestamp") Timestamp

typedef i32 (cpp.type = "nebula::IndexID") IndexID

typedef i32 (cpp.type = "nebula::Port") Port

typedef i64 (cpp.type = "nebula::SessionID") SessionID

typedef i64 (cpp.type = "nebula::ExecutionPlanID") ExecutionPlanID

union SchemaID {
    1: TagID     tag_id,
    2: EdgeType  edge_type,
}

// !! Struct Date has a shadow data type defined in the Date.h
// So any change here needs to be reflected to the shadow type there
struct Date {
    1: i16 year;    // Calendar year, such as 2019
    2: byte month;    // Calendar month: 1 - 12
    3: byte day;      // Calendar day: 1 -31
} (cpp.type = "nebula::Date")

// !! Struct Time has a shadow data type defined in the Date.h
// So any change here needs to be reflected to the shadow type there
struct Time {
    1: byte hour;         // Hour: 0 - 23
    2: byte minute;       // Minute: 0 - 59
    3: byte sec;          // Second: 0 - 59
    4: i32 microsec;    // Micro-second: 0 - 999,999
} (cpp.type = "nebula::Time")

// !! Struct DateTime has a shadow data type defined in the Date.h
// So any change here needs to be reflected to the shadow type there
struct DateTime {
    1: i16 year;
    2: byte month;
    3: byte day;
    4: byte hour;         // Hour: 0 - 23
    5: byte minute;       // Minute: 0 - 59
    6: byte sec;          // Second: 0 - 59
    7: i32 microsec;    // Micro-second: 0 - 999,999
} (cpp.type = "nebula::DateTime")

enum NullType {
    __NULL__ = 0,
    NaN      = 1,
    BAD_DATA = 2,
    BAD_TYPE = 3,
    ERR_OVERFLOW = 4,
    UNKNOWN_PROP = 5,
    DIV_BY_ZERO = 6,
    OUT_OF_RANGE = 7,
} (cpp.enum_strict, cpp.type = "nebula::NullType")


// The type to hold any supported values during the query
union Value {
    1: NullType                                 nVal;
    2: bool                                     bVal;
    3: i64                                      iVal;
    4: double                                   fVal;
    5: binary                                   sVal;
    6: Date                                     dVal;
    7: Time                                     tVal;
    8: DateTime                                 dtVal;
    9: Vertex (cpp.type = "nebula::Vertex")     vVal (cpp.ref_type = "unique");
    10: Edge (cpp.type = "nebula::Edge")        eVal (cpp.ref_type = "unique");
    11: Path (cpp.type = "nebula::Path")        pVal (cpp.ref_type = "unique");
    12: NList (cpp.type = "nebula::List")       lVal (cpp.ref_type = "unique");
    13: NMap (cpp.type = "nebula::Map")         mVal (cpp.ref_type = "unique");
    14: NSet (cpp.type = "nebula::Set")         uVal (cpp.ref_type = "unique");
    15: DataSet (cpp.type = "nebula::DataSet")  gVal (cpp.ref_type = "unique");
    16: Geography (cpp.type = "nebula::Geography")   ggVal (cpp.ref_type = "unique");
    17: Duration (cpp.type = "nebula::Duration")     duVal (cpp.ref_type = "unique");
} (cpp.type = "nebula::Value")


// Ordered list
struct NList {
    1: list<Value> values;
} (cpp.type = "nebula::List")


// Unordered key/values pairs
struct NMap {
    1: map<binary, Value> (cpp.template = "std::unordered_map") kvs;
} (cpp.type = "nebula::Map")


// Unordered and unique values
struct NSet {
    1: set<Value> (cpp.template = "std::unordered_set") values;
} (cpp.type = "nebula::Set")


struct Row {
    1: list<Value> values;
} (cpp.type = "nebula::Row")


struct DataSet {
    1: list<binary>    column_names;   // Column names
    2: list<Row>       rows;
} (cpp.type = "nebula::DataSet")

struct Coordinate {
    1: double          x;
    2: double          y;
} (cpp.type = "nebula::Coordinate")

struct Point {
    1: Coordinate      coord;
} (cpp.type = "nebula::Point")

struct LineString {
    1: list<Coordinate>             coordList;
} (cpp.type = "nebula::LineString")

struct Polygon {
    1: list<list<Coordinate>>       coordListList;
} (cpp.type = "nebula::Polygon")

union Geography {
    1: Point                                    ptVal (cpp.ref_type = "unique");
    2: LineString                               lsVal (cpp.ref_type = "unique");
    3: Polygon                                  pgVal (cpp.ref_type = "unique");
} (cpp.type = "nebula::Geography")


struct Tag {
    1: binary name,
    // List of <prop_name, prop_value>
    2: map<binary, Value> (cpp.template = "std::unordered_map") props,
} (cpp.type = "nebula::Tag")


struct Vertex {
    1: Value     vid,
    2: list<Tag> tags,
} (cpp.type = "nebula::Vertex")


struct Edge {
    1: Value src,
    2: Value dst,
    3: EdgeType type,
    4: binary name,
    5: EdgeRanking ranking,
    // List of <prop_name, prop_value>
    6: map<binary, Value> (cpp.template = "std::unordered_map") props,
} (cpp.type = "nebula::Edge")


struct Step {
    1: Vertex dst,
    2: EdgeType type,
    3: binary name,
    4: EdgeRanking ranking,
    5: map<binary, Value> (cpp.template = "std::unordered_map") props,
} (cpp.type = "nebula::Step")


// Special type to support path during the query
struct Path {
    1: Vertex src,
    2: list<Step> steps;
} (cpp.type = "nebula::Path")


struct HostAddr {
    // Host could be a valid IPv4 or IPv6 address, or a valid domain name
    1: string   host,
    2: Port     port,
} (cpp.type = "nebula::HostAddr")


struct KeyValue {
    1: binary key,
    2: binary value,
} (cpp.type = "nebula::KeyValue")

// !! Struct Duration has a shadow data type defined in the Duration.h
// So any change here needs to be reflected to the shadow type there
struct Duration {
    1: i64 seconds;
    2: i32 microseconds;
    3: i32 months;
} (cpp.type = "nebula::Duration")

struct LogInfo {
    1: LogID  log_id,
    2: TermID term_id,
    3: LogID  commit_log_id,
    // storage part checkpoint directory name
    4: binary checkpoint_path,
}

struct DirInfo {
    // Installation directory for nebula
    1: binary                   root,
    // nebula's data directory
    2: list<binary>             data,
}

struct CheckpointInfo {
    1: GraphSpaceID          space_id,
    // Only part of the leader
    2: map<PartitionID, LogInfo> (cpp.template = "std::unordered_map") parts,
    // The datapath corresponding to the current checkpointInfo
    3: binary                data_path,
}

// used for drainer
struct LogEntry {
    1: ClusterID cluster;
    2: binary log_str;
}

// These are all data types supported in the graph properties
enum PropertyType {
    UNKNOWN = 0,

    // Simple types
    BOOL = 1,
    INT64 = 2,          // This is the same as INT in v1
    VID = 3,            // Deprecated, only supported by v1
    FLOAT = 4,
    DOUBLE = 5,
    STRING = 6,
    // String with fixed length. If the string content is shorter
    // than the given length, '\0' will be padded to the end
    FIXED_STRING = 7,   // New in v2
    INT8 = 8,           // New in v2
    INT16 = 9,          // New in v2
    INT32 = 10,         // New in v2

    // Date time
    TIMESTAMP = 21,
    DURATION = 23,
    DATE = 24,
    DATETIME = 25,
    TIME = 26,

    // Geo spatial
    GEOGRAPHY = 31,

    //NEW LIST
    LIST_STRING = 32,
    LIST_INT = 33,
    LIST_FLOAT = 34,

    // NEW SET 
    SET_STRING = 35,
    SET_INT = 36,
    SET_FLOAT = 37,

} (cpp.enum_strict)

/*
 * ErrorCode for graphd, metad, storaged,raftd
 * -1xxx for graphd
 * -2xxx for metad
 * -3xxx for storaged
 */
enum ErrorCode {
    // for common code
    SUCCEEDED                         = 0,

    E_DISCONNECTED                    = -1,     // Lost connection
    E_FAIL_TO_CONNECT                 = -2,     // Unable to establish connection
    E_RPC_FAILURE                     = -3,     // RPC failure
    E_LEADER_CHANGED                  = -4,     // Raft leader has been changed

    // only unify metad and storaged error code
    E_SPACE_NOT_FOUND                 = -5,     // Graph space does not exist
    E_TAG_NOT_FOUND                   = -6,     // Tag does not exist
    E_EDGE_NOT_FOUND                  = -7,     // Edge type does not exist
    E_INDEX_NOT_FOUND                 = -8,     // Index does not exist
    E_EDGE_PROP_NOT_FOUND             = -9,     // Edge type property does not exist
    E_TAG_PROP_NOT_FOUND              = -10,    // Tag property does not exist
    E_ROLE_NOT_FOUND                  = -11,    // The current role does not exist
    E_CONFIG_NOT_FOUND                = -12,    // The current configuration does not exist
    E_MACHINE_NOT_FOUND               = -13,    // The current host does not exist
    E_ZONE_NOT_FOUND                  = -14,    // The zone does not exist
    E_LISTENER_NOT_FOUND              = -15,    // Listener does not exist
    E_PART_NOT_FOUND                  = -16,    // The current partition does not exist
    E_KEY_NOT_FOUND                   = -17,    // Key does not exist
    E_USER_NOT_FOUND                  = -18,    // User does not exist
    E_STATS_NOT_FOUND                 = -19,    // Statistics do not exist
    E_SERVICE_NOT_FOUND               = -20,    // No current service found
    E_DRAINER_NOT_FOUND               = -21,    // Drainer does not exist[only ent]
    E_DRAINER_CLIENT_NOT_FOUND        = -22,    // Drainer client does not exist[only ent]
    E_PART_STOPPED                    = -23,    // The current partition has already been stopped[only ent]

    // backup failed
    E_BACKUP_FAILED                   = -24,    // Backup failed
    E_BACKUP_EMPTY_TABLE              = -25,    // The backed-up table is empty
    E_BACKUP_TABLE_FAILED             = -26,    // Table backup failure
    E_PARTIAL_RESULT                  = -27,    // MultiGet could not get all data
    E_REBUILD_INDEX_FAILED            = -28,    // Index rebuild failed
    E_INVALID_PASSWORD                = -29,    // Password is invalid
    E_FAILED_GET_ABS_PATH             = -30,    // Unable to get absolute path
    E_LISTENER_PROGRESS_FAILED        = -31,    // Get listener sync progress failed[only ent]
    E_SYNC_LISTENER_NOT_FOUND         = -32,    // Sync Listener does not exist[only ent]
    E_DRAINER_PROGRESS_FAILED         = -33,    // Get drainer sync progress failed[only ent]

    E_PART_DISABLED                   = -34,    // [only ent]
    E_PART_ALREADY_STARTED            = -35,    // [only ent]
    E_PART_ALREADY_STOPPED            = -36,    // [only ent]

    // 1xxx for graphd
    E_BAD_USERNAME_PASSWORD           = -1001,  // Authentication failed
    E_SESSION_INVALID                 = -1002,  // Invalid session
    E_SESSION_TIMEOUT                 = -1003,  // Session timeout
    E_SYNTAX_ERROR                    = -1004,  // Syntax error
    E_EXECUTION_ERROR                 = -1005,  // Execution error
    E_STATEMENT_EMPTY                 = -1006,  // Statement is empty

    E_BAD_PERMISSION                  = -1008,  // Wrong permission
    E_SEMANTIC_ERROR                  = -1009,  // Semantic error
    E_TOO_MANY_CONNECTIONS            = -1010,  // Maximum number of connections exceeded
    E_PARTIAL_SUCCEEDED               = -1011,  // Access to storage failed (only some requests succeeded)

    // 2xxx for metad
    E_NO_HOSTS                        = -2001,  // Host does not exist
    E_EXISTED                         = -2002,  // Host already exists
    E_INVALID_HOST                    = -2003,  // Invalid host
    E_UNSUPPORTED                     = -2004,  // The current command, statement, or function is not supported
    E_NOT_DROP                        = -2005,  // Not allowed to drop
    E_BALANCER_RUNNING                = -2006,  // The balancer is running
    E_CONFIG_IMMUTABLE                = -2007,  // Configuration items cannot be changed
    E_CONFLICT                        = -2008,  // Parameters conflict with meta data
    E_INVALID_PARM                    = -2009,  // Invalid parameter
    E_WRONGCLUSTER                    = -2010,  // Wrong cluster
    E_ZONE_NOT_ENOUGH                 = -2011,  // Host is not enough
    E_ZONE_IS_EMPTY                   = -2012,  // Host does not exist
    E_LISTENER_CONFLICT               = -2013,  // Listener conflicts[only ent]
    E_SCHEMA_NAME_EXISTS              = -2014,  // Schema name already exists
    E_RELATED_INDEX_EXISTS            = -2015,  // There are still indexes related to tag or edge, cannot drop it
    E_RELATED_SPACE_EXISTS            = -2016,  // There are still some space on the host, cannot drop it
    E_RELATED_FULLTEXT_INDEX_EXISTS   = -2017,  // There are still fulltext index on tag/edge
    E_HISTORY_CONFLICT                = -2018,  // Existed before (e.g., schema)

    E_STORE_FAILURE                   = -2021,  // Failed to store data
    E_STORE_SEGMENT_ILLEGAL           = -2022,  // Illegal storage segment
    E_BAD_BALANCE_PLAN                = -2023,  // Invalid data balancing plan
    E_BALANCED                        = -2024,  // The cluster is already in the data balancing status
    E_NO_RUNNING_BALANCE_PLAN         = -2025,  // There is no running data balancing plan
    E_NO_VALID_HOST                   = -2026,  // Lack of valid hosts
    E_CORRUPTED_BALANCE_PLAN          = -2027,  // A data balancing plan that has been corrupted
    E_NO_INVALID_BALANCE_PLAN         = -2028,  // No invalid balance plan
    E_NO_VALID_DRAINER                = -2029,  // Lack of valid drainers[only ent]


    // Authentication Failure
    E_IMPROPER_ROLE                   = -2030,  // Failed to recover user role
    E_INVALID_PARTITION_NUM           = -2031,  // Number of invalid partitions
    E_INVALID_REPLICA_FACTOR          = -2032,  // Invalid replica factor
    E_INVALID_CHARSET                 = -2033,  // Invalid character set
    E_INVALID_COLLATE                 = -2034,  // Invalid character sorting rules
    E_CHARSET_COLLATE_NOT_MATCH       = -2035,  // Character set and character sorting rule mismatch
    E_PRIVILEGE_ALL_TAG_EDGE_SETTLED  = -2036,  // drop all tag/edge before do some grant/revoke[only ent]
    E_PRIVILEGE_NOT_EXIST             = -2037,  // remove un-exist privilege[only ent]
    E_PRIVILEGE_NEED_BASIC_ROLE       = -2038,  // only basic role support tag/edge privilege[only ent]
    E_PRIVILEGE_ACTION_INVALID        = -2039,  // only add and drop now.[only ent]

    // Admin Failure
    E_SNAPSHOT_FAILURE                = -2040,  // Failed to generate a snapshot
    E_SNAPSHOT_RUNNING_JOBS           = -2056,  // Failed to generate a snapshot because encounter running jobs
    E_SNAPSHOT_NOT_FOUND              = -2057,  // Snapshot not found when try to drop it
    E_BLOCK_WRITE_FAILURE             = -2041,  // Failed to write block data
    E_REBUILD_INDEX_FAILURE           = -2042,
    E_INDEX_WITH_TTL                  = -2043,  
    E_ADD_JOB_FAILURE                 = -2044,  // Failed to add new task
    E_STOP_JOB_FAILURE                = -2045,  // Failed to stop task
    E_SAVE_JOB_FAILURE                = -2046,  // Failed to save task information
    E_BALANCER_FAILURE                = -2047,  // Data balancing failed
    E_JOB_NOT_FINISHED                = -2048,  // The current task has not been completed
    E_TASK_REPORT_OUT_DATE            = -2049,  // Task report failed
    E_JOB_NOT_IN_SPACE                = -2050,  // The current task is not in the graph space
    E_JOB_NEED_RECOVER                = -2051,  // The current task needs to be resumed
    E_JOB_ALREADY_FINISH              = -2052,  // The job status has already been failed or finished
    E_JOB_SUBMITTED                   = -2053,  // Job default status.
    E_JOB_NOT_STOPPABLE               = -2054,  // The given job do not support stop
    E_JOB_HAS_NO_TARGET_STORAGE       = -2055,  // The leader distribution has not been reported, so can't send task to storage
    E_INVALID_JOB                     = -2065,  // Invalid task

    // Backup Failure
    E_BACKUP_RUNNING_JOBS             = -2066,  // Backup terminated (some data modification jobs running)
    E_BACKUP_SPACE_NOT_FOUND          = -2067,  // Graph space does not exist at the time of backup

    // RESTORE Failure
    E_RESTORE_FAILURE                 = -2068,  // Backup recovery failed

    E_SESSION_NOT_FOUND               = -2069,  // Session does not exist

    // ListClusterInfo Failure
    E_LIST_CLUSTER_FAILURE              = -2070,    // Failed to get cluster information
    E_LIST_CLUSTER_GET_ABS_PATH_FAILURE = -2071,    // Failed to get absolute path when getting cluster information
    E_LIST_CLUSTER_NO_AGENT_FAILURE     = -2072,    // Unable to get an agent when getting cluster information

    E_QUERY_NOT_FOUND                 = -2073,  // Query not found
    E_AGENT_HB_FAILUE                 = -2074,  // Failed to receive heartbeat from agent

    E_INVALID_VARIABLE                = -2080,  // [only ent]
    E_VARIABLE_TYPE_VALUE_MISMATCH    = -2081,  // [only ent]
    E_HOST_CAN_NOT_BE_ADDED           = -2082,  // the host can not be added for it's not a storage host

    E_ACCESS_ES_FAILURE               = -2090,  // Failed to access elasticsearch

    E_GRAPH_MEMORY_EXCEEDED           = -2600,  // Graph memory exceeded

    // 3xxx for storaged
    E_CONSENSUS_ERROR                 = -3001,  // Consensus cannot be reached during an election
    E_KEY_HAS_EXISTS                  = -3002,  // Key already exists
    E_DATA_TYPE_MISMATCH              = -3003,  // Data type mismatch
    E_INVALID_FIELD_VALUE             = -3004,  // Invalid field value
    E_INVALID_OPERATION               = -3005,  // Invalid operation
    E_NOT_NULLABLE                    = -3006,  // Current value is not allowed to be empty
    E_FIELD_UNSET                     = -3007,  // Field value must be set if the field value is NOT NULL or has no default value
    E_OUT_OF_RANGE                    = -3008,  // The value is out of the range of the current type
    E_DATA_CONFLICT_ERROR             = -3010,  // Data conflict, for index write without toss.

    E_WRITE_STALLED                   = -3011,  // Writes are delayed

    // meta failures
    E_IMPROPER_DATA_TYPE              = -3021,  // Incorrect data type
    E_INVALID_SPACEVIDLEN             = -3022,  // Invalid VID length

    // Invalid request
    E_INVALID_FILTER                  = -3031,  // Invalid filter
    E_INVALID_UPDATER                 = -3032,  // Invalid field update
    E_INVALID_STORE                   = -3033,  // Invalid KV storage
    E_INVALID_PEER                    = -3034,  // Peer invalid
    E_RETRY_EXHAUSTED                 = -3035,  // Out of retries
    E_TRANSFER_LEADER_FAILED          = -3036,  // Leader change failed
    E_INVALID_STAT_TYPE               = -3037,  // Invalid stat type
    E_INVALID_VID                     = -3038,  // VID is invalid
    E_NO_TRANSFORMED                  = -3039,  

    // meta client failed
    E_LOAD_META_FAILED                = -3040,  // Failed to load meta information

    // checkpoint failed
    E_FAILED_TO_CHECKPOINT            = -3041,  // Failed to generate checkpoint
    E_CHECKPOINT_BLOCKED              = -3042,  // Generating checkpoint is blocked

    // Filter out
    E_FILTER_OUT                      = -3043,  // Data is filtered
    E_INVALID_DATA                    = -3044,  // Invalid data

    E_MUTATE_EDGE_CONFLICT            = -3045,  // Concurrent write conflicts on the same edge
    E_MUTATE_TAG_CONFLICT             = -3046,  // Concurrent write conflict on the same vertex

    // transaction
    E_OUTDATED_LOCK                   = -3047,  // Lock is invalid

    // task manager failed
    E_INVALID_TASK_PARA               = -3051,  // Invalid task parameter
    E_USER_CANCEL                     = -3052,  // The user canceled the task
    E_TASK_EXECUTION_FAILED           = -3053,  // Task execution failed
    E_PLAN_IS_KILLED                  = -3060,  // Execution plan was cleared

    // toss
    E_NO_TERM                         = -3070,  // The heartbeat process was not completed when the request was received
    E_OUTDATED_TERM                   = -3071,  // Out-of-date heartbeat received from the old leader (the new leader has been elected)
    E_OUTDATED_EDGE                   = -3072,  
    E_WRITE_WRITE_CONFLICT            = -3073,  // Concurrent write conflicts with later requests

    E_CLIENT_SERVER_INCOMPATIBLE      = -3061,  // Client and server versions are not compatible
    E_ID_FAILED                       = -3062,  // Failed to get ID serial number

    // 35xx for storaged raft
    E_RAFT_UNKNOWN_PART               = -3500,  // Unknown partition

    // Raft consensus errors
    E_RAFT_LOG_GAP                    = -3501,  // Raft logs lag behind
    E_RAFT_LOG_STALE                  = -3502,  // Raft logs are out of date
    E_RAFT_TERM_OUT_OF_DATE           = -3503,  // Heartbeat messages are out of date
    E_RAFT_UNKNOWN_APPEND_LOG         = -3504,  // Unknown additional logs

    // Raft state errors
    E_RAFT_WAITING_SNAPSHOT           = -3511,  // Waiting for the snapshot to complete
    E_RAFT_SENDING_SNAPSHOT           = -3512,  // There was an error sending the snapshot
    E_RAFT_INVALID_PEER               = -3513,  // Invalid receiver
    E_RAFT_NOT_READY                  = -3514,  // Raft did not start
    E_RAFT_STOPPED                    = -3515,  // Raft has stopped
    E_RAFT_BAD_ROLE                   = -3516,  // Wrong role

    // Local errors
    E_RAFT_WAL_FAIL                   = -3521,  // Write to a WAL failed
    E_RAFT_HOST_STOPPED               = -3522,  // The host has stopped
    E_RAFT_TOO_MANY_REQUESTS          = -3523,  // Too many requests
    E_RAFT_PERSIST_SNAPSHOT_FAILED    = -3524,  // Persistent snapshot failed
    E_RAFT_RPC_EXCEPTION              = -3525,  // RPC exception
    E_RAFT_NO_WAL_FOUND               = -3526,  // No WAL logs found
    E_RAFT_HOST_PAUSED                = -3527,  // Host suspended
    E_RAFT_WRITE_BLOCKED              = -3528,  // Writes are blocked
    E_RAFT_BUFFER_OVERFLOW            = -3529,  // Cache overflow
    E_RAFT_ATOMIC_OP_FAILED           = -3530,  // Atomic operation failed
    E_LEADER_LEASE_FAILED             = -3531,  // Leader lease expired
    E_RAFT_CAUGHT_UP                  = -3532,  // Data has been synchronized on Raft[only ent]
    
    // 4xxx for drainer
    E_LOG_GAP                         = -4001,  // Drainer logs lag behind[only ent]
    E_LOG_STALE                       = -4002,  // Drainer logs are out of date[only ent]
    E_INVALID_DRAINER_STORE           = -4003,  // The drainer data storage is invalid[only ent]
    E_SPACE_MISMATCH                  = -4004,  // Graph space mismatch[only ent]
    E_PART_MISMATCH                   = -4005,  // Partition mismatch[only ent]
    E_DATA_CONFLICT                   = -4006,  // Data conflict[only ent]
    E_REQ_CONFLICT                    = -4007,  // Request conflict[only ent]
    E_DATA_ILLEGAL                    = -4008,  // Illegal data[only ent]

    // 5xxx for cache
    E_CACHE_CONFIG_ERROR              = -5001,  // Cache configuration error[only ent]
    E_NOT_ENOUGH_SPACE                = -5002,  // Insufficient space[only ent]
    E_CACHE_MISS                      = -5003,  // No cache hit[only ent]
    E_POOL_NOT_FOUND                  = -5004,  // [only ent]
    E_CACHE_WRITE_FAILURE             = -5005,  // Write cache failed[only ent]

    // 7xxx for nebula enterprise
    // license related
    E_NODE_NUMBER_EXCEED_LIMIT        = -7001,  // Number of machines exceeded the limit[only ent]
    E_PARSING_LICENSE_FAILURE         = -7002,  // Failed to resolve certificate[only ent]

    E_STORAGE_MEMORY_EXCEEDED         = -3600,  // Storage memory exceeded

    E_UNKNOWN                         = -8000,  // Unknown error
} (cpp.enum_strict)
