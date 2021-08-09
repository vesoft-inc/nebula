/* vim: ft=proto
 * Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


namespace cpp nebula
namespace java com.vesoft.nebula
namespace go nebula
namespace js nebula
namespace csharp nebula
namespace py nebula2.common

cpp_include "common/thrift/ThriftTypes.h"
cpp_include "common/datatypes/DateOps.inl"
cpp_include "common/datatypes/VertexOps.inl"
cpp_include "common/datatypes/EdgeOps.inl"
cpp_include "common/datatypes/PathOps.inl"
cpp_include "common/datatypes/ValueOps.inl"
cpp_include "common/datatypes/MapOps.inl"
cpp_include "common/datatypes/ListOps.inl"
cpp_include "common/datatypes/SetOps.inl"
cpp_include "common/datatypes/DataSetOps.inl"
cpp_include "common/datatypes/KeyValueOps.inl"
cpp_include "common/datatypes/HostAddrOps.inl"

/*
 *
 *  Note: In order to support multiple languages, all strings
 *        have to be defined as **binary** in the thrift file
 *
 */

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

struct LogInfo {
    1: LogID  log_id;
    2: TermID term_id;
}

struct DirInfo {
    // Installation directory for nebula
    1: binary                   root,
    // nebula's data directory
    2: list<binary>             data,
}

struct NodeInfo {
    1: HostAddr      host,
    2: DirInfo       dir,
}

struct PartitionBackupInfo {
    1: map<PartitionID, LogInfo> (cpp.template = "std::unordered_map")  info,
}

struct CheckpointInfo {
    1: PartitionBackupInfo   partition_info,
    // storage checkpoint directory name
    2: binary                path,
}

/*
 * ErrorCode for graphd, metad, storaged,raftd
 * -1xxx for graphd
 * -2xxx for metad
 * -3xxx for storaged
 */
enum ErrorCode {
    // for common code
    SUCCEEDED                         = 0,

    E_DISCONNECTED                    = -1,        // RPC Failure
    E_FAIL_TO_CONNECT                 = -2,
    E_RPC_FAILURE                     = -3,
    E_LEADER_CHANGED                  = -4,


    // only unify metad and storaged error code
    E_SPACE_NOT_FOUND                 = -5,
    E_TAG_NOT_FOUND                   = -6,
    E_EDGE_NOT_FOUND                  = -7,
    E_INDEX_NOT_FOUND                 = -8,
    E_EDGE_PROP_NOT_FOUND             = -9,
    E_TAG_PROP_NOT_FOUND              = -10,
    E_ROLE_NOT_FOUND                  = -11,
    E_CONFIG_NOT_FOUND                = -12,
    E_GROUP_NOT_FOUND                 = -13,
    E_ZONE_NOT_FOUND                  = -14,
    E_LISTENER_NOT_FOUND              = -15,
    E_PART_NOT_FOUND                  = -16,
    E_KEY_NOT_FOUND                   = -17,
    E_USER_NOT_FOUND                  = -18,

    // backup failed
    E_BACKUP_FAILED                   = -24,
    E_BACKUP_EMPTY_TABLE              = -25,
    E_BACKUP_TABLE_FAILED             = -26,
    E_PARTIAL_RESULT                  = -27,
    E_REBUILD_INDEX_FAILED            = -28,
    E_INVALID_PASSWORD                = -29,
    E_FAILED_GET_ABS_PATH             = -30,


    // 1xxx for graphd
    E_BAD_USERNAME_PASSWORD           = -1001,     // Authentication error
    E_SESSION_INVALID                 = -1002,     // Execution errors
    E_SESSION_TIMEOUT                 = -1003,
    E_SYNTAX_ERROR                    = -1004,
    E_EXECUTION_ERROR                 = -1005,
    E_STATEMENT_EMPTY                 = -1006,     // Nothing is executed When command is comment

    E_BAD_PERMISSION                  = -1008,
    E_SEMANTIC_ERROR                  = -1009,     // semantic error
    E_TOO_MANY_CONNECTIONS            = -1010,     // Exceeding the maximum number of connections
    E_PARTIAL_SUCCEEDED               = -1011,


    // 2xxx for metad
    E_NO_HOSTS                        = -2001,     // Operation Failure
    E_EXISTED                         = -2002,
    E_INVALID_HOST                    = -2003,
    E_UNSUPPORTED                     = -2004,
    E_NOT_DROP                        = -2005,
    E_BALANCER_RUNNING                = -2006,
    E_CONFIG_IMMUTABLE                = -2007,
    E_CONFLICT                        = -2008,
    E_INVALID_PARM                    = -2009,
    E_WRONGCLUSTER                    = -2010,

    E_STORE_FAILURE                   = -2021,
    E_STORE_SEGMENT_ILLEGAL           = -2022,
    E_BAD_BALANCE_PLAN                = -2023,
    E_BALANCED                        = -2024,
    E_NO_RUNNING_BALANCE_PLAN         = -2025,
    E_NO_VALID_HOST                   = -2026,
    E_CORRUPTTED_BALANCE_PLAN         = -2027,
    E_NO_INVALID_BALANCE_PLAN         = -2028,


    // Authentication Failure
    E_IMPROPER_ROLE                   = -2030,
    E_INVALID_PARTITION_NUM           = -2031,
    E_INVALID_REPLICA_FACTOR          = -2032,
    E_INVALID_CHARSET                 = -2033,
    E_INVALID_COLLATE                 = -2034,
    E_CHARSET_COLLATE_NOT_MATCH       = -2035,

    // Admin Failure
    E_SNAPSHOT_FAILURE                = -2040,
    E_BLOCK_WRITE_FAILURE             = -2041,
    E_REBUILD_INDEX_FAILURE           = -2042,
    E_INDEX_WITH_TTL                  = -2043,
    E_ADD_JOB_FAILURE                 = -2044,
    E_STOP_JOB_FAILURE                = -2045,
    E_SAVE_JOB_FAILURE                = -2046,
    E_BALANCER_FAILURE                = -2047,
    E_JOB_NOT_FINISHED                = -2048,
    E_TASK_REPORT_OUT_DATE            = -2049,
    E_INVALID_JOB                     = -2065,

    // Backup Failure
    E_BACKUP_BUILDING_INDEX           = -2066,
    E_BACKUP_SPACE_NOT_FOUND          = -2067,

    // RESTORE Failure
    E_RESTORE_FAILURE                 = -2068,

    E_SESSION_NOT_FOUND               = -2069,

    // ListClusterInfo Failure
    E_LIST_CLUSTER_FAILURE              = -2070,
    E_LIST_CLUSTER_GET_ABS_PATH_FAILURE = -2071,
    E_GET_META_DIR_FAILURE              = -2072,

    E_QUERY_NOT_FOUND                 = -2073,

    // 3xxx for storaged
    E_CONSENSUS_ERROR                 = -3001,
    E_KEY_HAS_EXISTS                  = -3002,
    E_DATA_TYPE_MISMATCH              = -3003,
    E_INVALID_FIELD_VALUE             = -3004,
    E_INVALID_OPERATION               = -3005,
    E_NOT_NULLABLE                    = -3006,     // Not allowed to be null
    // The field neither can be NULL, nor has a default value
    E_FIELD_UNSET                     = -3007,
    // Value exceeds the range of type
    E_OUT_OF_RANGE                    = -3008,
    // Atomic operation failed
    E_ATOMIC_OP_FAILED                = -3009,
    E_DATA_CONFLICT_ERROR             = -3010, // data conflict, for index write without toss.

    E_WRITE_STALLED                   = -3011,

    // meta failures
    E_IMPROPER_DATA_TYPE              = -3021,
    E_INVALID_SPACEVIDLEN             = -3022,

    // Invalid request
    E_INVALID_FILTER                  = -3031,
    E_INVALID_UPDATER                 = -3032,
    E_INVALID_STORE                   = -3033,
    E_INVALID_PEER                    = -3034,
    E_RETRY_EXHAUSTED                 = -3035,
    E_TRANSFER_LEADER_FAILED          = -3036,
    E_INVALID_STAT_TYPE               = -3037,
    E_INVALID_VID                     = -3038,
    E_NO_TRANSFORMED                  = -3039,

    // meta client failed
    E_LOAD_META_FAILED                = -3040,

    // checkpoint failed
    E_FAILED_TO_CHECKPOINT            = -3041,
    E_CHECKPOINT_BLOCKED              = -3042,

    // Filter out
    E_FILTER_OUT                      = -3043,
    E_INVALID_DATA                    = -3044,

    E_MUTATE_EDGE_CONFLICT            = -3045,
    E_MUTATE_TAG_CONFLICT             = -3046,

    // transaction
    E_OUTDATED_LOCK                   = -3047,

    // task manager failed
    E_INVALID_TASK_PARA               = -3051,
    E_USER_CANCEL                     = -3052,
    E_TASK_EXECUTION_FAILED           = -3053,

    E_UNKNOWN                         = -8000,
} (cpp.enum_strict)
