/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

namespace cpp nebula.meta
namespace java com.vesoft.nebula.meta
namespace go nebula.meta
namespace js nebula.meta
namespace csharp nebula.meta
namespace py nebula2.meta

include "common.thrift"

/*
 *
 *  Note: In order to support multiple languages, all strings
 *        have to be defined as **binary** in the thrift file
 *
 */

typedef i64 (cpp.type = "nebula::SchemaVer") SchemaVer

typedef i64 (cpp.type = "nebula::ClusterID") ClusterID

enum AlterSchemaOp {
    ADD    = 0x01,
    CHANGE = 0x02,
    DROP   = 0x03,
    UNKNOWN = 0x04,
} (cpp.enum_strict)

/*
 * GOD is A global senior administrator.like root of Linux systems.
 * ADMIN is an administrator for a given Graph Space.
 * USER is a normal user for a given Graph Space. A User can access (read and write)
 *      the data in the Graph Space.
 * GUEST is a read-only role for a given Graph Space. A Guest cannot modify the data
 *       in the Graph Space.
 *
 * Refer to header file src/graph/PermissionManager.h for details.
 */

enum RoleType {
    GOD    = 0x01,
    ADMIN  = 0x02,
	DBA	   = 0x03,
    USER   = 0x04,
    GUEST  = 0x05,
} (cpp.enum_strict)


union ID {
    1: common.GraphSpaceID  space_id,
    2: common.TagID         tag_id,
    3: common.EdgeType      edge_type,
    4: common.IndexID       index_id,
    5: ClusterID            cluster_id,
}


// Geo shape type
enum GeoShape {
    ANY = 0,
    POINT = 1,
    LINESTRING = 2,
    POLYGON = 3,
} (cpp.enum_strict)


struct ColumnTypeDef {
    1: required common.PropertyType    type,
    // type_length is valid for fixed_string type
    2: optional i16                    type_length = 0,
    // geo_shape is valid for geography type
    3: optional GeoShape               geo_shape,
}

struct ColumnDef {
    1: required binary          name,
    2: required ColumnTypeDef   type,
    3: optional binary          default_value,
    4: optional bool            nullable = false,
    5: optional binary          comment,
}

struct SchemaProp {
    1: optional i64      ttl_duration,
    2: optional binary   ttl_col,
    3: optional binary   comment,
}

struct Schema {
    1: list<ColumnDef> columns,
    2: SchemaProp schema_prop,
}

struct IdName {
    1: ID     id,
    2: binary name,
}

enum IsolationLevel {
    DEFAULT  = 0x00,    // allow add half edge(either in or out edge succeeded)
    TOSS     = 0x01,    // add in and out edge atomic
} (cpp.enum_strict)

struct SpaceDesc {
    1: binary                   space_name,
    2: i32                      partition_num = 0,
    3: i32                      replica_factor = 0,
    4: binary                   charset_name,
    5: binary                   collate_name,
    6: ColumnTypeDef            vid_type = {"type": common.PropertyType.FIXED_STRING, "type_length": 8},
    7: optional binary          group_name,
    8: optional IsolationLevel  isolation_level,
    9: optional binary          comment,
}

struct SpaceItem {
    1: common.GraphSpaceID  space_id,
    2: SpaceDesc            properties,
}

struct TagItem {
    1: common.TagID     tag_id,
    2: binary           tag_name,
    3: SchemaVer        version,
    4: Schema           schema,
}

struct AlterSchemaItem {
    1: AlterSchemaOp    op,
    2: Schema           schema,
}

struct EdgeItem {
    1: common.EdgeType  edge_type,
    2: binary           edge_name,
    3: SchemaVer        version,
    4: Schema           schema,
}

struct IndexItem {
    1: common.IndexID      index_id,
    2: binary              index_name,
    3: common.SchemaID     schema_id
    4: binary              schema_name,
    5: list<ColumnDef>     fields,
    6: optional binary     comment,
}

enum HostStatus {
    ONLINE  = 0x00,
    OFFLINE = 0x01,
    UNKNOWN = 0x02,
} (cpp.enum_strict)

enum SnapshotStatus {
    VALID    = 0x00,
    INVALID  = 0x01,
} (cpp.enum_strict)

struct HostItem {
    1: common.HostAddr      hostAddr,
    2: HostStatus           status,
    3: map<binary, list<common.PartitionID>>
        (cpp.template = "std::unordered_map") leader_parts,
    4: map<binary, list<common.PartitionID>>
        (cpp.template = "std::unordered_map") all_parts,
    5: HostRole             role,
    6: binary               git_info_sha,
    7: optional binary      zone_name,
    // version of binary
    8: optional binary      version,
}

struct UserItem {
    1: binary account,
    // Disable user if lock status is true.
    2: bool   is_lock,
    // The number of queries an account can issue per hour
    3: i32    max_queries_per_hour,
    // The number of updates an account can issue per hour
    4: i32    max_updates_per_hour,
    // The number of times an account can connect to the server per hour
    5: i32    max_connections_per_hour,
    // The number of simultaneous connections to the server by an account
    6: i32    max_user_connections,
}

struct RoleItem {
    1: binary               user_id,
    2: common.GraphSpaceID  space_id,
    3: RoleType             role_type,
}

struct ExecResp {
    1: common.ErrorCode code,
    // For custom kv operations, it is useless.
    2: ID               id,
    // Valid if ret equals E_LEADER_CHANGED.
    3: common.HostAddr  leader,
}

// Job related data structures
enum AdminJobOp {
    ADD         = 0x01,
    SHOW_All    = 0x02,
    SHOW        = 0x03,
    STOP        = 0x04,
    RECOVER     = 0x05,
} (cpp.enum_strict)

struct AdminJobReq {
    1: AdminJobOp       op,
    2: AdminCmd         cmd,
    3: list<binary>     paras,
}

enum AdminCmd {
    COMPACT                  = 0,
    FLUSH                    = 1,
    REBUILD_TAG_INDEX        = 2,
    REBUILD_EDGE_INDEX       = 3,
    REBUILD_FULLTEXT_INDEX   = 4,
    STATS                    = 5,
    DATA_BALANCE             = 6,
    DOWNLOAD                 = 7,
    INGEST                   = 8,
    UNKNOWN                  = 99,
} (cpp.enum_strict)

enum JobStatus {
    QUEUE           = 0x01,
    RUNNING         = 0x02,
    FINISHED        = 0x03,
    FAILED          = 0x04,
    STOPPED         = 0x05,
    INVALID         = 0xFF,
} (cpp.enum_strict)

struct JobDesc {
    1: i32              id,
    2: AdminCmd         cmd,
    3: list<string>     paras,
    4: JobStatus        status,
    5: i64              start_time,
    6: i64              stop_time,
}

struct TaskDesc {
    1: i32              task_id,
    2: common.HostAddr  host,
    3: JobStatus        status,
    4: i64              start_time,
    5: i64              stop_time,
    6: i32              job_id,
}

struct AdminJobResult {
    // used in a new added job, e.g. "flush" "compact"
    // other job type which also need jobId in their result
    // will use other filed. e.g. JobDesc::id
    1: optional i32                 job_id,

    // used in "show jobs" and "show job <id>"
    2: optional list<JobDesc>       job_desc,

    // used in "show job <id>"
    3: optional list<TaskDesc>      task_desc,

    // used in "recover job"
    4: optional i32                 recovered_job_num,
}

struct AdminJobResp {
    1: common.ErrorCode             code,
    2: common.HostAddr              leader,
    3: AdminJobResult               result,
}

struct Correlativity {
    1: common.PartitionID part_id,
    2: double             proportion,
}

struct StatsItem {
    // The number of vertices of tagName
    1: map<binary, i64>
        (cpp.template = "std::unordered_map") tag_vertices,
    // The number of out edges of edgeName
    2: map<binary, i64>
        (cpp.template = "std::unordered_map") edges,
    // The number of vertices of current space
    3: i64                                    space_vertices,
    // The number of edges of current space
    4: i64                                    space_edges,
    // Used to describe the proportion of positive edges
    // between the current partition and other partitions.
    5: map<common.PartitionID, list<Correlativity>>
        (cpp.template = "std::unordered_map") positive_part_correlativity,
    // Used to describe the proportion of negative edges
    // between the current partition and other partitions.
    6: map<common.PartitionID, list<Correlativity>>
        (cpp.template = "std::unordered_map") negative_part_correlativity,
    7: JobStatus                              status,
}

// Graph space related operations.
struct CreateSpaceReq {
    1: SpaceDesc        properties,
    2: bool             if_not_exists,
}

struct CreateSpaceAsReq {
    1: binary        old_space_name,
    2: binary        new_space_name,
}

struct DropSpaceReq {
    1: binary space_name
    2: bool   if_exists,
}

struct ListSpacesReq {
}

struct ListSpacesResp {
    1: common.ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: list<IdName>     spaces,
}

struct GetSpaceReq {
    1: binary   space_name,
}

struct GetSpaceResp {
    1: common.ErrorCode  code,
    2: common.HostAddr   leader,
    3: SpaceItem         item,
}

// Tags related operations
struct CreateTagReq {
    1: common.GraphSpaceID  space_id,
    2: binary               tag_name,
    3: Schema               schema,
    4: bool                 if_not_exists,
}

struct AlterTagReq {
    1: common.GraphSpaceID      space_id,
    2: binary                   tag_name,
    3: list<AlterSchemaItem>    tag_items,
    4: SchemaProp               schema_prop,
}

struct DropTagReq {
    1: common.GraphSpaceID space_id,
    2: binary              tag_name,
    3: bool                if_exists,
}

struct ListTagsReq {
    1: common.GraphSpaceID space_id,
}

struct ListTagsResp {
    1: common.ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: list<TagItem>    tags,
}

struct GetTagReq {
    1: common.GraphSpaceID  space_id,
    2: binary               tag_name,
    3: SchemaVer            version,
}

struct GetTagResp {
    1: common.ErrorCode code,
    2: common.HostAddr  leader,
    3: Schema           schema,
}

// Edge related operations.
struct CreateEdgeReq {
    1: common.GraphSpaceID  space_id,
    2: binary               edge_name,
    3: Schema               schema,
    4: bool                 if_not_exists,
}

struct AlterEdgeReq {
    1: common.GraphSpaceID      space_id,
    2: binary                   edge_name,
    3: list<AlterSchemaItem>    edge_items,
    4: SchemaProp               schema_prop,
}

struct GetEdgeReq {
    1: common.GraphSpaceID  space_id,
    2: binary               edge_name,
    3: SchemaVer            version,
}

struct GetEdgeResp {
    1: common.ErrorCode code,
    2: common.HostAddr  leader,
    3: Schema           schema,
}

struct DropEdgeReq {
    1: common.GraphSpaceID space_id,
    2: binary              edge_name,
    3: bool                if_exists,
}

struct ListEdgesReq {
    1: common.GraphSpaceID space_id,
}

struct ListEdgesResp {
    1: common.ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: list<EdgeItem>   edges,
}

enum ListHostType {
    // nebula 1.0 show hosts, show leader, partition info
    ALLOC       = 0x00,
    GRAPH       = 0x01,
    META        = 0x02,
    STORAGE     = 0x03,
} (cpp.enum_strict)

struct ListHostsReq {
    1: ListHostType      type
}

struct ListHostsResp {
    1: common.ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: list<HostItem>   hosts,
}

struct PartItem {
    1: required common.PartitionID       part_id,
    2: optional common.HostAddr          leader,
    3: required list<common.HostAddr>    peers,
    4: required list<common.HostAddr>    losts,
}

struct ListPartsReq {
    1: common.GraphSpaceID      space_id,
    2: list<common.PartitionID> part_ids;
}

struct ListPartsResp {
    1: common.ErrorCode code,
    2: common.HostAddr  leader,
    3: list<PartItem>   parts,
}

struct GetPartsAllocReq {
    1: common.GraphSpaceID space_id,
}

struct GetPartsAllocResp {
    1: common.ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: map<common.PartitionID, list<common.HostAddr>>(cpp.template = "std::unordered_map") parts,
    4: optional map<common.PartitionID, i64>(cpp.template = "std::unordered_map") terms,
}

struct MultiPutReq {
    // segment is used to avoid conflict with system data.
    // it should be comprised of numbers and letters.
    1: binary                   segment,
    2: list<common.KeyValue>    pairs,
}

struct GetReq {
    1: binary segment,
    2: binary key,
}

struct GetResp {
    1: common.ErrorCode code,
    2: common.HostAddr  leader,
    3: binary           value,
}

struct MultiGetReq {
    1: binary       segment,
    2: list<binary> keys,
}

struct MultiGetResp {
    1: common.ErrorCode code,
    2: common.HostAddr  leader,
    3: list<binary>     values,
}

struct RemoveReq {
    1: binary segment,
    2: binary key,
}

struct RemoveRangeReq {
    1: binary segment,
    2: binary start,
    3: binary end,
}

struct ScanReq {
    1: binary segment,
    2: binary start,
    3: binary end,
}

struct ScanResp {
    1: common.ErrorCode code,
    2: common.HostAddr  leader,
    3: list<binary>     values,
}

struct HBResp {
    1: common.ErrorCode code,
    2: common.HostAddr  leader,
    3: ClusterID        cluster_id,
    4: i64              last_update_time_in_ms,
    5: i32              meta_version,
}

enum HostRole {
    GRAPH       = 0x00,
    META        = 0x01,
    STORAGE     = 0x02,
    LISTENER    = 0x03,
    UNKNOWN     = 0x04
} (cpp.enum_strict)

struct LeaderInfo {
    1: common.PartitionID part_id,
    2: i64                term
}

struct HBReq {
    1: HostRole   role,
    2: common.HostAddr host,
    3: ClusterID cluster_id,
    4: optional map<common.GraphSpaceID, list<LeaderInfo>>
        (cpp.template = "std::unordered_map") leader_partIds;
    5: binary     git_info_sha,
    // version of binary
    6: optional binary version,
}

struct IndexFieldDef {
    1: required binary       name,
    // type_length is required if the field type is STRING.
    2: optional i16          type_length,
}

struct CreateTagIndexReq {
    1: common.GraphSpaceID  space_id,
    2: binary               index_name,
    3: binary               tag_name,
    4: list<IndexFieldDef>  fields,
    5: bool                 if_not_exists,
    6: optional binary      comment,
}

struct DropTagIndexReq {
    1: common.GraphSpaceID space_id,
    2: binary              index_name,
    3: bool                if_exists,
}

struct GetTagIndexReq {
    1: common.GraphSpaceID space_id,
    2: binary              index_name,
}

struct GetTagIndexResp {
    1: common.ErrorCode		code,
    2: common.HostAddr      leader,
    3: IndexItem           	item,
}

struct ListTagIndexesReq {
    1: common.GraphSpaceID space_id,
}

struct ListTagIndexesResp {
    1: common.ErrorCode     code,
    2: common.HostAddr      leader,
    3: list<IndexItem>		items,
}

struct CreateEdgeIndexReq {
    1: common.GraphSpaceID 	space_id,
    2: binary              	index_name,
    3: binary              	edge_name,
    4: list<IndexFieldDef>	fields,
    5: bool                	if_not_exists,
    6: optional binary      comment,
}

struct DropEdgeIndexReq {
    1: common.GraphSpaceID space_id,
    2: binary              index_name,
    3: bool                if_exists,
}

struct GetEdgeIndexReq {
    1: common.GraphSpaceID space_id,
    2: binary              index_name,
}

struct GetEdgeIndexResp {
    1: common.ErrorCode     code,
    2: common.HostAddr      leader,
    3: IndexItem          	item,
}

struct ListEdgeIndexesReq {
    1: common.GraphSpaceID space_id,
}

struct ListEdgeIndexesResp {
    1: common.ErrorCode     code,
    2: common.HostAddr      leader,
    3: list<IndexItem>    	items,
}

struct RebuildIndexReq {
    1: common.GraphSpaceID space_id,
    2: binary              index_name,
}

struct CreateUserReq {
    1: binary	account,
    2: binary   encoded_pwd,
    3: bool     if_not_exists,
}

struct DropUserReq {
    1: binary account,
    2: bool if_exists,
}

struct AlterUserReq {
    1: binary	account,
    2: binary   encoded_pwd,
}

struct GrantRoleReq {
    1: RoleItem role_item,
}

struct RevokeRoleReq {
    1: RoleItem role_item,
}

struct ListUsersReq {
}

struct ListUsersResp {
    1: common.ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    // map<account, encoded password>
    3: map<binary, binary> (cpp.template = "std::unordered_map") users,
}

struct ListRolesReq {
    1: common.GraphSpaceID space_id,
}

struct ListRolesResp {
    1: common.ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: list<RoleItem>   roles,
}

struct GetUserRolesReq {
    1: binary	account,
}

struct ChangePasswordReq {
    1: binary account,
    2: binary new_encoded_pwd,
    3: binary old_encoded_pwd,
}

struct BalanceReq {
    1: optional common.GraphSpaceID     space_id,
    // Specify the balance id to check the status of the related balance plan
    2: optional i64                     id,
    3: optional list<common.HostAddr>   host_del,
    4: optional bool                    stop,
    5: optional bool                    reset,
}

enum TaskResult {
    SUCCEEDED  = 0x00,
    FAILED = 0x01,
    IN_PROGRESS = 0x02,
    INVALID = 0x03,
} (cpp.enum_strict)


struct BalanceTask {
    1: binary id,
    2: TaskResult result,
}

struct BalanceResp {
    1: common.ErrorCode code,
    2: i64              id,
    // Valid if code equals E_LEADER_CHANGED.
    3: common.HostAddr  leader,
    4: list<BalanceTask> tasks,
}

struct LeaderBalanceReq {
}

enum ConfigModule {
    UNKNOWN = 0x00,
    ALL     = 0x01,
    GRAPH   = 0x02,
    META    = 0x03,
    STORAGE = 0x04,
} (cpp.enum_strict)

enum ConfigMode {
    IMMUTABLE   = 0x00,
    REBOOT      = 0x01,
    MUTABLE     = 0x02,
    IGNORED     = 0x03,
} (cpp.enum_strict)

struct ConfigItem {
    1: ConfigModule         module,
    2: binary               name,
    3: ConfigMode           mode,
    4: common.Value         value,
}

struct RegConfigReq {
    1: list<ConfigItem>     items,
}

struct GetConfigReq {
    1: ConfigItem item,
}

struct GetConfigResp {
    1: common.ErrorCode     code,
    2: common.HostAddr      leader,
    3: list<ConfigItem>     items,
}

struct SetConfigReq {
    1: ConfigItem           item,
}

struct ListConfigsReq {
    1: binary               space,
    2: ConfigModule         module,
}

struct ListConfigsResp {
    1: common.ErrorCode     code,
    2: common.HostAddr      leader,
    3: list<ConfigItem>     items,
}

struct CreateSnapshotReq {
}

struct DropSnapshotReq {
    1: binary   name,
}

struct ListSnapshotsReq {
}

struct Snapshot {
    1: binary         name,
    2: SnapshotStatus status,
    3: binary         hosts,
}

struct ListSnapshotsResp {
    1: common.ErrorCode     code,
    // Valid if code equals E_LEADER_CHANGED.
    2: common.HostAddr      leader,
    3: list<Snapshot>       snapshots,
}

struct ListIndexStatusReq {
    1: common.GraphSpaceID space_id,
}

struct IndexStatus {
    1: binary   name,
    2: binary   status,
}

struct ListIndexStatusResp {
    1: common.ErrorCode     code,
    2: common.HostAddr      leader,
    3: list<IndexStatus>    statuses,
}

// Zone related interface
struct AddZoneReq {
    1: binary                 zone_name,
    2: list<common.HostAddr>  nodes,
}

struct DropZoneReq {
    1: binary                 zone_name,
}

struct AddHostIntoZoneReq {
    1: common.HostAddr  node,
    2: binary           zone_name,
}

struct DropHostFromZoneReq {
    1: common.HostAddr  node,
    2: binary           zone_name,
}

struct GetZoneReq {
    1: binary                 zone_name,
}

struct GetZoneResp {
    1: common.ErrorCode       code,
    2: common.HostAddr        leader,
    3: list<common.HostAddr>  hosts,
}

struct ListZonesReq {
}

struct Zone {
    1: binary                 zone_name,
    2: list<common.HostAddr>  nodes,
}

struct ListZonesResp {
    1: common.ErrorCode code,
    2: common.HostAddr  leader,
    3: list<Zone>       zones,
}

struct AddGroupReq {
    1: binary        group_name,
    2: list<binary>  zone_names,
}

struct DropGroupReq {
    1: binary                 group_name,
}

struct AddZoneIntoGroupReq {
    1: binary  zone_name,
    2: binary  group_name,
}

struct DropZoneFromGroupReq {
    1: binary  zone_name,
    2: binary  group_name,
}

struct GetGroupReq {
    1: binary                 group_name,
}

struct GetGroupResp {
    1: common.ErrorCode      code,
    2: common.HostAddr       leader,
    3: list<binary>          zone_names,
}

struct ListGroupsReq {
}

struct Group {
    1: binary                 group_name,
    2: list<binary>           zone_names,
}

struct ListGroupsResp {
    1: common.ErrorCode code,
    2: common.HostAddr  leader,
    3: list<Group>      groups,
}

enum ListenerType {
    UNKNOWN       = 0x00,
    ELASTICSEARCH = 0x01,
} (cpp.enum_strict)

struct AddListenerReq {
    1: common.GraphSpaceID     space_id,
    2: ListenerType            type,
    3: list<common.HostAddr>   hosts,
}

struct RemoveListenerReq {
    1: common.GraphSpaceID     space_id,
    2: ListenerType            type,
}

struct ListListenerReq {
    1: common.GraphSpaceID     space_id,
}

struct ListenerInfo {
    1: ListenerType            type,
    2: common.HostAddr         host,
    3: common.PartitionID      part_id,
    4: HostStatus              status,
}

struct ListListenerResp {
    1: common.ErrorCode        code,
    2: common.HostAddr         leader,
    3: list<ListenerInfo>      listeners,
}

struct GetStatsReq {
    1: common.GraphSpaceID     space_id,
}

struct GetStatsResp {
    1: common.ErrorCode code,
    // Valid if ret equals E_LEADER_CHANGED.
    2: common.HostAddr  leader,
    3: StatsItem        stats,
}

struct BackupInfo {
    1: common.HostAddr host,
    2: list<common.CheckpointInfo> info,
}

struct SpaceBackupInfo {
    1: SpaceDesc           space,
    2: list<BackupInfo>    info,
}

struct BackupMeta {
    // space_name => SpaceBackupInfo
    1: map<common.GraphSpaceID, SpaceBackupInfo> (cpp.template = "std::unordered_map")  backup_info,
    // sst file
    2: list<binary>                               meta_files,
    // backup
    3: binary                                     backup_name,
    4: bool                                       full,
    5: bool                                       include_system_space,
    6: i64                                        create_time,
}

struct CreateBackupReq {
    // null means all spaces
    1: optional list<binary>  spaces,
}

struct CreateBackupResp {
    1: common.ErrorCode   code,
    2: common.HostAddr    leader,
    3: BackupMeta         meta,
}

struct HostPair {
    1: common.HostAddr   from_host,
    2: common.HostAddr   to_host,
}

struct RestoreMetaReq {
    1: list<binary>     files,
    2: list<HostPair>   hosts,
}

enum FTServiceType {
    ELASTICSEARCH = 0x01,
} (cpp.enum_strict)

struct FTClient {
    1: required common.HostAddr    host,
    2: optional binary             user,
    3: optional binary             pwd,
}

struct SignInFTServiceReq {
    1: FTServiceType                type,
    2: list<FTClient>               clients,
}

struct SignOutFTServiceReq {
}

struct ListFTClientsReq {
}

struct ListFTClientsResp {
    1: common.ErrorCode    code,
    2: common.HostAddr     leader,
    3: list<FTClient>      clients,
}

struct FTIndex {
    1: common.GraphSpaceID  space_id,
    2: common.SchemaID      depend_schema,
    3: list<binary>         fields,
}

struct CreateFTIndexReq {
    1: binary              fulltext_index_name,
    2: FTIndex             index,
}

struct DropFTIndexReq {
    1: common.GraphSpaceID space_id,
    2: binary              fulltext_index_name,
}

struct ListFTIndexesReq {
}

struct ListFTIndexesResp {
    1: common.ErrorCode     code,
    2: common.HostAddr      leader,
    3: map<binary, FTIndex> (cpp.template = "std::unordered_map") indexes,
}

enum QueryStatus {
    RUNNING         = 0x01,
    KILLING         = 0x02,
} (cpp.enum_strict)

struct QueryDesc {
    1: common.Timestamp start_time;
    2: QueryStatus status;
    3: i64 duration;
    4: binary query;
    // The session might transfer between query engines, but the query do not, we must
    // record which query engine the query belongs to
    5: common.HostAddr graph_addr,
}

struct Session {
    1: common.SessionID session_id,
    2: common.Timestamp create_time,
    3: common.Timestamp update_time,
    4: binary user_name,
    5: binary space_name,
    6: common.HostAddr graph_addr,
    7: i32 timezone,
    8: binary client_ip,
    9: map<binary, common.Value>(cpp.template = "std::unordered_map") configs,
    10: map<common.ExecutionPlanID, QueryDesc>(cpp.template = "std::unordered_map") queries;
}

struct CreateSessionReq {
    1: binary               user,
    2: common.HostAddr      graph_addr,
    3: binary               client_ip,
}

struct CreateSessionResp {
    1: common.ErrorCode     code,
    2: common.HostAddr      leader,
    3: Session              session,
}

struct UpdateSessionsReq {
    1: list<Session>        sessions,
}

struct UpdateSessionsResp {
    1: common.ErrorCode     code,
    2: common.HostAddr      leader,
    3: map<common.SessionID, map<common.ExecutionPlanID, QueryDesc> (cpp.template = "std::unordered_map")>
        (cpp.template = "std::unordered_map") killed_queries,
}

struct ListSessionsReq {
}

struct ListSessionsResp {
    1: common.ErrorCode      code,
    2: common.HostAddr       leader,
    3: list<Session>         sessions,
}

struct GetSessionReq {
    1: common.SessionID  session_id,
}

struct GetSessionResp {
    1: common.ErrorCode     code,
    2: common.HostAddr      leader,
    3: Session              session,
}

struct RemoveSessionReq {
    1: common.SessionID      session_id,
}

struct KillQueryReq {
    1: map<common.SessionID, set<common.ExecutionPlanID> (cpp.template = "std::unordered_set")>
        (cpp.template = "std::unordered_map") kill_queries,
}

struct ReportTaskReq {
    1: common.ErrorCode     code,
    2: i32                  job_id,
    3: i32                  task_id,
    4: optional StatsItem   stats
}

struct ListClusterInfoResp {
    1: common.ErrorCode         code,
    2: common.HostAddr          leader,
    3: list<common.HostAddr>    meta_servers,
    4: list<common.NodeInfo>    storage_servers,
}

struct ListClusterInfoReq {
}

struct GetMetaDirInfoResp {
    1: common.ErrorCode  code,
    2: common.DirInfo    dir,
}

struct GetMetaDirInfoReq {
}

struct VerifyClientVersionResp {
    1: common.ErrorCode         code,
    2: common.HostAddr          leader,
    3: optional binary           error_msg;
}


struct VerifyClientVersionReq {
    1: required binary version = common.version;
}

service MetaService {
    ExecResp createSpace(1: CreateSpaceReq req);
    ExecResp dropSpace(1: DropSpaceReq req);
    GetSpaceResp getSpace(1: GetSpaceReq req);
    ListSpacesResp listSpaces(1: ListSpacesReq req);

    ExecResp createSpaceAs(1: CreateSpaceAsReq req);

    ExecResp createTag(1: CreateTagReq req);
    ExecResp alterTag(1: AlterTagReq req);
    ExecResp dropTag(1: DropTagReq req);
    GetTagResp getTag(1: GetTagReq req);
    ListTagsResp listTags(1: ListTagsReq req);

    ExecResp createEdge(1: CreateEdgeReq req);
    ExecResp alterEdge(1: AlterEdgeReq req);
    ExecResp dropEdge(1: DropEdgeReq req);
    GetEdgeResp getEdge(1: GetEdgeReq req);
    ListEdgesResp listEdges(1: ListEdgesReq req);

    ListHostsResp listHosts(1: ListHostsReq req);

    GetPartsAllocResp getPartsAlloc(1: GetPartsAllocReq req);
    ListPartsResp listParts(1: ListPartsReq req);

    ExecResp multiPut(1: MultiPutReq req);
    GetResp get(1: GetReq req);
    MultiGetResp multiGet(1: MultiGetReq req);
    ExecResp remove(1: RemoveReq req);
    ExecResp removeRange(1: RemoveRangeReq req);
    ScanResp scan(1: ScanReq req);

    ExecResp             createTagIndex(1: CreateTagIndexReq req);
    ExecResp             dropTagIndex(1: DropTagIndexReq req );
    GetTagIndexResp      getTagIndex(1: GetTagIndexReq req);
    ListTagIndexesResp   listTagIndexes(1:ListTagIndexesReq req);
    ExecResp             rebuildTagIndex(1: RebuildIndexReq req);
    ListIndexStatusResp  listTagIndexStatus(1: ListIndexStatusReq req);
    ExecResp             createEdgeIndex(1: CreateEdgeIndexReq req);
    ExecResp             dropEdgeIndex(1: DropEdgeIndexReq req );
    GetEdgeIndexResp     getEdgeIndex(1: GetEdgeIndexReq req);
    ListEdgeIndexesResp  listEdgeIndexes(1: ListEdgeIndexesReq req);
    ExecResp             rebuildEdgeIndex(1: RebuildIndexReq req);
    ListIndexStatusResp  listEdgeIndexStatus(1: ListIndexStatusReq req);

    ExecResp createUser(1: CreateUserReq req);
    ExecResp dropUser(1: DropUserReq req);
    ExecResp alterUser(1: AlterUserReq req);
    ExecResp grantRole(1: GrantRoleReq req);
    ExecResp revokeRole(1: RevokeRoleReq req);
    ListUsersResp listUsers(1: ListUsersReq req);
    ListRolesResp listRoles(1: ListRolesReq req);
    ListRolesResp getUserRoles(1: GetUserRolesReq req);
    ExecResp changePassword(1: ChangePasswordReq req);

    HBResp           heartBeat(1: HBReq req);
    BalanceResp      balance(1: BalanceReq req);
    ExecResp         leaderBalance(1: LeaderBalanceReq req);

    ExecResp regConfig(1: RegConfigReq req);
    GetConfigResp getConfig(1: GetConfigReq req);
    ExecResp setConfig(1: SetConfigReq req);
    ListConfigsResp listConfigs(1: ListConfigsReq req);

    ExecResp createSnapshot(1: CreateSnapshotReq req);
    ExecResp dropSnapshot(1: DropSnapshotReq req);
    ListSnapshotsResp listSnapshots(1: ListSnapshotsReq req);

    AdminJobResp runAdminJob(1: AdminJobReq req);

    ExecResp       addZone(1: AddZoneReq req);
    ExecResp       dropZone(1: DropZoneReq req);
    ExecResp       addHostIntoZone(1: AddHostIntoZoneReq req);
    ExecResp       dropHostFromZone(1: DropHostFromZoneReq req);
    GetZoneResp    getZone(1: GetZoneReq req);
    ListZonesResp  listZones(1: ListZonesReq req);

    ExecResp       addGroup(1: AddGroupReq req);
    ExecResp       dropGroup(1: DropGroupReq req);
    ExecResp       addZoneIntoGroup(1: AddZoneIntoGroupReq req);
    ExecResp       dropZoneFromGroup(1: DropZoneFromGroupReq req);
    GetGroupResp   getGroup(1: GetGroupReq req);
    ListGroupsResp listGroups(1: ListGroupsReq req);

    CreateBackupResp createBackup(1: CreateBackupReq req);
    ExecResp       restoreMeta(1: RestoreMetaReq req);
    ExecResp       addListener(1: AddListenerReq req);
    ExecResp       removeListener(1: RemoveListenerReq req);
    ListListenerResp listListener(1: ListListenerReq req);

    GetStatsResp  getStats(1: GetStatsReq req);
    ExecResp signInFTService(1: SignInFTServiceReq req);
    ExecResp signOutFTService(1: SignOutFTServiceReq req);
    ListFTClientsResp listFTClients(1: ListFTClientsReq req);

    ExecResp createFTIndex(1: CreateFTIndexReq req);
    ExecResp dropFTIndex(1: DropFTIndexReq req);
    ListFTIndexesResp listFTIndexes(1: ListFTIndexesReq req);

    CreateSessionResp createSession(1: CreateSessionReq req);
    UpdateSessionsResp updateSessions(1: UpdateSessionsReq req);
    ListSessionsResp listSessions(1: ListSessionsReq req);
    GetSessionResp getSession(1: GetSessionReq req);
    ExecResp removeSession(1: RemoveSessionReq req);
    ExecResp killQuery(1: KillQueryReq req);

    ExecResp reportTaskFinish(1: ReportTaskReq req);

    ListClusterInfoResp listCluster(1: ListClusterInfoReq req);
    GetMetaDirInfoResp getMetaDirInfo(1: GetMetaDirInfoReq req);

    VerifyClientVersionResp verifyClientVersion(1: VerifyClientVersionReq req)
}
