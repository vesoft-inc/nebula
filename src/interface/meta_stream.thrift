/* vim: ft=proto
 * Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

namespace cpp nebula.meta

include "meta.thrift"

/*
 *
 *  Note: In order to support multiple languages, all strings
 *        have to be defined as **binary** in the thrift file
 *
 */

/*
 *  MetaInfo structure holds all persisted meta information, excluding
 *    the status (such as leader changes and job status changes).
 */
struct MetaInfo {
  // <id, name>
  1:  map<common.GraphSpaceID, binary> (cpp.template = "std::unordered_map") spaces,
  2:  map<common.GraphSpaceID,
          map<common.TagID, meta.TagItem> (cpp.template = "std::unordered_map")
         > (cpp.template = "std::unordered_map") tags,
  3:  map<common.GraphSpaceID,
          map<common.EdgeType, meta.EdgeItem> (cpp.template = "std::unordered_map")
         > (cpp.template = "std::unordered_map") edges,
  4:  map<common.GraphSpaceID,
          map<common.IndexID, meta.IndexItem> (cpp.template = "std::unordered_map")
         > (cpp.template = "std::unordered_map") tag_indices,
  5:  map<common.GraphSpaceID,
          map<common.IndexID, meta.IndexItem> (cpp.template = "std::unordered_map")
         > (cpp.template = "std::unordered_map") edge_indices,
  // TODO(sye) Not sure whetherwe should broadcast the full-text index client info
  6:  list<meta.FTClient> fulltext_index_clients,
  7:  map<binary, meta.FTIndex>  // <index_name, index_def>
      (cpp.template = "std::unordered_map") fulltext_indices,
  8:  set<binary> (cpp.template = "std::set") users,
  9:  map<common.GraphSpaceID,
          map<binary, meta.RoleType> (cpp.template = "std::unordered_map")
         > (cpp.template = "std::unordered_map") user_roles,
  10: map<common.GraphSpaceID, map<common.PartitionID, list<common.HostAddr>>
        (cpp.template = "std::unordered_map")> (cpp.template = "std::unordered_map") parts,
  // TODO(sye) We probably want to re-consider how we are going to control the listeners
  11: map<common.GraphSpaceID, list<meta.ListenerInfo>>
        (cpp.template = "std::unordered_map") listeners,
  12: map<meta.ConfigModule,
          map<binary, meta.ConfigItem> (cpp.template = "std::unordered_map")
         > (cpp.template = "std::unordered_map") config,
}


struct LeaderAndTerm {
  1: common.HostAddr leader,
  2: i64 term,
}


/*
 *  StatusInfo structure holds ephemeral status, such as partition leader and
 *    job status
 */
struct StatusInfo {
  1:  map<common.GraphSpaceID,
          map<common.PartitionID, LeaderAndTerm> (cpp.template = "std::unordered_map")
         > (cpp.template = "std::unordered_map") leader_status,
}


struct MetaData {
  1:  required i32 key_version,
  2:  required i64 meta_revision,
  3:  optional MetaInfo meta_info (cpp.ref_type = "shared"),
  4:  optional StatusInfo status_info (cpp.ref_type = "shared"),
}


service MetaStreamService {
    // To get stream for all necessary meta info changes
    //
    // When the stream service receives a call from a client, it will compare
    //    the lastUpdateTimeInMs to the timestamp on the meta service. If the client
    //    time is newer, empty StreamMessage will be sent, otherwise, the entire meta
    //    info (including all sections) and all status info will be sent to the client.
    //    In the following stream, it will only send the meta info or status that changes,
    //    all other sections will be null
    stream<MetaData> readMetaData(1: i64 lastRevision);
}

