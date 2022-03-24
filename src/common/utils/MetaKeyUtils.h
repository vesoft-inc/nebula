/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_KEY_UTILS_H_
#define META_KEY_UTILS_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/datatypes/HostAddr.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/KVStore.h"

namespace nebula {

static const PartitionID kDefaultPartId = 0;
static const GraphSpaceID kDefaultSpaceId = 0;

using BalanceID = int64_t;

enum class BalanceTaskStatus : uint8_t {
  START = 0x01,
  CHANGE_LEADER = 0x02,
  ADD_PART_ON_DST = 0x03,
  ADD_LEARNER = 0x04,
  CATCH_UP_DATA = 0x05,
  MEMBER_CHANGE_ADD = 0x06,
  MEMBER_CHANGE_REMOVE = 0x07,
  UPDATE_PART_META = 0x08,  // After this state, we can't rollback anymore.
  REMOVE_PART_ON_SRC = 0x09,
  CHECK = 0x0A,
  END = 0xFF,
};

enum class BalanceTaskResult : uint8_t {
  SUCCEEDED = 0x01,
  FAILED = 0x02,
  IN_PROGRESS = 0x03,
  INVALID = 0x04,
};

enum class BalanceStatus : uint8_t {
  NOT_START = 0x01,
  IN_PROGRESS = 0x02,
  SUCCEEDED = 0x03,
  /**
   * TODO(heng): Currently, after the plan failed, we will try to resume it
   * when running "balance" again. But in many cases, the plan will be failed
   * forever, it this cases, we should rollback the plan.
   * */
  FAILED = 0x04,
};

enum class EntryType : int8_t {
  SPACE = 0x01,
  TAG = 0x02,
  EDGE = 0x03,
  INDEX = 0x04,
  CONFIG = 0x05,
  ZONE = 0x07,
};

using ConfigName = std::pair<meta::cpp2::ConfigModule, std::string>;
using LeaderParts = std::unordered_map<GraphSpaceID, std::vector<PartitionID>>;

class MetaKeyUtils final {
 public:
  MetaKeyUtils() = delete;

  static std::string lastUpdateTimeKey();

  static std::string lastUpdateTimeVal(const int64_t timeInMilliSec);

  static std::string spaceKey(GraphSpaceID spaceId);

  static std::string spaceVal(const meta::cpp2::SpaceDesc& spaceDesc);

  static meta::cpp2::SpaceDesc parseSpace(folly::StringPiece rawData);

  static const std::string& spacePrefix();

  static GraphSpaceID spaceId(folly::StringPiece rawKey);

  static std::string spaceName(folly::StringPiece rawVal);

  static std::string partKey(GraphSpaceID spaceId, PartitionID partId);

  static GraphSpaceID parsePartKeySpaceId(folly::StringPiece key);

  static PartitionID parsePartKeyPartId(folly::StringPiece key);

  static std::string partVal(const std::vector<HostAddr>& hosts);

  static std::string partValV1(const std::vector<HostAddr>& hosts);

  static std::string partValV2(const std::vector<HostAddr>& hosts);

  static std::string partPrefix();

  static std::string partPrefix(GraphSpaceID spaceId);

  static std::string encodeHostAddrV2(int ip, int port);

  static HostAddr decodeHostAddrV2(folly::StringPiece val, int& offset);

  static std::vector<HostAddr> parsePartVal(folly::StringPiece val, int parNum = 0);

  static std::vector<HostAddr> parsePartValV1(folly::StringPiece val);

  static std::vector<HostAddr> parsePartValV2(folly::StringPiece val);

  static std::string machineKey(std::string ip, Port port);

  static const std::string& machinePrefix();

  static HostAddr parseMachineKey(folly::StringPiece key);

  // hostDir store service(metad/storaged/graphd) address -> dir info(root path and data paths)
  // agent will use these to start/stop service and backup/restore data
  static std::string hostDirKey(std::string ip);

  static std::string hostDirKey(std::string host, Port port);

  static HostAddr parseHostDirKey(folly::StringPiece key);

  static const std::string& hostDirPrefix();

  static const std::string hostDirHostPrefix(std::string host);

  static std::string hostDirVal(cpp2::DirInfo dir);

  static cpp2::DirInfo parseHostDir(folly::StringPiece val);

  static std::string hostKey(std::string host, Port port);

  static std::string hostKeyV2(std::string host, Port port);

  static const std::string& hostPrefix();

  static HostAddr parseHostKey(folly::StringPiece key);

  static HostAddr parseHostKeyV1(folly::StringPiece key);

  static HostAddr parseHostKeyV2(folly::StringPiece key);

  static std::string versionKey(const HostAddr& h);

  static std::string versionVal(const std::string& version);

  static std::string parseVersion(folly::StringPiece val);

  static std::string leaderKey(std::string ip, Port port);

  static std::string leaderKeyV2(std::string addr, Port port);

  static std::string leaderKey(GraphSpaceID spaceId, PartitionID partId);

  static std::string leaderVal(const LeaderParts& leaderParts);

  static std::string leaderValV3(const HostAddr& h, int64_t term);

  static std::string leaderPrefix(GraphSpaceID spaceId);

  static const std::string& leaderPrefix();

  static HostAddr parseLeaderKey(folly::StringPiece key);

  static HostAddr parseLeaderKeyV1(folly::StringPiece key);

  static std::pair<GraphSpaceID, PartitionID> parseLeaderKeyV3(folly::StringPiece key);

  static HostAddr parseLeaderKeyV2(folly::StringPiece key);

  static LeaderParts parseLeaderValV1(folly::StringPiece val);

  static std::tuple<HostAddr, int64_t, nebula::cpp2::ErrorCode> parseLeaderValV3(
      folly::StringPiece val);

  static std::string schemaVal(const std::string& name, const meta::cpp2::Schema& schema);

  static std::string schemaEdgePrefix(GraphSpaceID spaceId, EdgeType edgeType);

  static std::string schemaEdgesPrefix(GraphSpaceID spaceId);

  static const std::string& schemaEdgesPrefix();

  static std::string schemaEdgeKey(GraphSpaceID spaceId, EdgeType edgeType, SchemaVer version);

  static EdgeType parseEdgeType(folly::StringPiece key);

  static SchemaVer parseEdgeVersion(folly::StringPiece key);

  static SchemaVer getLatestEdgeScheInfo(kvstore::KVIterator* iter, folly::StringPiece& val);

  static std::string schemaTagKey(GraphSpaceID spaceId, TagID tagId, SchemaVer version);

  static TagID parseTagId(folly::StringPiece key);

  static SchemaVer parseTagVersion(folly::StringPiece key);

  static SchemaVer getLatestTagScheInfo(kvstore::KVIterator* iter, folly::StringPiece& val);

  static std::string schemaTagPrefix(GraphSpaceID spaceId, TagID tagId);

  static std::string schemaTagsPrefix(GraphSpaceID spaceId);

  static const std::string& schemaTagsPrefix();

  static meta::cpp2::Schema parseSchema(folly::StringPiece rawData);

  static std::string indexKey(GraphSpaceID spaceId, IndexID indexID);

  static std::string indexVal(const meta::cpp2::IndexItem& item);

  static std::string indexPrefix(GraphSpaceID spaceId);

  static const std::string& indexPrefix();

  static IndexID parseIndexesKeyIndexID(folly::StringPiece key);

  static meta::cpp2::IndexItem parseIndex(const folly::StringPiece& rawData);

  static std::string rebuildIndexStatus(GraphSpaceID space,
                                        char type,
                                        const std::string& indexName);

  static std::string rebuildIndexStatusPrefix(GraphSpaceID spaceId, char type);

  static std::string rebuildIndexStatusPrefix();

  static std::string rebuildTagIndexStatusPrefix(GraphSpaceID spaceId) {
    return rebuildIndexStatusPrefix(spaceId, 'T');
  }

  static std::string rebuildEdgeIndexStatusPrefix(GraphSpaceID spaceId) {
    return rebuildIndexStatusPrefix(spaceId, 'E');
  }

  static std::string indexSpaceKey(const std::string& name);

  static std::string parseIndexSpaceKey(folly::StringPiece key);

  static EntryType parseIndexType(folly::StringPiece key);

  static std::string indexTagKey(GraphSpaceID spaceId, const std::string& name);

  static std::string indexEdgeKey(GraphSpaceID spaceId, const std::string& name);

  static std::string indexIndexKey(GraphSpaceID spaceId, const std::string& name);

  static std::string indexZoneKey(const std::string& name);

  static std::string userPrefix();

  static std::string userKey(const std::string& account);

  static std::string userVal(const std::string& val);

  static std::string parseUser(folly::StringPiece key);

  static std::string parseUserPwd(folly::StringPiece val);

  static std::string roleKey(GraphSpaceID spaceId, const std::string& account);

  static std::string roleVal(meta::cpp2::RoleType roleType);

  static std::string parseRoleUser(folly::StringPiece key);

  static GraphSpaceID parseRoleSpace(folly::StringPiece key);

  static std::string rolesPrefix();

  static std::string roleSpacePrefix(GraphSpaceID spaceId);

  static std::string parseRoleStr(folly::StringPiece key);

  static std::string configKey(const meta::cpp2::ConfigModule& module, const std::string& name);

  static std::string configKeyPrefix(const meta::cpp2::ConfigModule& module);

  static std::string configValue(const meta::cpp2::ConfigMode& valueMode, const Value& config);

  static ConfigName parseConfigKey(folly::StringPiece rawData);

  static meta::cpp2::ConfigItem parseConfigValue(folly::StringPiece rawData);

  static std::string snapshotKey(const std::string& name);

  static std::string snapshotVal(const meta::cpp2::SnapshotStatus& status,
                                 const std::string& hosts);

  static meta::cpp2::SnapshotStatus parseSnapshotStatus(folly::StringPiece rawData);

  static std::string parseSnapshotHosts(folly::StringPiece rawData);

  static std::string parseSnapshotName(folly::StringPiece rawData);

  static const std::string& snapshotPrefix();

  static std::string serializeHostAddr(const HostAddr& host);

  static HostAddr deserializeHostAddr(folly::StringPiece str);

  static std::string balanceTaskKey(
      JobID jobId, GraphSpaceID spaceId, PartitionID partId, HostAddr src, HostAddr dst);

  static std::string balanceTaskVal(BalanceTaskStatus status,
                                    BalanceTaskResult result,
                                    int64_t startTime,
                                    int64_t endTime);

  static std::string balanceTaskPrefix(JobID jobId);

  static std::tuple<BalanceID, GraphSpaceID, PartitionID, HostAddr, HostAddr> parseBalanceTaskKey(
      const folly::StringPiece& rawKey);

  static std::tuple<BalanceTaskStatus, BalanceTaskResult, int64_t, int64_t> parseBalanceTaskVal(
      const folly::StringPiece& rawVal);

  static std::string zoneKey(const std::string& zone);

  static std::string zoneVal(const std::vector<HostAddr>& hosts);

  static const std::string& zonePrefix();

  static std::string parseZoneName(folly::StringPiece rawData);

  static std::vector<HostAddr> parseZoneHosts(folly::StringPiece rawData);

  static std::string listenerKey(GraphSpaceID spaceId,
                                 PartitionID partId,
                                 meta::cpp2::ListenerType type);

  static std::string listenerPrefix(GraphSpaceID spaceId);

  static std::string listenerPrefix(GraphSpaceID spaceId, meta::cpp2::ListenerType type);

  static meta::cpp2::ListenerType parseListenerType(folly::StringPiece rawData);

  static GraphSpaceID parseListenerSpace(folly::StringPiece rawData);

  static PartitionID parseListenerPart(folly::StringPiece rawData);

  static std::string statsKey(GraphSpaceID spaceId);

  static std::string statsVal(const meta::cpp2::StatsItem& statsItem);

  static meta::cpp2::StatsItem parseStatsVal(folly::StringPiece rawData);

  static const std::string& statsKeyPrefix();

  static GraphSpaceID parseStatsSpace(folly::StringPiece rawData);

  static std::string serviceKey(const meta::cpp2::ExternalServiceType& type);

  static std::string serviceVal(const std::vector<meta::cpp2::ServiceClient>& client);

  static const std::string& servicePrefix();

  static meta::cpp2::ExternalServiceType parseServiceType(folly::StringPiece rawData);

  static std::vector<meta::cpp2::ServiceClient> parseServiceClients(folly::StringPiece rawData);

  static const std::string& sessionPrefix();

  static std::string sessionKey(SessionID sessionId);

  static std::string sessionVal(const meta::cpp2::Session& session);

  static SessionID getSessionId(const folly::StringPiece& key);

  static meta::cpp2::Session parseSessionVal(const folly::StringPiece& val);

  static std::string fulltextIndexKey(const std::string& indexName);

  static std::string fulltextIndexVal(const meta::cpp2::FTIndex& index);

  static std::string parsefulltextIndexName(folly::StringPiece key);

  static meta::cpp2::FTIndex parsefulltextIndex(folly::StringPiece val);

  static std::string fulltextIndexPrefix();

  static std::string genTimestampStr();

  static GraphSpaceID parseEdgesKeySpaceID(folly::StringPiece key);
  static GraphSpaceID parseTagsKeySpaceID(folly::StringPiece key);
  static GraphSpaceID parseIndexesKeySpaceID(folly::StringPiece key);
  static GraphSpaceID parseIndexStatusKeySpaceID(folly::StringPiece key);
  static GraphSpaceID parseIndexKeySpaceID(folly::StringPiece key);
  static GraphSpaceID parseDefaultKeySpaceID(folly::StringPiece key);

  static std::string idKey();

  static std::string localIdKey(GraphSpaceID spaceId);

  static GraphSpaceID parseLocalIdSpace(folly::StringPiece rawData);

  static std::string getIndexTable();

  static std::unordered_map<std::string,
                            std::pair<std::string, std::function<decltype(MetaKeyUtils::spaceId)>>>
  getTableMaps();

  static std::unordered_map<std::string, std::pair<std::string, bool>> getSystemInfoMaps();

  static std::unordered_map<std::string, std::pair<std::string, bool>> getSystemTableMaps();

  static GraphSpaceID parseDiskPartsSpace(const folly::StringPiece& rawData);

  static HostAddr parseDiskPartsHost(const folly::StringPiece& rawData);

  static std::string parseDiskPartsPath(const folly::StringPiece& rawData);

  static std::string diskPartsPrefix();

  static std::string diskPartsPrefix(HostAddr addr);

  static std::string diskPartsPrefix(HostAddr addr, GraphSpaceID spaceId);

  static std::string diskPartsKey(HostAddr addr, GraphSpaceID spaceId, const std::string& path);

  static std::string diskPartsVal(const meta::cpp2::PartitionList& partList);

  static meta::cpp2::PartitionList parseDiskPartsVal(const folly::StringPiece& rawData);

  // job related
  static const std::string& jobPrefix();

  static std::string jobPrefix(GraphSpaceID spaceId);

  /**
   * @brief Encoded job key, it should be
   * kJobTable+spceId+jobid
   */
  static std::string jobKey(GraphSpaceID spaceId, JobID iJob);

  static bool isJobKey(const folly::StringPiece& rawKey);

  static std::string jobVal(const meta::cpp2::JobType& type,
                            std::vector<std::string> paras,
                            meta::cpp2::JobStatus jobStatus,
                            int64_t startTime,
                            int64_t stopTime);
  /**
   * @brief Decode val from kvstore, return
   * {jobType, paras, status, start time, stop time}
   */
  static std::
      tuple<meta::cpp2::JobType, std::vector<std::string>, meta::cpp2::JobStatus, int64_t, int64_t>
      parseJobVal(folly::StringPiece rawVal);

  static std::pair<GraphSpaceID, JobID> parseJobKey(folly::StringPiece key);

  // task related
  /**
   * @brief Encoded task key, it should bekJobTable+spceId+jobid+taskid
   */
  static std::string taskKey(GraphSpaceID spaceId, JobID jobId, TaskID taskId);

  static std::tuple<GraphSpaceID, JobID, TaskID> parseTaskKey(folly::StringPiece key);

  static std::string taskVal(HostAddr host,
                             meta::cpp2::JobStatus jobStatus,
                             int64_t startTime,
                             int64_t stopTime);

  /**
   * @brief Decode task val，it should be
   * {host, status, start time, stop time}
   */
  static std::tuple<HostAddr, meta::cpp2::JobStatus, int64_t, int64_t> parseTaskVal(
      folly::StringPiece rawVal);
};

}  // namespace nebula

#endif  // META_KEY_UTILS_H_
