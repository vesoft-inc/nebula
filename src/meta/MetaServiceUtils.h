/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METAUTILS_H_
#define META_METAUTILS_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/datatypes/HostAddr.h"
#include "common/interface/gen-cpp2/meta_types.h"
#include "kvstore/NebulaStore.h"
#include "meta/processors/Common.h"

namespace nebula {
namespace meta {

enum class EntryType : int8_t {
    SPACE       = 0x01,
    TAG         = 0x02,
    EDGE        = 0x03,
    INDEX       = 0x04,
    CONFIG      = 0x05,
    GROUP       = 0x06,
    ZONE        = 0x07,
};

using ConfigName = std::pair<cpp2::ConfigModule, std::string>;
using LeaderParts = std::unordered_map<GraphSpaceID, std::vector<PartitionID>>;

class MetaServiceUtils final {
public:
    MetaServiceUtils() = delete;

    static std::string lastUpdateTimeKey();

    static std::string lastUpdateTimeVal(const int64_t timeInMilliSec);

    static std::string spaceKey(GraphSpaceID spaceId);

    static std::string spaceVal(const cpp2::SpaceDesc& spaceDesc);

    static cpp2::SpaceDesc parseSpace(folly::StringPiece rawData);

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

    static std::string hostKey(std::string ip, Port port);

    static std::string hostKeyV2(std::string addr, Port port);

    static const std::string& hostPrefix();

    static HostAddr parseHostKey(folly::StringPiece key);

    static HostAddr parseHostKeyV1(folly::StringPiece key);

    static HostAddr parseHostKeyV2(folly::StringPiece key);

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

    static std::tuple<HostAddr, int64_t, nebula::cpp2::ErrorCode>
    parseLeaderValV3(folly::StringPiece val);

    static std::string schemaVal(const std::string& name, const cpp2::Schema& schema);

    static std::string schemaEdgePrefix(GraphSpaceID spaceId, EdgeType edgeType);

    static std::string schemaEdgesPrefix(GraphSpaceID spaceId);

    static std::string schemaEdgeKey(GraphSpaceID spaceId, EdgeType edgeType, SchemaVer version);

    static EdgeType parseEdgeType(folly::StringPiece key);

    static SchemaVer parseEdgeVersion(folly::StringPiece key);

    static std::string schemaTagKey(GraphSpaceID spaceId, TagID tagId, SchemaVer version);

    static TagID parseTagId(folly::StringPiece key);

    static SchemaVer parseTagVersion(folly::StringPiece key);

    static std::string schemaTagPrefix(GraphSpaceID spaceId, TagID tagId);

    static std::string schemaTagsPrefix(GraphSpaceID spaceId);

    static cpp2::Schema parseSchema(folly::StringPiece rawData);

    static std::string indexKey(GraphSpaceID spaceId, IndexID indexID);

    static std::string indexVal(const cpp2::IndexItem& item);

    static std::string indexPrefix(GraphSpaceID spaceId);

    static IndexID parseIndexesKeyIndexID(folly::StringPiece key);

    static cpp2::IndexItem parseIndex(const folly::StringPiece& rawData);

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

    static std::string indexTagKey(GraphSpaceID spaceId, const std::string& name);

    static std::string indexEdgeKey(GraphSpaceID spaceId, const std::string& name);

    static std::string indexIndexKey(GraphSpaceID spaceId, const std::string& name);

    static std::string indexGroupKey(const std::string& name);

    static std::string indexZoneKey(const std::string& name);

    static std::string assembleSegmentKey(const std::string& segment, const std::string& key);

    static nebula::cpp2::ErrorCode
    alterColumnDefs(std::vector<cpp2::ColumnDef>& cols,
                    cpp2::SchemaProp& prop,
                    const cpp2::ColumnDef col,
                    const cpp2::AlterSchemaOp op,
                    bool isEdge = false);

    static std::string userPrefix();

    static nebula::cpp2::ErrorCode
    alterSchemaProp(std::vector<cpp2::ColumnDef>& cols,
                    cpp2::SchemaProp& schemaProp,
                    cpp2::SchemaProp alterSchemaProp,
                    bool existIndex,
                    bool isEdge = false);

    static std::string userKey(const std::string& account);

    static std::string userVal(const std::string& val);

    static std::string parseUser(folly::StringPiece key);

    static std::string parseUserPwd(folly::StringPiece val);

    static std::string roleKey(GraphSpaceID spaceId, const std::string& account);

    static std::string roleVal(cpp2::RoleType roleType);

    static std::string parseRoleUser(folly::StringPiece key);

    static GraphSpaceID parseRoleSpace(folly::StringPiece key);

    static std::string rolesPrefix();

    static std::string roleSpacePrefix(GraphSpaceID spaceId);

    static std::string parseRoleStr(folly::StringPiece key);

    static std::string configKey(const cpp2::ConfigModule& module, const std::string& name);

    static std::string configKeyPrefix(const cpp2::ConfigModule& module);

    static std::string configValue(const cpp2::ConfigMode& valueMode, const Value& config);

    static ConfigName parseConfigKey(folly::StringPiece rawData);

    static cpp2::ConfigItem parseConfigValue(folly::StringPiece rawData);

    static std::string snapshotKey(const std::string& name);

    static std::string snapshotVal(const cpp2::SnapshotStatus& status, const std::string& hosts);

    static cpp2::SnapshotStatus parseSnapshotStatus(folly::StringPiece rawData);

    static std::string parseSnapshotHosts(folly::StringPiece rawData);

    static std::string parseSnapshotName(folly::StringPiece rawData);

    static const std::string& snapshotPrefix();

    static std::string serializeHostAddr(const HostAddr& host);

    static HostAddr deserializeHostAddr(folly::StringPiece str);

    static std::string balanceTaskKey(BalanceID balanceId,
                                      GraphSpaceID spaceId,
                                      PartitionID partId,
                                      HostAddr src,
                                      HostAddr dst);

    static std::string balanceTaskVal(BalanceTaskStatus status,
                                      BalanceTaskResult retult,
                                      int64_t startTime,
                                      int64_t endTime);

    static std::string balanceTaskPrefix(BalanceID balanceId);

    static std::string balancePlanKey(BalanceID id);

    static std::string balancePlanVal(BalanceStatus status);

    static std::string balancePlanPrefix();

    static BalanceID parseBalanceID(const folly::StringPiece& rawKey);

    static BalanceStatus parseBalanceStatus(const folly::StringPiece& rawVal);

    static std::tuple<BalanceID, GraphSpaceID, PartitionID, HostAddr, HostAddr>
    parseBalanceTaskKey(const folly::StringPiece& rawKey);

    static std::tuple<BalanceTaskStatus, BalanceTaskResult, int64_t, int64_t>
    parseBalanceTaskVal(const folly::StringPiece& rawVal);

    static std::string groupKey(const std::string& group);

    static std::string groupVal(const std::vector<std::string>& zones);

    static const std::string& groupPrefix();

    static std::string parseGroupName(folly::StringPiece rawData);

    static std::vector<std::string> parseZoneNames(folly::StringPiece rawData);

    static std::string zoneKey(const std::string& zone);

    static std::string zoneVal(const std::vector<HostAddr>& hosts);

    static const std::string& zonePrefix();

    static std::string parseZoneName(folly::StringPiece rawData);

    static std::vector<HostAddr> parseZoneHosts(folly::StringPiece rawData);

    static std::string listenerKey(GraphSpaceID spaceId,
                                   PartitionID partId,
                                   cpp2::ListenerType type);

    static std::string listenerPrefix(GraphSpaceID spaceId);

    static std::string listenerPrefix(GraphSpaceID spaceId,
                                      cpp2::ListenerType type);

    static cpp2::ListenerType parseListenerType(folly::StringPiece rawData);

    static GraphSpaceID parseListenerSpace(folly::StringPiece rawData);

    static PartitionID parseListenerPart(folly::StringPiece rawData);

    static std::string statisKey(GraphSpaceID spaceId);

    static std::string statisVal(const cpp2::StatisItem &statisItem);

    static cpp2::StatisItem parseStatisVal(folly::StringPiece rawData);

    static const std::string& statisKeyPrefix();

    static GraphSpaceID parseStatisSpace(folly::StringPiece rawData);

    static std::string fulltextServiceKey();

    static std::string fulltextServiceVal(cpp2::FTServiceType type,
                                          const std::vector<cpp2::FTClient>& clients);

    static std::vector<cpp2::FTClient> parseFTClients(folly::StringPiece rawData);

    static const std::string& sessionPrefix();

    static std::string sessionKey(SessionID sessionId);

    static std::string sessionVal(const meta::cpp2::Session &session);

    static SessionID getSessionId(const folly::StringPiece &key);

    static meta::cpp2::Session parseSessionVal(const folly::StringPiece &val);

    static std::string fulltextIndexKey(const std::string& indexName);

    static std::string fulltextIndexVal(const cpp2::FTIndex& index);

    static std::string parsefulltextIndexName(folly::StringPiece key);

    static cpp2::FTIndex parsefulltextIndex(folly::StringPiece val);

    static std::string fulltextIndexPrefix();

    static std::string genTimestampStr();

    static GraphSpaceID parseEdgesKeySpaceID(folly::StringPiece key);
    static GraphSpaceID parseTagsKeySpaceID(folly::StringPiece key);
    static GraphSpaceID parseIndexesKeySpaceID(folly::StringPiece key);
    static GraphSpaceID parseIndexStatusKeySpaceID(folly::StringPiece key);
    static GraphSpaceID parseIndexKeySpaceID(folly::StringPiece key);
    static GraphSpaceID parseDefaultKeySpaceID(folly::StringPiece key);

    static std::string idKey();

    // backup
    static ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> backupIndex(
        kvstore::KVStore* kvstore,
        const std::unordered_set<GraphSpaceID>& spaces,
        const std::string& backupName,
        const std::vector<std::string>* spaceName);

    static std::function<bool(const folly::StringPiece& key)> spaceFilter(
        const std::unordered_set<GraphSpaceID>& spaces,
        std::function<GraphSpaceID(folly::StringPiece rawKey)> parseSpace);

    static ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> backupSpaces(
        kvstore::KVStore* kvstore,
        const std::unordered_set<GraphSpaceID>& spaces,
        const std::string& backupName,
        const std::vector<std::string>* spaceName);
};

}   // namespace meta
}   // namespace nebula

#endif   // META_METAUTILS_H_
