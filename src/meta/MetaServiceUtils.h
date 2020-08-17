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

namespace nebula {
namespace meta {

enum class EntryType : int8_t {
    SPACE       = 0x01,
    TAG         = 0x02,
    EDGE        = 0x03,
    INDEX       = 0x04,
    CONFIG      = 0x05,
};

using ConfigName = std::pair<cpp2::ConfigModule, std::string>;
using LeaderParts = std::unordered_map<GraphSpaceID, std::vector<PartitionID>>;

class MetaServiceUtils final {
public:
    MetaServiceUtils() = delete;

    static std::string lastUpdateTimeKey();

    static std::string lastUpdateTimeVal(const int64_t timeInMilliSec);

    static std::string spaceKey(GraphSpaceID spaceId);

    static std::string spaceVal(const cpp2::SpaceProperties &properties);

    static cpp2::SpaceProperties parseSpace(folly::StringPiece rawData);

    static const std::string& spacePrefix();

    static GraphSpaceID spaceId(folly::StringPiece rawKey);

    static std::string spaceName(folly::StringPiece rawVal);

    static std::string partKey(GraphSpaceID spaceId, PartitionID partId);

    static GraphSpaceID parsePartKeySpaceId(folly::StringPiece key);

    static PartitionID parsePartKeyPartId(folly::StringPiece key);

    static std::string partVal(const std::vector<HostAddr>& hosts);

    static std::string partValV1(const std::vector<HostAddr>& hosts);

    static std::string partValV2(const std::vector<HostAddr>& hosts);

    static const std::string& partPrefix();

    static std::string partPrefix(GraphSpaceID spaceId);

    static std::string encodeHostAddrV2(int ip, int port);

    static HostAddr decodeHostAddrV2(folly::StringPiece val, int& offset);

    static std::vector<HostAddr> parsePartVal(folly::StringPiece val, int parNum = 0);

    static std::vector<HostAddr> parsePartValV1(folly::StringPiece val);

    static std::vector<HostAddr> parsePartValV2(folly::StringPiece val);

    static std::string hostKey(std::string ip, Port port);

    static std::string hostKeyV2(std::string addr, Port port);

    static std::string hostValOnline();

    static std::string hostValOffline();

    static const std::string& hostPrefix();

    static HostAddr parseHostKey(folly::StringPiece key);

    static HostAddr parseHostKeyV1(folly::StringPiece key);

    static HostAddr parseHostKeyV2(folly::StringPiece key);

    static std::string leaderKey(std::string ip, Port port);

    static std::string leaderKeyV2(std::string addr, Port port);

    static std::string leaderVal(const LeaderParts& leaderParts);

    static const std::string& leaderPrefix();

    static HostAddr parseLeaderKey(folly::StringPiece key);

    static HostAddr parseLeaderKeyV1(folly::StringPiece key);

    static HostAddr parseLeaderKeyV2(folly::StringPiece key);

    static LeaderParts parseLeaderVal(folly::StringPiece val);

    static std::string schemaVal(const std::string& name, const cpp2::Schema& schema);

    static std::string schemaEdgePrefix(GraphSpaceID spaceId, EdgeType edgeType);

    static std::string schemaEdgesPrefix(GraphSpaceID spaceId);

    static std::string schemaEdgeKey(GraphSpaceID spaceId, EdgeType edgeType, SchemaVer version);

    static SchemaVer parseEdgeVersion(folly::StringPiece key);

    static std::string schemaTagKey(GraphSpaceID spaceId, TagID tagId, SchemaVer version);

    static SchemaVer parseTagVersion(folly::StringPiece key);

    static std::string schemaTagPrefix(GraphSpaceID spaceId, TagID tagId);

    static std::string schemaTagsPrefix(GraphSpaceID spaceId);

    static cpp2::Schema parseSchema(folly::StringPiece rawData);

    static std::string indexKey(GraphSpaceID spaceId, IndexID indexID);

    static std::string indexVal(const cpp2::IndexItem& item);

    static std::string indexPrefix(GraphSpaceID spaceId);

    static cpp2::IndexItem parseIndex(const folly::StringPiece& rawData);

    static std::string rebuildIndexStatus(GraphSpaceID space,
                                          char type,
                                          const std::string& indexName);

    static std::string rebuildIndexStatusPrefix(GraphSpaceID spaceId, char type);

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

    static std::string assembleSegmentKey(const std::string& segment, const std::string& key);

    static cpp2::ErrorCode alterColumnDefs(std::vector<cpp2::ColumnDef>& cols,
                                           cpp2::SchemaProp&  prop,
                                           const cpp2::ColumnDef col,
                                           const cpp2::AlterSchemaOp op);

    static cpp2::ErrorCode alterSchemaProp(std::vector<cpp2::ColumnDef>& cols,
                                           cpp2::SchemaProp&  schemaProp,
                                           cpp2::SchemaProp alterSchemaProp,
                                           bool existIndex);

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

    static std::string tagDefaultKey(GraphSpaceID spaceId,
                                     TagID tag,
                                     const std::string& field);

    static std::string edgeDefaultKey(GraphSpaceID spaceId,
                                      EdgeType edge,
                                      const std::string& field);

    static const std::string& defaultPrefix();

    static std::string configKey(const cpp2::ConfigModule& module,
                                 const std::string& name);

    static std::string configKeyPrefix(const cpp2::ConfigModule& module);

    static std::string configValue(const cpp2::ConfigMode& valueMode,
                                   const Value& config);

    static ConfigName parseConfigKey(folly::StringPiece rawData);

    static cpp2::ConfigItem parseConfigValue(folly::StringPiece rawData);

    static std::string snapshotKey(const std::string& name);

    static std::string snapshotVal(const cpp2::SnapshotStatus& status ,
                                   const std::string& hosts);

    static cpp2::SnapshotStatus parseSnapshotStatus(folly::StringPiece rawData);

    static std::string parseSnapshotHosts(folly::StringPiece rawData);

    static std::string parseSnapshotName(folly::StringPiece rawData);

    static const std::string& snapshotPrefix();

    static void upgradeMetaDataV1toV2(nebula::kvstore::KVStore* kv);

    static std::string serializeHostAddr(const HostAddr& host);

    static HostAddr deserializeHostAddr(folly::StringPiece str);
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METAUTILS_H_

