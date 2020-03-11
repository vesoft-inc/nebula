/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METAUTILS_H_
#define META_METAUTILS_H_

#include "base/Base.h"
#include "base/Status.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace meta {

enum class EntryType : int8_t {
    SPACE       = 0x01,
    TAG         = 0x02,
    EDGE        = 0x03,
    USER        = 0x04,
    INDEX       = 0x05,
    CONFIG      = 0x07,
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

    static std::string partVal(const std::vector<nebula::cpp2::HostAddr>& hosts);

    static const std::string& partPrefix();

    static std::string partPrefix(GraphSpaceID spaceId);

    static std::vector<nebula::cpp2::HostAddr> parsePartVal(folly::StringPiece val);

    static std::string hostKey(IPv4 ip, Port port);

    static std::string hostValOnline();

    static std::string hostValOffline();

    static const std::string& hostPrefix();

    static nebula::cpp2::HostAddr parseHostKey(folly::StringPiece key);

    static std::string leaderKey(IPv4 ip, Port port);

    static std::string leaderVal(const LeaderParts& leaderParts);

    static const std::string& leaderPrefix();

    static nebula::cpp2::HostAddr parseLeaderKey(folly::StringPiece key);

    static LeaderParts parseLeaderVal(folly::StringPiece val);

    static std::string schemaEdgePrefix(GraphSpaceID spaceId, EdgeType edgeType);

    static std::string schemaEdgesPrefix(GraphSpaceID spaceId);

    static std::string schemaEdgeKey(GraphSpaceID spaceId, EdgeType edgeType, SchemaVer version);

    static std::string schemaEdgeVal(const std::string& name, const nebula::cpp2::Schema& schema);

    static SchemaVer parseEdgeVersion(folly::StringPiece key);

    static std::string schemaTagKey(GraphSpaceID spaceId, TagID tagId, SchemaVer version);

    static std::string schemaTagVal(const std::string& name, const nebula::cpp2::Schema& schema);

    static SchemaVer parseTagVersion(folly::StringPiece key);

    static std::string schemaTagPrefix(GraphSpaceID spaceId, TagID tagId);

    static std::string schemaTagsPrefix(GraphSpaceID spaceId);

    static nebula::cpp2::Schema parseSchema(folly::StringPiece rawData);

    static std::string indexKey(GraphSpaceID spaceId, IndexID indexID);

    static std::string indexVal(const nebula::cpp2::IndexItem& item);

    static std::string indexPrefix(GraphSpaceID spaceId);

    static nebula::cpp2::IndexItem parseIndex(const folly::StringPiece& rawData);

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

    static cpp2::ErrorCode alterColumnDefs(std::vector<nebula::cpp2::ColumnDef>& cols,
                                           nebula::cpp2::SchemaProp&  prop,
                                           const nebula::cpp2::ColumnDef col,
                                           const cpp2::AlterSchemaOp op);

    static cpp2::ErrorCode alterSchemaProp(std::vector<nebula::cpp2::ColumnDef>& cols,
                                           nebula::cpp2::SchemaProp&  schemaProp,
                                           nebula::cpp2::SchemaProp alterSchemaProp,
                                           bool existIndex);

    static std::string indexUserKey(const std::string& account);

    static std::string userKey(UserID userId);

    static std::string userVal(const std::string& password,
                               const cpp2::UserItem& userItem);

    static folly::StringPiece userItemVal(folly::StringPiece rawVal);

    static std::string replaceUserVal(const cpp2::UserItem& user, folly::StringPiece rawVal);

    static std::string roleKey(GraphSpaceID spaceId, UserID userId);

    static std::string roleVal(cpp2::RoleType roleType);

    static std::string changePassword(folly::StringPiece val, folly::StringPiece newPwd);

    static cpp2::UserItem parseUserItem(folly::StringPiece val);

    static std::string rolesPrefix();

    static std::string roleSpacePrefix(GraphSpaceID spaceId);

    static UserID parseRoleUserId(folly::StringPiece val);

    static UserID parseUserId(folly::StringPiece val);

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

    static std::string configValue(const cpp2::ConfigType& valueType,
                                   const cpp2::ConfigMode& valueMode,
                                   const std::string& config);

    static ConfigName parseConfigKey(folly::StringPiece rawData);

    static cpp2::ConfigItem parseConfigValue(folly::StringPiece rawData);

    static std::string snapshotKey(const std::string& name);

    static std::string snapshotVal(const cpp2::SnapshotStatus& status ,
                                   const std::string& hosts);

    static cpp2::SnapshotStatus parseSnapshotStatus(folly::StringPiece rawData);

    static std::string parseSnapshotHosts(folly::StringPiece rawData);

    static std::string parseSnapshotName(folly::StringPiece rawData);

    static const std::string& snapshotPrefix();
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METAUTILS_H_

