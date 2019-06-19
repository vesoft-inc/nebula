/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>

namespace nebula {
namespace meta {

const std::string kSpacesTable = "__spaces__";  // NOLINT
const std::string kPartsTable  = "__parts__";   // NOLINT
const std::string kHostsTable  = "__hosts__";   // NOLINT
const std::string kTagsTable   = "__tags__";    // NOLINT
const std::string kEdgesTable  = "__edges__";   // NOLINT
const std::string kIndexTable  = "__index__";   // NOLINT
const std::string kUsersTable  = "__users__";    // NOLINT
const std::string kRolesTable  = "__roles__";    // NOLINT

const std::string kHostOnline = "Online";       // NOLINT
const std::string kHostOffline = "Offline";     // NOLINT

std::string MetaServiceUtils::spaceKey(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(128);
    key.append(kSpacesTable.data(), kSpacesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    return key;
}

std::string MetaServiceUtils::spaceVal(const cpp2::SpaceProperties &properties) {
    std::string val;
    apache::thrift::CompactSerializer::serialize(properties, &val);
    return val;
}

cpp2::SpaceProperties  MetaServiceUtils::parseSpace(folly::StringPiece rawData) {
    cpp2::SpaceProperties properties;
    apache::thrift::CompactSerializer::deserialize(rawData, properties);
    return properties;
}

const std::string& MetaServiceUtils::spacePrefix() {
    return kSpacesTable;
}

GraphSpaceID MetaServiceUtils::spaceId(folly::StringPiece rawKey) {
    return *reinterpret_cast<const GraphSpaceID*>(rawKey.data() + kSpacesTable.size());
}

std::string MetaServiceUtils::spaceName(folly::StringPiece rawVal) {
    return parseSpace(rawVal).get_space_name();
}

std::string MetaServiceUtils::partKey(GraphSpaceID spaceId, PartitionID partId) {
    std::string key;
    key.reserve(128);
    key.append(kPartsTable.data(), kPartsTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    key.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
    return key;
}

std::string MetaServiceUtils::partVal(const std::vector<nebula::cpp2::HostAddr>& hosts) {
    std::string val;
    val.reserve(128);
    for (auto& h : hosts) {
        val.append(reinterpret_cast<const char*>(&h.ip), sizeof(h.ip));
        val.append(reinterpret_cast<const char*>(&h.port), sizeof(h.port));
    }
    return val;
}

std::string MetaServiceUtils::partPrefix(GraphSpaceID spaceId) {
    std::string prefix;
    prefix.reserve(128);
    prefix.append(kPartsTable.data(), kPartsTable.size());
    prefix.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return prefix;
}

std::vector<nebula::cpp2::HostAddr> MetaServiceUtils::parsePartVal(folly::StringPiece val) {
    std::vector<nebula::cpp2::HostAddr> hosts;
    static const size_t unitSize = sizeof(int32_t) * 2;
    auto hostsNum = val.size() / unitSize;
    hosts.reserve(hostsNum);
    VLOG(3) << "Total size:" << val.size()
            << ", host size:" << unitSize
            << ", host num:" << hostsNum;
    for (decltype(hostsNum) i = 0; i < hostsNum; i++) {
        nebula::cpp2::HostAddr h;
        h.set_ip(*reinterpret_cast<const int32_t*>(val.data() + i * unitSize));
        h.set_port(*reinterpret_cast<const int32_t*>(val.data() + i * unitSize + sizeof(int32_t)));
        hosts.emplace_back(std::move(h));
    }
    return hosts;
}

std::string MetaServiceUtils::hostKey(IPv4 ip, Port port) {
    std::string key;
    key.reserve(128);
    key.append(kHostsTable.data(), kHostsTable.size());
    key.append(reinterpret_cast<const char*>(&ip), sizeof(ip));
    key.append(reinterpret_cast<const char*>(&port), sizeof(port));
    return key;
}

std::string MetaServiceUtils::hostValOnline() {
    return kHostOnline;
}

std::string MetaServiceUtils::hostValOffline() {
    return kHostOffline;
}

const std::string& MetaServiceUtils::hostPrefix() {
    return kHostsTable;
}

nebula::cpp2::HostAddr MetaServiceUtils::parseHostKey(folly::StringPiece key) {
    nebula::cpp2::HostAddr host;
    memcpy(&host, key.data() + kHostsTable.size(), sizeof(host));
    return host;
}

std::string MetaServiceUtils::schemaEdgePrefix(GraphSpaceID spaceId, EdgeType edgeType) {
    std::string key;
    key.reserve(128);
    key.append(kEdgesTable.data(), kEdgesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    key.append(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType));
    return key;
}

std::string MetaServiceUtils::schemaEdgesPrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kEdgesTable.size() + sizeof(GraphSpaceID));
    key.append(kEdgesTable.data(), kEdgesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    return key;
}

std::string MetaServiceUtils::schemaEdgeKey(GraphSpaceID spaceId,
                                            EdgeType edgeType,
                                            SchemaVer version) {
    auto storageVer = std::numeric_limits<SchemaVer>::max() - version;
    std::string key;
    key.reserve(128);
    key.append(kEdgesTable.data(), kEdgesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    key.append(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType));
    key.append(reinterpret_cast<const char*>(&storageVer), sizeof(storageVer));
    return key;
}

std::string MetaServiceUtils::schemaEdgeVal(const std::string& name, nebula::cpp2::Schema schema) {
    auto len = name.size();
    std::string val, sval;
    apache::thrift::CompactSerializer::serialize(schema, &sval);
    val.reserve(sizeof(int32_t) + name.size() + sval.size());
    val.append(reinterpret_cast<const char*>(&len), sizeof(int32_t));
    val.append(name);
    val.append(sval);
    return val;
}

SchemaVer MetaServiceUtils::parseEdgeVersion(folly::StringPiece key) {
    auto offset = kEdgesTable.size() + sizeof(GraphSpaceID) + sizeof(EdgeType);
    return std::numeric_limits<SchemaVer>::max() -
          *reinterpret_cast<const SchemaVer*>(key.begin() + offset);
}

std::string MetaServiceUtils::schemaTagKey(GraphSpaceID spaceId, TagID tagId, SchemaVer version) {
    auto storageVer = std::numeric_limits<SchemaVer>::max() - version;
    std::string key;
    key.reserve(128);
    key.append(kTagsTable.data(), kTagsTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    key.append(reinterpret_cast<const char*>(&tagId), sizeof(tagId));
    key.append(reinterpret_cast<const char*>(&storageVer), sizeof(version));
    return key;
}

SchemaVer MetaServiceUtils::parseTagVersion(folly::StringPiece key) {
    auto offset = kTagsTable.size() + sizeof(GraphSpaceID) + sizeof(TagID);
    return std::numeric_limits<SchemaVer>::max() -
          *reinterpret_cast<const SchemaVer*>(key.begin() + offset);
}

std::string MetaServiceUtils::schemaTagPrefix(GraphSpaceID spaceId, TagID tagId) {
    std::string key;
    key.reserve(128);
    key.append(kTagsTable.data(), kTagsTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    key.append(reinterpret_cast<const char*>(&tagId), sizeof(tagId));
    return key;
}

std::string MetaServiceUtils::schemaTagsPrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kTagsTable.size() + sizeof(GraphSpaceID));
    key.append(kTagsTable.data(), kTagsTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(spaceId));
    return key;
}

std::string MetaServiceUtils::schemaTagVal(const std::string& name, nebula::cpp2::Schema schema) {
    int32_t len = name.size();
    std::string val, sval;
    apache::thrift::CompactSerializer::serialize(schema, &sval);
    val.reserve(sizeof(int32_t) + name.size() + sval.size());
    val.append(reinterpret_cast<const char*>(&len), sizeof(int32_t));
    val.append(name);
    val.append(sval);
    return val;
}

nebula::cpp2::Schema MetaServiceUtils::parseSchema(folly::StringPiece rawData) {
    nebula::cpp2::Schema schema;
    int32_t offset = sizeof(int32_t) + *reinterpret_cast<const int32_t *>(rawData.begin());
    auto schval = rawData.subpiece(offset, rawData.size() - offset);
    apache::thrift::CompactSerializer::deserialize(schval, schema);
    return schema;
}

std::string MetaServiceUtils::indexSpaceKey(const std::string& name) {
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size());
    EntryType type = EntryType::SPACE;
    key.append(reinterpret_cast<const char*>(&type), sizeof(type));
    key.append(name);
    return key;
}

std::string MetaServiceUtils::indexTagKey(GraphSpaceID spaceId,
                                          const std::string& name) {
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size());
    EntryType type = EntryType::TAG;
    key.append(reinterpret_cast<const char*>(&type), sizeof(type));
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    key.append(name);
    return key;
}

std::string MetaServiceUtils::indexEdgeKey(GraphSpaceID spaceId,
                                           const std::string& name) {
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size());
    EntryType type = EntryType::EDGE;
    key.append(reinterpret_cast<const char*>(&type), sizeof(type));
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    key.append(name);
    return key;
}

std::string MetaServiceUtils::assembleSegmentKey(const std::string& segment,
                                                 const std::string& key) {
    std::string segmentKey;
    segmentKey.reserve(64);
    segmentKey.append(segment);
    segmentKey.append(key.data(), key.size());
    return segmentKey;
}

cpp2::ErrorCode MetaServiceUtils::alterColumnDefs(std::vector<nebula::cpp2::ColumnDef>& cols,
                                                  const nebula::cpp2::ColumnDef col,
                                                  const cpp2::AlterSchemaOp op) {
    switch (op) {
        case cpp2::AlterSchemaOp::ADD :
        {
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                if (it->get_name() == col.get_name()) {
                    LOG(WARNING) << "Column existing : " << col.get_name();
                    return cpp2::ErrorCode::E_EXISTED;
                }
            }
            cols.emplace_back(std::move(col));
            return cpp2::ErrorCode::SUCCEEDED;
        }
        case cpp2::AlterSchemaOp::CHANGE :
        {
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                if (col.get_name() == it->get_name()) {
                    *it = col;
                    return cpp2::ErrorCode::SUCCEEDED;
                }
            }
            break;
        }
        case cpp2::AlterSchemaOp::DROP :
        {
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                if (col.get_name() == it->get_name()) {
                    cols.erase(it);
                    return cpp2::ErrorCode::SUCCEEDED;
                }
            }
            break;
        }
        default :
            return cpp2::ErrorCode::E_UNKNOWN;
    }
    LOG(WARNING) << "Column not found : " << col.get_name();
    return cpp2::ErrorCode::E_NOT_FOUND;
}

std::string MetaServiceUtils::indexUserKey(const std::string& account) {
    std::string key;
    EntryType type = EntryType::USER;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size());
    key.append(reinterpret_cast<const char*>(&type), sizeof(type));
    key.append(account);
    return key;
}

std::string MetaServiceUtils::userKey(UserID userId) {
    std::string key;
    key.reserve(64);
    key.append(kUsersTable.data(), kUsersTable.size());
    key.append(reinterpret_cast<const char*>(&userId), sizeof(userId));
    return key;
}

std::string MetaServiceUtils::userVal(const std::string& password,
                                      const cpp2::UserItem& userItem) {
    auto len = password.size();
    std::string val, userVal;
    apache::thrift::CompactSerializer::serialize(userItem, &userVal);
    val.reserve(sizeof(int32_t) + len + userVal.size());
    val.append(reinterpret_cast<const char*>(&len), sizeof(int32_t));
    val.append(password);
    val.append(userVal);
    return val;
}

folly::StringPiece MetaServiceUtils::userItemVal(folly::StringPiece rawVal) {
    auto offset = sizeof(int32_t) + *reinterpret_cast<const int32_t *>(rawVal.begin());
    return rawVal.subpiece(offset, rawVal.size() - offset);
}

std::string MetaServiceUtils::replaceUserVal(const cpp2::UserItem& user, folly::StringPiece val) {
    cpp2:: UserItem oldUser;
    apache::thrift::CompactSerializer::deserialize(userItemVal(val), oldUser);
    if (user.__isset.is_lock) {
        oldUser.set_is_lock(user.get_is_lock());
    }
    if (user.__isset.max_queries_per_hour) {
        oldUser.set_max_queries_per_hour(user.get_max_queries_per_hour());
    }
    if (user.__isset.max_updates_per_hour) {
        oldUser.set_max_updates_per_hour(user.get_max_updates_per_hour());
    }
    if (user.__isset.max_connections_per_hour) {
        oldUser.set_max_connections_per_hour(user.get_max_connections_per_hour());
    }
    if (user.__isset.max_user_connections) {
        oldUser.set_max_user_connections(user.get_max_user_connections());
    }

    std::string newVal, userVal;
    apache::thrift::CompactSerializer::serialize(oldUser, &userVal);
    auto len = sizeof(int32_t) + *reinterpret_cast<const int32_t *>(val.begin());
    newVal.reserve(len + userVal.size());
    newVal.append(val.subpiece(0, len).str());
    newVal.append(userVal);
    return newVal;
}

std::string MetaServiceUtils::roleKey(GraphSpaceID spaceId, UserID userId) {
    std::string key;
    key.reserve(64);
    key.append(kRolesTable.data(), kRolesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    key.append(reinterpret_cast<const char*>(&userId), sizeof(UserID));
    return key;
}

std::string MetaServiceUtils::roleVal(cpp2::RoleType roleType) {
    std::string val;
    val.reserve(64);
    val.append(reinterpret_cast<const char*>(&roleType), sizeof(roleType));
    return val;
}

std::string MetaServiceUtils::changePassword(folly::StringPiece val, folly::StringPiece newPwd) {
    auto pwdLen = newPwd.size();
    auto len = sizeof(int32_t) + *reinterpret_cast<const int32_t *>(val.begin());
    auto userVal = val.subpiece(len, val.size() - len);
    std::string newVal;
    newVal.reserve(sizeof(int32_t) + pwdLen+ userVal.size());
    newVal.append(reinterpret_cast<const char*>(&pwdLen), sizeof(int32_t));
    newVal.append(newPwd.str());
    newVal.append(userVal.str());
    return newVal;
}

cpp2::UserItem MetaServiceUtils::parseUserItem(folly::StringPiece val) {
    cpp2:: UserItem user;
    apache::thrift::CompactSerializer::deserialize(userItemVal(val), user);
    return user;
}

std::string MetaServiceUtils::rolesPrefix() {
    return kRolesTable;
}

std::string MetaServiceUtils::roleSpacePrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(64);
    key.append(kRolesTable.data(), kRolesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return key;
}

UserID MetaServiceUtils::parseRoleUserId(folly::StringPiece val) {
    return *reinterpret_cast<const UserID *>(val.begin() +
                                             kRolesTable.size() +
                                             sizeof(GraphSpaceID));
}

UserID MetaServiceUtils::parseUserId(folly::StringPiece val) {
    return *reinterpret_cast<const UserID *>(val.begin() +
                                             kUsersTable.size());
}

}  // namespace meta
}  // namespace nebula
