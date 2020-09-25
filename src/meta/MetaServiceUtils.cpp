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

const std::string kSpacesTable         = "__spaces__";         // NOLINT
const std::string kPartsTable          = "__parts__";          // NOLINT
const std::string kHostsTable          = "__hosts__";          // NOLINT
const std::string kTagsTable           = "__tags__";           // NOLINT
const std::string kEdgesTable          = "__edges__";          // NOLINT
const std::string kIndexesTable        = "__indexes__";        // NOLINT
const std::string kIndexTable          = "__index__";          // NOLINT
const std::string kIndexStatusTable    = "__index_status__";   // NOLINT
const std::string kUsersTable          = "__users__";          // NOLINT
const std::string kRolesTable          = "__roles__";          // NOLINT
const std::string kConfigsTable        = "__configs__";        // NOLINT
const std::string kDefaultTable        = "__default__";        // NOLINT
const std::string kSnapshotsTable      = "__snapshots__";      // NOLINT
const std::string kLastUpdateTimeTable = "__last_update_time__"; // NOLINT
const std::string kLeadersTable        = "__leaders__";          // NOLINT
const std::string kDomainsTable        = "__domains__";          // NOLINT
const std::string kIPTable             = "__ip__";               // NOLINT

const std::string kHostOnline  = "Online";       // NOLINT
const std::string kHostOffline = "Offline";      // NOLINT

std::string MetaServiceUtils::lastUpdateTimeKey() {
    std::string key;
    key.reserve(kLastUpdateTimeTable.size());
    key.append(kLastUpdateTimeTable.data(), kLastUpdateTimeTable.size());
    return key;
}

std::string MetaServiceUtils::lastUpdateTimeVal(const int64_t timeInMilliSec) {
    std::string val;
    val.reserve(sizeof(int64_t));
    val.append(reinterpret_cast<const char*>(&timeInMilliSec), sizeof(int64_t));
    return val;
}

std::string MetaServiceUtils::spaceKey(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kSpacesTable.size() + sizeof(GraphSpaceID));
    key.append(kSpacesTable.data(), kSpacesTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return key;
}

std::string MetaServiceUtils::spaceVal(const cpp2::SpaceProperties &properties) {
    std::string val;
    apache::thrift::CompactSerializer::serialize(properties, &val);
    return val;
}

cpp2::SpaceProperties MetaServiceUtils::parseSpace(folly::StringPiece rawData) {
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
    key.reserve(kPartsTable.size() + sizeof(GraphSpaceID) + sizeof(PartitionID));
    key.append(kPartsTable.data(), kPartsTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
    return key;
}

GraphSpaceID MetaServiceUtils::parsePartKeySpaceId(folly::StringPiece key) {
    return *reinterpret_cast<const GraphSpaceID*>(key.data() + kPartsTable.size());
}

PartitionID MetaServiceUtils::parsePartKeyPartId(folly::StringPiece key) {
    return *reinterpret_cast<const PartitionID*>(key.data()
                                                 + kPartsTable.size()
                                                 + sizeof(GraphSpaceID));
}

std::string MetaServiceUtils::partVal(const std::vector<nebula::cpp2::HostAddr>& hosts) {
    std::string val;
    val.reserve(hosts.size() * (sizeof(nebula::cpp2::IPv4) + sizeof(nebula::cpp2::Port)));
    for (auto& h : hosts) {
        val.append(reinterpret_cast<const char*>(&h.ip), sizeof(h.ip))
           .append(reinterpret_cast<const char*>(&h.port), sizeof(h.port));
    }
    return val;
}

std::string MetaServiceUtils::partPrefix(GraphSpaceID spaceId) {
    std::string prefix;
    prefix.reserve(kPartsTable.size() + sizeof(GraphSpaceID));
    prefix.append(kPartsTable.data(), kPartsTable.size())
          .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return prefix;
}

std::string MetaServiceUtils::partPrefix() {
    std::string prefix;
    prefix.reserve(kPartsTable.size() + sizeof(GraphSpaceID));
    prefix.append(kPartsTable.data(), kPartsTable.size());
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
    key.reserve(kHostsTable.size() + sizeof(nebula::cpp2::IPv4) + sizeof(nebula::cpp2::Port));
    key.append(kHostsTable.data(), kHostsTable.size())
       .append(reinterpret_cast<const char*>(&ip), sizeof(ip))
       .append(reinterpret_cast<const char*>(&port), sizeof(port));
    return key;
}

std::string MetaServiceUtils::domainKey(const std::string& domain, Port port) {
    std::string key;
    auto size = domain.size();
    key.reserve(kDomainsTable.size() + sizeof(std::string::size_type) + domain.size() +
                sizeof(Port));
    key.append(kDomainsTable.data(), kDomainsTable.size())
        .append(reinterpret_cast<const char*>(&size), sizeof(std::string::size_type))
        .append(domain.data(), domain.size())
        .append(reinterpret_cast<const char*>(&port), sizeof(port));
    return key;
}

std::string MetaServiceUtils::ipKey(const network::InetAddress& address) {
    std::string key;
    auto ipStr = address.encode();
    key.reserve(kIPTable.size() + ipStr.size());
    key.append(kIPTable).append(ipStr);
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

const std::string& MetaServiceUtils::domainPrefix() {
    return kDomainsTable;
}

const std::string& MetaServiceUtils::ipPrefix() {
    return kIPTable;
}

nebula::cpp2::HostAddr MetaServiceUtils::parseHostKey(folly::StringPiece key) {
    nebula::cpp2::HostAddr host;
    memcpy(&host, key.data() + kHostsTable.size(), sizeof(host));
    return host;
}

folly::StringPiece MetaServiceUtils::parseDomainKey(folly::StringPiece key) {
    return folly::StringPiece(
        key.data() + kDomainsTable.size() + sizeof(std::string::size_type),
        key.size() - kDomainsTable.size() - sizeof(std::string::size_type) - sizeof(Port));
}

nebula::cpp2::HostAddr MetaServiceUtils::parseDomainVal(folly::StringPiece key) {
    nebula::cpp2::HostAddr host;
    memcpy(&host, key.data(), sizeof(host));
    return host;
}

std::string MetaServiceUtils::leaderKey(IPv4 ip, Port port) {
    std::string key;
    key.reserve(kLeadersTable.size() + sizeof(nebula::cpp2::IPv4) + sizeof(nebula::cpp2::Port));
    key.append(kLeadersTable.data(), kLeadersTable.size());
    key.append(reinterpret_cast<const char*>(&ip), sizeof(ip));
    key.append(reinterpret_cast<const char*>(&port), sizeof(port));
    return key;
}

std::string MetaServiceUtils::leaderVal(const LeaderParts& leaderParts) {
    std::string value;
    value.reserve(512);
    for (const auto& spaceEntry : leaderParts) {
        GraphSpaceID spaceId = spaceEntry.first;
        value.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
        size_t leaderCount = spaceEntry.second.size();
        value.append(reinterpret_cast<const char*>(&leaderCount), sizeof(size_t));
        for (const auto& partId : spaceEntry.second) {
            value.append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
        }
    }
    return value;
}

const std::string& MetaServiceUtils::leaderPrefix() {
    return kLeadersTable;
}

nebula::cpp2::HostAddr MetaServiceUtils::parseLeaderKey(folly::StringPiece key) {
    nebula::cpp2::HostAddr host;
    memcpy(&host, key.data() + kLeadersTable.size(), sizeof(host));
    return host;
}

LeaderParts MetaServiceUtils::parseLeaderVal(folly::StringPiece val) {
    LeaderParts leaderParts;
    size_t size = val.size();
    // decode leader info
    size_t offset = 0;
    while (offset + sizeof(GraphSpaceID) + sizeof(size_t) < size) {
        GraphSpaceID spaceId = *reinterpret_cast<const GraphSpaceID*>(val.data() + offset);
        offset += sizeof(GraphSpaceID);
        size_t leaderCount = *reinterpret_cast<const size_t*>(val.data() + offset);
        offset += sizeof(size_t);
        std::vector<PartitionID> partIds;
        for (size_t i = 0; i < leaderCount && offset < size; i++) {
            partIds.emplace_back(*reinterpret_cast<const PartitionID*>(val.data() + offset));
            offset += sizeof(PartitionID);
        }
        leaderParts.emplace(spaceId, std::move(partIds));
    }
    return leaderParts;
}

std::string MetaServiceUtils::schemaEdgePrefix(GraphSpaceID spaceId, EdgeType edgeType) {
    std::string key;
    key.reserve(kEdgesTable.size() + sizeof(GraphSpaceID) + sizeof(edgeType));
    key.append(kEdgesTable.data(), kEdgesTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType));
    return key;
}

std::string MetaServiceUtils::schemaEdgesPrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kEdgesTable.size() + sizeof(GraphSpaceID));
    key.append(kEdgesTable.data(), kEdgesTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return key;
}

std::string MetaServiceUtils::schemaEdgeKey(GraphSpaceID spaceId,
                                            EdgeType edgeType,
                                            SchemaVer version) {
    auto storageVer = std::numeric_limits<SchemaVer>::max() - version;
    std::string key;
    key.reserve(kEdgesTable.size() + sizeof(GraphSpaceID) + sizeof(EdgeType) + sizeof(SchemaVer));
    key.append(kEdgesTable.data(), kEdgesTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(reinterpret_cast<const char*>(&edgeType), sizeof(EdgeType))
       .append(reinterpret_cast<const char*>(&storageVer), sizeof(SchemaVer));
    return key;
}

std::string MetaServiceUtils::schemaEdgeVal(const std::string& name,
                                            const nebula::cpp2::Schema& schema) {
    auto len = name.size();
    std::string val, sval;
    apache::thrift::CompactSerializer::serialize(schema, &sval);
    val.reserve(sizeof(int32_t) + name.size() + sval.size());
    val.append(reinterpret_cast<const char*>(&len), sizeof(int32_t))
       .append(name)
       .append(sval);
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
    key.reserve(kTagsTable.size() + sizeof(GraphSpaceID) + sizeof(TagID) + sizeof(SchemaVer));
    key.append(kTagsTable.data(), kTagsTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID))
       .append(reinterpret_cast<const char*>(&storageVer), sizeof(SchemaVer));
    return key;
}

std::string MetaServiceUtils::schemaTagVal(const std::string& name,
                                           const nebula::cpp2::Schema& schema) {
    int32_t len = name.size();
    std::string val, sval;
    apache::thrift::CompactSerializer::serialize(schema, &sval);
    val.reserve(sizeof(int32_t) + name.size() + sval.size());
    val.append(reinterpret_cast<const char*>(&len), sizeof(int32_t))
       .append(name)
       .append(sval);
    return val;
}

SchemaVer MetaServiceUtils::parseTagVersion(folly::StringPiece key) {
    auto offset = kTagsTable.size() + sizeof(GraphSpaceID) + sizeof(TagID);
    return std::numeric_limits<SchemaVer>::max() -
          *reinterpret_cast<const SchemaVer*>(key.begin() + offset);
}

std::string MetaServiceUtils::schemaTagPrefix(GraphSpaceID spaceId, TagID tagId) {
    std::string key;
    key.reserve(kTagsTable.size() + sizeof(GraphSpaceID) + sizeof(TagID));
    key.append(kTagsTable.data(), kTagsTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
    return key;
}

std::string MetaServiceUtils::schemaTagsPrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kTagsTable.size() + sizeof(GraphSpaceID));
    key.append(kTagsTable.data(), kTagsTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return key;
}

nebula::cpp2::Schema MetaServiceUtils::parseSchema(folly::StringPiece rawData) {
    nebula::cpp2::Schema schema;
    int32_t offset = sizeof(int32_t) + *reinterpret_cast<const int32_t *>(rawData.begin());
    auto schval = rawData.subpiece(offset, rawData.size() - offset);
    apache::thrift::CompactSerializer::deserialize(schval, schema);
    return schema;
}

std::string MetaServiceUtils::indexKey(GraphSpaceID spaceID, IndexID indexID) {
    std::string key;
    key.reserve(sizeof(GraphSpaceID) + sizeof(IndexID));
    key.append(kIndexesTable.data(), kIndexesTable.size());
    key.append(reinterpret_cast<const char*>(&spaceID), sizeof(GraphSpaceID))
       .append(reinterpret_cast<const char*>(&indexID), sizeof(IndexID));
    return key;
}

std::string MetaServiceUtils::indexVal(const nebula::cpp2::IndexItem& item) {
    std::string value;
    apache::thrift::CompactSerializer::serialize(item, &value);
    return value;
}

std::string MetaServiceUtils::indexPrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kIndexesTable.size() + sizeof(GraphSpaceID));
    key.append(kIndexesTable.data(), kIndexesTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return key;
}

nebula::cpp2::IndexItem MetaServiceUtils::parseIndex(const folly::StringPiece& rawData) {
    nebula::cpp2::IndexItem item;
    apache::thrift::CompactSerializer::deserialize(rawData, item);
    return item;
}

// This method should replace with JobManager when it ready.
std::string MetaServiceUtils::rebuildIndexStatus(GraphSpaceID space,
                                                 char type,
                                                 const std::string& indexName) {
    std::string key;
    key.reserve(64);
    key.append(kIndexStatusTable.data(), kIndexStatusTable.size())
       .append(reinterpret_cast<const char*>(&space), sizeof(GraphSpaceID))
       .append(1, type)
       .append(indexName);
    return key;
}

// This method should replace with JobManager when it ready.
std::string MetaServiceUtils::rebuildIndexStatusPrefix(GraphSpaceID space,
                                                       char type) {
    std::string key;
    key.reserve(kIndexStatusTable.size() + sizeof(GraphSpaceID) + sizeof(char));
    key.append(kIndexStatusTable.data(), kIndexStatusTable.size())
       .append(reinterpret_cast<const char*>(&space), sizeof(GraphSpaceID))
       .append(1, type);
    return key;
}

std::string MetaServiceUtils::rebuildIndexStatusPrefix() {
    std::string key;
    key.reserve(kIndexStatusTable.size());
    key.append(kIndexStatusTable.data(), kIndexStatusTable.size());
    return key;
}

std::string MetaServiceUtils::indexSpaceKey(const std::string& name) {
    EntryType type = EntryType::SPACE;
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size())
       .append(reinterpret_cast<const char*>(&type), sizeof(type))
       .append(name);
    return key;
}

std::string MetaServiceUtils::indexTagKey(GraphSpaceID spaceId,
                                          const std::string& name) {
    EntryType type = EntryType::TAG;
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size())
       .append(reinterpret_cast<const char*>(&type), sizeof(type))
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(name);
    return key;
}

std::string MetaServiceUtils::indexEdgeKey(GraphSpaceID spaceId,
                                           const std::string& name) {
    EntryType type = EntryType::EDGE;
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size())
       .append(reinterpret_cast<const char*>(&type), sizeof(type))
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(name);
    return key;
}

std::string MetaServiceUtils::indexIndexKey(GraphSpaceID spaceID,
                                            const std::string& indexName) {
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size());
    EntryType type = EntryType::INDEX;
    key.append(reinterpret_cast<const char*>(&type), sizeof(type))
       .append(reinterpret_cast<const char*>(&spaceID), sizeof(GraphSpaceID))
       .append(indexName);
    return key;
}

std::string MetaServiceUtils::assembleSegmentKey(const std::string& segment,
                                                 const std::string& key) {
    std::string segmentKey;
    segmentKey.reserve(64);
    segmentKey.append(segment)
              .append(key.data(), key.size());
    return segmentKey;
}

cpp2::ErrorCode MetaServiceUtils::alterColumnDefs(std::vector<nebula::cpp2::ColumnDef>& cols,
                                                  nebula::cpp2::SchemaProp&  prop,
                                                  std::vector<kvstore::KV>& defaultKVs,
                                                  std::vector<std::string>& removeDefaultKeys,
                                                  const GraphSpaceID spaceId,
                                                  const TagID Id,
                                                  const nebula::cpp2::ColumnDef col,
                                                  const cpp2::AlterSchemaOp op) {
    static_assert(std::is_same<TagID, EdgeType>::value, "");
    auto name = col.get_name();
    auto dKey = MetaServiceUtils::defaultKey(spaceId, Id, name);
    std::string defaultValue;
    if (col.__isset.default_value) {
        auto value = col.get_default_value();
        switch (col.get_type().get_type()) {
            case nebula::cpp2::SupportedType::BOOL:
                if (value->getType() != nebula::cpp2::Value::Type::bool_value) {
                    LOG(ERROR) << "Handle Column Failed: " << name
                                << " type mismatch";
                    return cpp2::ErrorCode::E_CONFLICT;
                }
                defaultValue = folly::to<std::string>(value->get_bool_value());
                break;
            case nebula::cpp2::SupportedType::INT:
                if (value->getType() != nebula::cpp2::Value::Type::int_value) {
                    LOG(ERROR) << "Handle Column Failed: " << name
                                << " type mismatch";
                    return cpp2::ErrorCode::E_CONFLICT;
                }
                defaultValue = folly::to<std::string>(value->get_int_value());
                break;
            case nebula::cpp2::SupportedType::DOUBLE:
                if (value->getType() != nebula::cpp2::Value::Type::double_value) {
                    LOG(ERROR) << "Handle Column Failed: " << name
                                << " type mismatch";
                    return  cpp2::ErrorCode::E_CONFLICT;
                }
                defaultValue = folly::to<std::string>(value->get_double_value());
                break;
            case nebula::cpp2::SupportedType::STRING:
                if (value->getType() != nebula::cpp2::Value::Type::string_value) {
                    LOG(ERROR) << "Handle Column Failed: " << name
                                << " type mismatch";
                    return cpp2::ErrorCode::E_CONFLICT;
                }
                defaultValue = folly::to<std::string>(value->get_string_value());
                break;
            case nebula::cpp2::SupportedType::TIMESTAMP:
                if (value->getType() != nebula::cpp2::Value::Type::timestamp) {
                    LOG(ERROR) << "Handle Column Failed: " << name
                                << " type mismatch";
                    return cpp2::ErrorCode::E_CONFLICT;
                }
                defaultValue = folly::to<std::string>(value->get_timestamp());
                break;
            default:
                LOG(ERROR) << "Unsupported type";
                return cpp2::ErrorCode::E_CONFLICT;
        }

        LOG(INFO) << "Get Tag Default value: Property Name " << name
                << ", Value " << defaultValue;
    }

    switch (op) {
        case cpp2::AlterSchemaOp::ADD:
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                if (it->get_name() == col.get_name()) {
                    LOG(ERROR) << "Column existing: " << col.get_name();
                    return cpp2::ErrorCode::E_EXISTED;
                }
            }
            cols.emplace_back(std::move(col));
            if (col.__isset.default_value) {
                defaultKVs.emplace_back(std::move(dKey), std::move(defaultValue));
            }
            return cpp2::ErrorCode::SUCCEEDED;
        case cpp2::AlterSchemaOp::CHANGE:
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                auto colName = col.get_name();
                if (colName == it->get_name()) {
                    // If this col is ttl_col, change not allowed
                    if (prop.get_ttl_col() && (*prop.get_ttl_col() == colName)) {
                        LOG(ERROR) << "Column: " << colName << " as ttl_col, change not allowed";
                        return cpp2::ErrorCode::E_UNSUPPORTED;
                    }
                    *it = col;
                    if (col.__isset.default_value) {
                        defaultKVs.emplace_back(std::move(dKey), std::move(defaultValue));
                    } else {
                        removeDefaultKeys.emplace_back(std::move(dKey));
                    }
                    return cpp2::ErrorCode::SUCCEEDED;
                }
            }
            LOG(ERROR) << "Column not found: " << col.get_name();
            return cpp2::ErrorCode::E_NOT_FOUND;
        case cpp2::AlterSchemaOp::DROP:
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                auto colName = col.get_name();
                if (colName == it->get_name()) {
                    if (prop.get_ttl_col() && (*prop.get_ttl_col() == colName)) {
                        prop.set_ttl_duration(0);
                        prop.set_ttl_col("");
                    }
                    cols.erase(it);
                    if (it->__isset.default_value) {
                        removeDefaultKeys.emplace_back(std::move(dKey));
                    }
                    return cpp2::ErrorCode::SUCCEEDED;
                }
            }
            LOG(ERROR) << "Column not found: " << col.get_name();
            return cpp2::ErrorCode::E_NOT_FOUND;
        default:
            LOG(ERROR) << "Alter schema operator not supported";
            return cpp2::ErrorCode::E_UNSUPPORTED;
    }
}

cpp2::ErrorCode MetaServiceUtils::alterSchemaProp(std::vector<nebula::cpp2::ColumnDef>& cols,
                                                  nebula::cpp2::SchemaProp& schemaProp,
                                                  nebula::cpp2::SchemaProp alterSchemaProp,
                                                  bool existIndex) {
    if (existIndex && (alterSchemaProp.__isset.ttl_duration || alterSchemaProp.__isset.ttl_col)) {
        LOG(ERROR) << "Has index, can't set ttl";
        return cpp2::ErrorCode::E_UNSUPPORTED;
    }
    if (alterSchemaProp.__isset.ttl_duration) {
        schemaProp.set_ttl_duration(*alterSchemaProp.get_ttl_duration());
    }
    if (alterSchemaProp.__isset.ttl_col) {
        auto ttlCol = *alterSchemaProp.get_ttl_col();
        // Disable ttl, ttl_col is empty, ttl_duration is 0
        if (ttlCol.empty()) {
            schemaProp.set_ttl_duration(0);
            schemaProp.set_ttl_col(ttlCol);
            return cpp2::ErrorCode::SUCCEEDED;
        }

        auto existed = false;
        for (auto& col : cols) {
            if (col.get_name() == ttlCol) {
                // Only integer and timestamp columns can be used as ttl_col
                if (col.type.type != nebula::cpp2::SupportedType::INT &&
                    col.type.type != nebula::cpp2::SupportedType::TIMESTAMP) {
                    LOG(ERROR) << "TTL column type illegal";
                    return cpp2::ErrorCode::E_UNSUPPORTED;
                }
                existed = true;
                schemaProp.set_ttl_col(ttlCol);
                break;
            }
        }

        if (!existed) {
            LOG(ERROR) << "TTL column not found: " << ttlCol;
            return cpp2::ErrorCode::E_NOT_FOUND;
        }
    }

    // Disable implicit TTL mode
    if ((schemaProp.get_ttl_duration() && (*schemaProp.get_ttl_duration() != 0)) &&
        (!schemaProp.get_ttl_col() || (schemaProp.get_ttl_col() &&
         schemaProp.get_ttl_col()->empty()))) {
        LOG(WARNING) << "Implicit ttl_col not support";
        return cpp2::ErrorCode::E_UNSUPPORTED;
    }

    return cpp2::ErrorCode::SUCCEEDED;
}

std::string MetaServiceUtils::userKey(const std::string& account) {
    std::string key;
    key.reserve(kUsersTable.size() + account.size());
    key.append(kUsersTable.data(), kUsersTable.size())
       .append(account);
    return key;
}

std::string MetaServiceUtils::userVal(const std::string& val) {
    std::string key;
    auto pwdLen = val.size();
    key.reserve(sizeof(int64_t) + pwdLen);
    key.append(reinterpret_cast<const char*>(&pwdLen), sizeof(size_t))
       .append(val);
    return key;
}

std::string MetaServiceUtils::parseUser(folly::StringPiece key) {
    return key.subpiece(kUsersTable.size(), key.size() - kUsersTable.size()).str();
}

std::string MetaServiceUtils::parseUserPwd(folly::StringPiece val) {
    auto len = *reinterpret_cast<const size_t*>(val.data());
    return val.subpiece(sizeof(size_t), len).str();
}

std::string MetaServiceUtils::roleKey(GraphSpaceID spaceId, const std::string& account) {
    std::string key;
    key.reserve(kRolesTable.size() + sizeof(GraphSpaceID) + account.size());
    key.append(kRolesTable.data(), kRolesTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(account);
    return key;
}

std::string MetaServiceUtils::roleVal(nebula::cpp2::RoleType roleType) {
    std::string val;
    val.reserve(sizeof(nebula::cpp2::RoleType));
    val.append(reinterpret_cast<const char*>(&roleType), sizeof(nebula::cpp2::RoleType));
    return val;
}

std::string MetaServiceUtils::parseRoleUser(folly::StringPiece key) {
    auto offset = kRolesTable.size() + sizeof(GraphSpaceID);
    return key.subpiece(offset, key.size() - offset).str();
}

GraphSpaceID MetaServiceUtils::parseRoleSpace(folly::StringPiece key) {
    return *reinterpret_cast<const GraphSpaceID*>(key.data() + kRolesTable.size());
}

std::string MetaServiceUtils::rolesPrefix() {
    return kRolesTable;
}

std::string MetaServiceUtils::roleSpacePrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kRolesTable.size() + sizeof(GraphSpaceID));
    key.append(kRolesTable.data(), kRolesTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return key;
}

std::string MetaServiceUtils::parseRoleStr(folly::StringPiece key) {
    auto type = *reinterpret_cast<const nebula::cpp2::RoleType*>(&key);
    std::string role;
    switch (type) {
        case nebula::cpp2::RoleType::GOD : {
            role = "GOD";
            break;
        }
        case nebula::cpp2::RoleType::ADMIN : {
            role = "ADMIN";
            break;
        }
        case nebula::cpp2::RoleType::DBA : {
            role = "DBA";
            break;
        }
        case nebula::cpp2::RoleType::USER : {
            role = "USER";
            break;
        }
        case nebula::cpp2::RoleType::GUEST : {
            role = "GUEST";
            break;
        }
    }
    return role;
}

std::string MetaServiceUtils::defaultKey(GraphSpaceID spaceId,
                                         TagID id,
                                         const std::string& field) {
    // Assume edge/tag default value key is same formated
    static_assert(std::is_same<TagID, EdgeType>::value, "");
    std::string key;
    key.reserve(kDefaultTable.size() + sizeof(GraphSpaceID) + sizeof(TagID));
    key.append(kDefaultTable.data(), kDefaultTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(reinterpret_cast<const char*>(&id), sizeof(TagID))
       .append(field);
    return key;
}

const std::string& MetaServiceUtils::defaultPrefix() {
    return kDefaultTable;
}

std::string MetaServiceUtils::configKey(const cpp2::ConfigModule& module,
                                        const std::string& name) {
    int32_t nSize = name.size();
    std::string key;
    key.reserve(128);
    key.append(kConfigsTable.data(), kConfigsTable.size())
       .append(reinterpret_cast<const char*>(&module), sizeof(cpp2::ConfigModule))
       .append(reinterpret_cast<const char*>(&nSize), sizeof(int32_t))
       .append(name);
    return key;
}

std::string MetaServiceUtils::configKeyPrefix(const cpp2::ConfigModule& module) {
    std::string key;
    key.reserve(128);
    key.append(kConfigsTable.data(), kConfigsTable.size());
    if (module != cpp2::ConfigModule::ALL) {
        key.append(reinterpret_cast<const char*>(&module), sizeof(cpp2::ConfigModule));
    }
    return key;
}

std::string MetaServiceUtils::configValue(const cpp2::ConfigType& valueType,
                                          const cpp2::ConfigMode& valueMode,
                                          const std::string& config) {
    std::string val;
    val.reserve(sizeof(cpp2::ConfigType) + sizeof(cpp2::ConfigMode) + config.size());
    val.append(reinterpret_cast<const char*>(&valueType), sizeof(cpp2::ConfigType))
       .append(reinterpret_cast<const char*>(&valueMode), sizeof(cpp2::ConfigMode))
       .append(config);
    return val;
}

ConfigName MetaServiceUtils::parseConfigKey(folly::StringPiece rawKey) {
    std::string key;
    auto offset = kConfigsTable.size();
    auto module = *reinterpret_cast<const cpp2::ConfigModule*>(rawKey.data() + offset);
    offset += sizeof(cpp2::ConfigModule);
    int32_t nSize = *reinterpret_cast<const int32_t*>(rawKey.data() + offset);
    offset += sizeof(int32_t);
    auto name = rawKey.subpiece(offset, nSize);
    return {module, name.str()};
}

cpp2::ConfigItem MetaServiceUtils::parseConfigValue(folly::StringPiece rawData) {
    int32_t offset = 0;
    cpp2::ConfigType type = *reinterpret_cast<const cpp2::ConfigType*>(rawData.data() + offset);
    offset += sizeof(cpp2::ConfigType);
    cpp2::ConfigMode mode = *reinterpret_cast<const cpp2::ConfigMode*>(rawData.data() + offset);
    offset += sizeof(cpp2::ConfigMode);
    auto value = rawData.subpiece(offset, rawData.size() - offset);

    cpp2::ConfigItem item;
    item.set_type(type);
    item.set_mode(mode);
    item.set_value(value.str());
    return item;
}

std::string MetaServiceUtils::snapshotKey(const std::string& name) {
    std::string key;
    key.reserve(kSnapshotsTable.size() + name.size());
    key.append(kSnapshotsTable.data(), kSnapshotsTable.size())
       .append(name);
    return key;
}

std::string MetaServiceUtils::snapshotVal(const cpp2::SnapshotStatus& status,
                                          const std::string& hosts) {
    std::string val;
    val.reserve(sizeof(cpp2::SnapshotStatus) + sizeof(hosts));
    val.append(reinterpret_cast<const char*>(&status), sizeof(cpp2::SnapshotStatus))
       .append(hosts);
    return val;
}

cpp2::SnapshotStatus MetaServiceUtils::parseSnapshotStatus(folly::StringPiece rawData) {
    return *reinterpret_cast<const cpp2::SnapshotStatus*>(rawData.data());
}

std::string MetaServiceUtils::parseSnapshotHosts(folly::StringPiece rawData) {
    return rawData.subpiece(sizeof(cpp2::SnapshotStatus),
                            rawData.size() - sizeof(cpp2::SnapshotStatus)).str();
}

std::string MetaServiceUtils::parseSnapshotName(folly::StringPiece rawData) {
    int32_t offset = kSnapshotsTable.size();
    return rawData.subpiece(offset, rawData.size() - offset).str();
}

const std::string& MetaServiceUtils::snapshotPrefix() {
    return kSnapshotsTable;
}

}  // namespace meta
}  // namespace nebula
