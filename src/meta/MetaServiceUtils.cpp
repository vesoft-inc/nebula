/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include "common/network/NetworkUtils.h"
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
const std::string kGroupsTable         = "__groups__";           // NOLINT
const std::string kZonesTable          = "__zones__";            // NOLINT
const std::string kListenerTable       = "__listener__";         // NOLINT


const std::string kHostOnline  = "Online";       // NOLINT
const std::string kHostOffline = "Offline";      // NOLINT

const int kMaxIpAddrLen = 15;   // '255.255.255.255'

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

std::string MetaServiceUtils::spaceVal(const cpp2::SpaceDesc &spaceDesc) {
    std::string val;
    apache::thrift::CompactSerializer::serialize(spaceDesc, &val);
    return val;
}

cpp2::SpaceDesc MetaServiceUtils::parseSpace(folly::StringPiece rawData) {
    cpp2::SpaceDesc spaceDesc;
    apache::thrift::CompactSerializer::deserialize(rawData, spaceDesc);
    return spaceDesc;
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

std::string MetaServiceUtils::partVal(const std::vector<HostAddr>& hosts) {
    return partValV2(hosts);
}

// dataVer(int) + vectorSize(size_t) + vector of (strIp(string) + port(int))
std::string MetaServiceUtils::partValV2(const std::vector<HostAddr>& hosts) {
    std::string encodedVal;
    int dataVersion = 2;
    encodedVal.append(reinterpret_cast<const char*>(&dataVersion), sizeof(int))
              .append(network::NetworkUtils::toHostsStr(hosts));
    return encodedVal;
}

std::string MetaServiceUtils::partPrefix() {
    std::string prefix;
    prefix.reserve(kPartsTable.size() + sizeof(GraphSpaceID));
    prefix.append(kPartsTable.data(), kPartsTable.size());
    return prefix;
}

std::string MetaServiceUtils::partPrefix(GraphSpaceID spaceId) {
    std::string prefix;
    prefix.reserve(kPartsTable.size() + sizeof(GraphSpaceID));
    prefix.append(kPartsTable.data(), kPartsTable.size())
          .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return prefix;
}

std::vector<HostAddr> MetaServiceUtils::parsePartVal(folly::StringPiece val, int partNum) {
    static const size_t unitSizeV1 = sizeof(int64_t);
    if (unitSizeV1 * partNum == val.size()) {
        return parsePartValV1(val);
    }
    int dataVer = *reinterpret_cast<const int32_t*>(val.data());
    UNUSED(dataVer);  // currently if not ver1, it must be v2
    val.advance(sizeof(int));
    return parsePartValV2(val);
}

// partion val is ip(int) + port(int)
std::vector<HostAddr> MetaServiceUtils::parsePartValV1(folly::StringPiece val) {
    std::vector<HostAddr> hosts;
    static const size_t unitSize = sizeof(int32_t) * 2;
    auto hostsNum = val.size() / unitSize;
    hosts.reserve(hostsNum);
    VLOG(3) << "Total size:" << val.size()
            << ", host size:" << unitSize
            << ", host num:" << hostsNum;
    for (decltype(hostsNum) i = 0; i < hostsNum; i++) {
        HostAddr h;
        uint32_t ip = *reinterpret_cast<const int32_t*>(val.data() + i * unitSize);
        h.host = network::NetworkUtils::intToIPv4(ip);
        h.port = *reinterpret_cast<const int32_t*>(val.data() + i * unitSize + sizeof(int32_t));
        hosts.emplace_back(std::move(h));
    }
    return hosts;
}

// dataVer(int) + vectorSize(size_t) + vector of (strIpV4(string) + port(int))
std::vector<HostAddr> MetaServiceUtils::parsePartValV2(folly::StringPiece val) {
    std::vector<HostAddr> ret;
    auto hostsOrErr = network::NetworkUtils::toHosts(val.str());
    if (hostsOrErr.ok()) {
        ret = std::move(hostsOrErr.value());
    } else {
        LOG(ERROR) << "invalid input for parsePartValV2()";
    }
    return ret;
}

std::string MetaServiceUtils::hostKey(std::string addr, Port port) {
    return hostKeyV2(addr, port);
}

std::string MetaServiceUtils::hostKeyV2(std::string addr, Port port) {
    std::string key;
    HostAddr h(addr, port);
    key.append(kHostsTable.data(), kHostsTable.size())
       .append(MetaServiceUtils::serializeHostAddr(h));
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

HostAddr MetaServiceUtils::parseHostKey(folly::StringPiece key) {
    if (key.size() == kHostsTable.size() + sizeof(int64_t)) {
        return parseHostKeyV1(key);
    }
    return parseHostKeyV2(key);
}

HostAddr MetaServiceUtils::parseHostKeyV1(folly::StringPiece key) {
    HostAddr host;
    key.advance(kHostsTable.size());
    uint32_t ip = *reinterpret_cast<const uint32_t*>(key.begin());
    host.host = network::NetworkUtils::intToIPv4(ip);
    host.port = *reinterpret_cast<const int32_t*>(key.begin() + sizeof(uint32_t));
    return host;
}

HostAddr MetaServiceUtils::parseHostKeyV2(folly::StringPiece key) {
    key.advance(kHostsTable.size());
    return MetaServiceUtils::deserializeHostAddr(key);
}

std::string MetaServiceUtils::leaderKey(std::string addr, Port port) {
    return leaderKeyV2(addr, port);
}

std::string MetaServiceUtils::leaderKeyV2(std::string addr, Port port) {
    std::string key;
    HostAddr h(addr, port);

    key.reserve(kLeadersTable.size() + kMaxIpAddrLen + sizeof(Port));
    key.append(kLeadersTable.data(), kLeadersTable.size())
       .append(MetaServiceUtils::serializeHostAddr(h));
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

HostAddr MetaServiceUtils::parseLeaderKey(folly::StringPiece key) {
    if (key.size() == kLeadersTable.size() + sizeof(int64_t)) {
        return parseLeaderKeyV1(key);
    }
    return parseLeaderKeyV2(key);
}

// input should be a pair of int32_t
HostAddr MetaServiceUtils::parseLeaderKeyV1(folly::StringPiece key) {
    HostAddr host;
    CHECK_EQ(key.size(), kLeadersTable.size() + sizeof(int64_t));
    key.advance(kLeadersTable.size());
    auto ip = *reinterpret_cast<const uint32_t*>(key.begin());
    host.host = network::NetworkUtils::intToIPv4(ip);
    host.port = *reinterpret_cast<const uint32_t*>(key.begin() + sizeof(ip));
    return host;
}

HostAddr MetaServiceUtils::parseLeaderKeyV2(folly::StringPiece key) {
    key.advance(kLeadersTable.size());
    return MetaServiceUtils::deserializeHostAddr(key);
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

std::string MetaServiceUtils::schemaVal(const std::string& name,
                                        const cpp2::Schema& schema) {
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

cpp2::Schema MetaServiceUtils::parseSchema(folly::StringPiece rawData) {
    cpp2::Schema schema;
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

std::string MetaServiceUtils::indexVal(const nebula::meta::cpp2::IndexItem& item) {
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

nebula::meta::cpp2::IndexItem MetaServiceUtils::parseIndex(const folly::StringPiece& rawData) {
    nebula::meta::cpp2::IndexItem item;
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
    key.reserve(64);
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
    EntryType type = EntryType::INDEX;
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size())
       .append(reinterpret_cast<const char*>(&type), sizeof(type))
       .append(reinterpret_cast<const char*>(&spaceID), sizeof(GraphSpaceID))
       .append(indexName);
    return key;
}

std::string MetaServiceUtils::indexGroupKey(const std::string& name) {
    EntryType type = EntryType::GROUP;
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size())
       .append(reinterpret_cast<const char*>(&type), sizeof(type))
       .append(name);
    return key;
}

std::string MetaServiceUtils::indexZoneKey(const std::string& name) {
    EntryType type = EntryType::ZONE;
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size())
       .append(reinterpret_cast<const char*>(&type), sizeof(type))
       .append(name);
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

cpp2::ErrorCode MetaServiceUtils::alterColumnDefs(std::vector<cpp2::ColumnDef>& cols,
                                                  cpp2::SchemaProp&  prop,
                                                  const cpp2::ColumnDef col,
                                                  const cpp2::AlterSchemaOp op) {
    switch (op) {
        case cpp2::AlterSchemaOp::ADD:
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                if (it->get_name() == col.get_name()) {
                    LOG(ERROR) << "Column existing: " << col.get_name();
                    return cpp2::ErrorCode::E_EXISTED;
                }
            }
            cols.emplace_back(std::move(col));
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

cpp2::ErrorCode MetaServiceUtils::alterSchemaProp(std::vector<cpp2::ColumnDef>& cols,
                                                  cpp2::SchemaProp& schemaProp,
                                                  cpp2::SchemaProp alterSchemaProp,
                                                  bool existIndex) {
    if (existIndex && (alterSchemaProp.__isset.ttl_duration || alterSchemaProp.__isset.ttl_col)) {
        LOG(ERROR) << "Has index, can't set ttl";
        return cpp2::ErrorCode::E_UNSUPPORTED;
    }
    if (alterSchemaProp.__isset.ttl_duration) {
        // Graph check  <=0 to = 0
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
                auto colType = col.get_type().get_type();
                // Only integer and timestamp columns can be used as ttl_col
                if (colType != cpp2::PropertyType::INT64 &&
                    colType != cpp2::PropertyType::TIMESTAMP) {
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

std::string MetaServiceUtils::roleVal(cpp2::RoleType roleType) {
    std::string val;
    val.reserve(sizeof(cpp2::RoleType));
    val.append(reinterpret_cast<const char*>(&roleType), sizeof(cpp2::RoleType));
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
    auto type = *reinterpret_cast<const cpp2::RoleType*>(&key);
    std::string role;
    switch (type) {
        case cpp2::RoleType::GOD : {
            role = "GOD";
            break;
        }
        case cpp2::RoleType::ADMIN : {
            role = "ADMIN";
            break;
        }
        case cpp2::RoleType::DBA : {
            role = "DBA";
            break;
        }
        case cpp2::RoleType::USER : {
            role = "USER";
            break;
        }
        case cpp2::RoleType::GUEST : {
            role = "GUEST";
            break;
        }
    }
    return role;
}

std::string MetaServiceUtils::tagDefaultKey(GraphSpaceID spaceId,
                                            TagID tag,
                                            const std::string& field) {
    std::string key;
    key.reserve(kDefaultTable.size() + sizeof(GraphSpaceID) + sizeof(TagID));
    key.append(kDefaultTable.data(), kDefaultTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(reinterpret_cast<const char*>(&tag), sizeof(TagID))
       .append(field);
    return key;
}

std::string MetaServiceUtils::edgeDefaultKey(GraphSpaceID spaceId,
                                             EdgeType edge,
                                             const std::string& field) {
    std::string key;
    key.reserve(kDefaultTable.size() + sizeof(GraphSpaceID) + sizeof(EdgeType));
    key.append(kDefaultTable.data(), kDefaultTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(reinterpret_cast<const char*>(&edge), sizeof(EdgeType))
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

std::string MetaServiceUtils::configValue(const cpp2::ConfigMode& valueMode,
                                          const Value& value) {
    std::string val, cVal;
    apache::thrift::CompactSerializer::serialize(value, &cVal);
    val.reserve(sizeof(cpp2::ConfigMode) + cVal.size());
    val.append(reinterpret_cast<const char*>(&valueMode), sizeof(cpp2::ConfigMode))
       .append(cVal);
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
    cpp2::ConfigMode mode = *reinterpret_cast<const cpp2::ConfigMode*>(rawData.data() + offset);
    offset += sizeof(cpp2::ConfigMode);
    Value value;
    apache::thrift::CompactSerializer::deserialize(
            rawData.subpiece(offset, rawData.size() - offset), value);

    cpp2::ConfigItem item;
    item.set_mode(mode);
    item.set_value(value);
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

// when do nebula upgrade, some format data in sys table may change
void MetaServiceUtils::upgradeMetaDataV1toV2(nebula::kvstore::KVStore* kv) {
    const int kDefaultSpaceId = 0;
    const int kDefaultPartId = 0;
    auto suc = nebula::kvstore::ResultCode::SUCCEEDED;
    using nebula::meta::MetaServiceUtils;
    CHECK_NOTNULL(kv);
    std::vector<std::string> removeData;
    std::vector<nebula::kvstore::KV> data;
    {
        // 1. kPartsTable
        const auto& spacePrefix = nebula::meta::MetaServiceUtils::spacePrefix();
        std::unique_ptr<nebula::kvstore::KVIterator> itSpace;
        if (kv->prefix(kDefaultSpaceId, kDefaultPartId, spacePrefix, &itSpace) != suc) {
            return;
        }
        while (itSpace->valid()) {
            auto spaceId = MetaServiceUtils::spaceId(itSpace->key());
            auto partPrefix = MetaServiceUtils::partPrefix(spaceId);
            std::unique_ptr<nebula::kvstore::KVIterator> itPart;
            if (kv->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &itPart) != suc) {
                return;
            }
            auto spaceProp = MetaServiceUtils::parseSpace(itSpace->val());
            auto partNum = spaceProp.get_partition_num();
            while (itPart->valid()) {
                auto formatSizeV1 = sizeof(int64_t) * partNum;
                if (itSpace->val().size() != formatSizeV1) {
                    continue;   // skip data v2
                }
                auto hosts = MetaServiceUtils::parsePartValV1(itPart->val());
                removeData.emplace_back(itPart->key());
                data.emplace_back(itPart->key(), MetaServiceUtils::partValV2(hosts));
            }
        }
    }

    {
        // 2. kHostsTable
        const auto& hostPrefix = nebula::meta::MetaServiceUtils::hostPrefix();
        std::unique_ptr<nebula::kvstore::KVIterator> iter;
        if (kv->prefix(kDefaultSpaceId, kDefaultPartId, hostPrefix, &iter) != suc) {
            return;
        }
        while (iter->valid()) {
            auto szKeyV1 = hostPrefix.size() + sizeof(int64_t);
            if (iter->key().size() != szKeyV1) {
                continue;
            }
            auto host = nebula::meta::MetaServiceUtils::parseHostKey(iter->key());
            removeData.emplace_back(iter->key());
            data.emplace_back(MetaServiceUtils::hostKey(host.host, host.port), iter->val());
        }
    }

    {
        // 3. kLeadersTable
        const auto& leaderPrefix = nebula::meta::MetaServiceUtils::leaderPrefix();
        std::unique_ptr<nebula::kvstore::KVIterator> iter;
        auto ret = kv->prefix(kDefaultSpaceId, kDefaultPartId, leaderPrefix, &iter);
        if (ret != nebula::kvstore::ResultCode::SUCCEEDED) {
            return;
        }
        while (iter->valid()) {
            auto szKeyV1 = leaderPrefix.size() + sizeof(int64_t);
            if (iter->key().size() != szKeyV1) {
                continue;
            }
            auto host = nebula::meta::MetaServiceUtils::parseLeaderKey(iter->key());
            removeData.emplace_back(iter->key());
            data.emplace_back(MetaServiceUtils::leaderKey(host.host, host.port), iter->val());
        }
    }

    folly::Baton<true, std::atomic> baton;
    kv->asyncMultiRemove(
        kDefaultSpaceId, kDefaultPartId, removeData, [&baton](nebula::kvstore::ResultCode code) {
            UNUSED(code);
            baton.post();
        });
    baton.wait();

    if (!data.empty()) {
        baton.reset();
        kv->asyncMultiPut(
            kDefaultSpaceId, kDefaultPartId, std::move(data),
            [&](nebula::kvstore::ResultCode code) {
                UNUSED(code);
                baton.post();
            });
        baton.wait();
    }
}

std::string MetaServiceUtils::serializeHostAddr(const HostAddr& host) {
    std::string ret;
    ret.reserve(sizeof(size_t) + 15 + sizeof(Port));    // 255.255.255.255
    size_t len = host.host.size();
    ret.append(reinterpret_cast<char*>(&len), sizeof(size_t))
       .append(host.host.data(), len)
       .append(reinterpret_cast<const char*>(&host.port), sizeof(Port));
    return ret;
}

HostAddr MetaServiceUtils::deserializeHostAddr(folly::StringPiece raw) {
    HostAddr addr;
    CHECK_GE(raw.size(), sizeof(size_t) + sizeof(Port));  // host may be ""
    size_t offset = 0;
    size_t len = *reinterpret_cast<const size_t*>(raw.begin() + offset);
    offset += sizeof(size_t);
    addr.host = std::string(raw.begin() + offset, len);
    offset += len;
    addr.port = *reinterpret_cast<const Port*>(raw.begin() + offset);
    return addr;
}

std::string MetaServiceUtils::groupKey(const std::string& group) {
    std::string key;
    key.reserve(kGroupsTable.size() + group.size());
    key.append(kGroupsTable.data(), kGroupsTable.size())
       .append(group);
    return key;
}

std::string MetaServiceUtils::groupVal(const std::vector<std::string>& zones) {
    return folly::join(",", zones);
}

const std::string& MetaServiceUtils::groupPrefix() {
    return kGroupsTable;
}

std::string MetaServiceUtils::parseGroupName(folly::StringPiece rawData) {
    return rawData.subpiece(kGroupsTable.size(), rawData.size()).toString();
}

std::vector<std::string> MetaServiceUtils::parseZoneNames(folly::StringPiece rawData) {
    std::vector<std::string> zones;
    folly::split(',', rawData.str(), zones);
    return zones;
}

std::string MetaServiceUtils::zoneKey(const std::string& zone) {
    std::string key;
    key.reserve(kZonesTable.size() + zone.size());
    key.append(kZonesTable.data(), kZonesTable.size())
       .append(zone);
    return key;
}

std::string MetaServiceUtils::zoneVal(const std::vector<HostAddr>& hosts) {
    std::string value;
    value.append(network::NetworkUtils::toHostsStr(hosts));
    return value;
}

const std::string& MetaServiceUtils::zonePrefix() {
    return kZonesTable;
}

std::string MetaServiceUtils::parseZoneName(folly::StringPiece rawData) {
    return rawData.subpiece(kZonesTable.size(), rawData.size()).toString();
}

std::vector<HostAddr> MetaServiceUtils::parseZoneHosts(folly::StringPiece rawData) {
    std::vector<HostAddr> addresses;
    auto hostsOrErr = network::NetworkUtils::toHosts(rawData.str());
    if (hostsOrErr.ok()) {
        addresses = std::move(hostsOrErr.value());
    } else {
        LOG(ERROR) << "invalid input for parseZoneHosts()";
    }
    return addresses;
}

bool MetaServiceUtils::zoneDefined() {
    return false;
}

std::string MetaServiceUtils::listenerKey(GraphSpaceID spaceId,
                                          PartitionID partId,
                                          cpp2::ListenerType type) {
    std::string key;
    key.reserve(kListenerTable.size() + sizeof(GraphSpaceID) +
                sizeof(cpp2::ListenerType) + sizeof(PartitionID));
    key.append(kListenerTable.data(), kListenerTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(reinterpret_cast<const char*>(&type), sizeof(cpp2::ListenerType))
       .append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
    return key;
}

std::string MetaServiceUtils::listenerPrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kListenerTable.size() + sizeof(GraphSpaceID));
    key.append(kListenerTable.data(), kListenerTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return key;
}

std::string MetaServiceUtils::listenerPrefix(GraphSpaceID spaceId,
                                             cpp2::ListenerType type) {
    std::string key;
    key.reserve(kListenerTable.size() + sizeof(GraphSpaceID) + sizeof(cpp2::ListenerType));
    key.append(kListenerTable.data(), kListenerTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(reinterpret_cast<const char*>(&type), sizeof(cpp2::ListenerType));
    return key;
}

cpp2::ListenerType MetaServiceUtils::parseListenerType(folly::StringPiece rawData) {
    auto offset = kListenerTable.size() + sizeof(GraphSpaceID);
    return *reinterpret_cast<const cpp2::ListenerType*>(rawData.data() + offset);
}

GraphSpaceID MetaServiceUtils::parseListenerSpace(folly::StringPiece rawData) {
    auto offset = kListenerTable.size();
    return *reinterpret_cast<const GraphSpaceID*>(rawData.data() + offset);
}

PartitionID MetaServiceUtils::parseListenerPart(folly::StringPiece rawData) {
    auto offset = kListenerTable.size() + sizeof(cpp2::ListenerType) + sizeof(GraphSpaceID);
    return *reinterpret_cast<const PartitionID*>(rawData.data() + offset);
}

}  // namespace meta
}  // namespace nebula
