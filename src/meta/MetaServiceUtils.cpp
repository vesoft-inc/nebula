/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include <thrift/lib/cpp/util/EnumUtils.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>
#include <boost/stacktrace.hpp>
#include "common/network/NetworkUtils.h"
#include "processors/Common.h"

namespace nebula {
namespace meta {

// Systemtable means that it does not contain any space information(id).false means that the backup
// should be skipped.
static const std::unordered_map<std::string, std::pair<std::string, bool>> systemTableMaps = {
    {"users", {"__users__", true}},
    {"hosts", {"__hosts__", false}},
    {"snapshots", {"__snapshots__", false}},
    {"configs", {"__configs__", true}},
    {"groups", {"__groups__", true}},
    {"zones", {"__zones__", true}},
    {"ft_service", {"__ft_service__", false}},
    {"sessions", {"__sessions__", true}},
    {"id", {"__id__", true}}
    };

// name => {prefix, parseSpaceid}, nullptr means that the backup should be skipped.
static const std::unordered_map<
    std::string,
    std::pair<std::string, std::function<decltype(MetaServiceUtils::spaceId)>>>
    tableMaps = {
        {"spaces", {"__spaces__", MetaServiceUtils::spaceId}},
        {"parts", {"__parts__", MetaServiceUtils::parsePartKeySpaceId}},
        {"tags", {"__tags__", MetaServiceUtils::parseTagsKeySpaceID}},
        {"edges", {"__edges__", MetaServiceUtils::parseEdgesKeySpaceID}},
        {"indexes", {"__indexes__", MetaServiceUtils::parseIndexesKeySpaceID}},
        // Index tables are handled separately.
        {"index", {"__index__", nullptr}},
        {"index_status", {"__index_status__", MetaServiceUtils::parseIndexStatusKeySpaceID}},
        {"roles", {"__roles__", MetaServiceUtils::parseRoleSpace}},
        {"last_update_time", {"__last_update_time__", nullptr}},
        {"leaders", {"__leaders__", nullptr}},
        {"leader_terms", {"__leader_terms__", nullptr}},
        {"listener", {"__listener__", nullptr}},
        {"statis", {"__statis__", MetaServiceUtils::parseStatisSpace}},
        {"balance_task", {"__balance_task__", nullptr}},
        {"balance_plan", {"__balance_plan__", nullptr}},
        {"ft_index", {"__ft_index__", nullptr}}};

static const std::string kSpacesTable         = tableMaps.at("spaces").first;         // NOLINT
static const std::string kPartsTable          = tableMaps.at("parts").first;          // NOLINT
static const std::string kHostsTable          = systemTableMaps.at("hosts").first;          // NOLINT
static const std::string kTagsTable           = tableMaps.at("tags").first;           // NOLINT
static const std::string kEdgesTable          = tableMaps.at("edges").first;          // NOLINT
static const std::string kIndexesTable        = tableMaps.at("indexes").first;        // NOLINT
static const std::string kIndexTable          = tableMaps.at("index").first;          // NOLINT
static const std::string kIndexStatusTable    = tableMaps.at("index_status").first;   // NOLINT
static const std::string kUsersTable          = systemTableMaps.at("users").first;          // NOLINT
static const std::string kRolesTable          = tableMaps.at("roles").first;          // NOLINT
static const std::string kConfigsTable        = systemTableMaps.at("configs").first;        // NOLINT
static const std::string kSnapshotsTable      = systemTableMaps.at("snapshots").first;      // NOLINT
static const std::string kLastUpdateTimeTable = tableMaps.at("last_update_time").first; // NOLINT
static const std::string kLeadersTable        = tableMaps.at("leaders").first;          // NOLINT
static const std::string kLeaderTermsTable    = tableMaps.at("leader_terms").first;     // NOLINT
static const std::string kGroupsTable         = systemTableMaps.at("groups").first;           // NOLINT
static const std::string kZonesTable          = systemTableMaps.at("zones").first;            // NOLINT
static const std::string kListenerTable       = tableMaps.at("listener").first;         // NOLINT

// Used to record the number of vertices and edges in the space
// The number of vertices of each tag in the space
// The number of edges of each edgetype in the space
static const std::string kStatisTable         = tableMaps.at("statis").first;           // NOLINT
static const std::string kBalanceTaskTable    = tableMaps.at("balance_task").first;     // NOLINT
static const std::string kBalancePlanTable    = tableMaps.at("balance_plan").first;     // NOLINT

const std::string kFTIndexTable        = tableMaps.at("ft_index").first;         // NOLINT
const std::string kFTServiceTable = systemTableMaps.at("ft_service").first;      // NOLINT
const std::string kSessionsTable = systemTableMaps.at("sessions").first;         // NOLINT

const std::string kIdKey = systemTableMaps.at("id").first;                       // NOLINT

const int kMaxIpAddrLen = 15;   // '255.255.255.255'

namespace {
nebula::cpp2::ErrorCode backupTable(kvstore::KVStore* kvstore,
                                const std::string& backupName,
                                const std::string& tableName,
                                std::vector<std::string>& files,
                                std::function<bool(const folly::StringPiece& key)> filter) {
    auto backupFilePath = kvstore->backupTable(kDefaultSpaceId, backupName, tableName, filter);
    if (!ok(backupFilePath)) {
        auto result = error(backupFilePath);
        if (result == nebula::cpp2::ErrorCode::E_BACKUP_EMPTY_TABLE) {
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        return result;
    }

    files.insert(files.end(),
                 std::make_move_iterator(value(backupFilePath).begin()),
                 std::make_move_iterator(value(backupFilePath).end()));
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}
}   // namespace

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

std::string MetaServiceUtils::spaceVal(const cpp2::SpaceDesc& spaceDesc) {
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
    return *reinterpret_cast<const PartitionID*>(key.data() + kPartsTable.size() +
                                                 sizeof(GraphSpaceID));
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
    UNUSED(dataVer);   // currently if not ver1, it must be v2
    val.advance(sizeof(int));
    return parsePartValV2(val);
}

// partion val is ip(int) + port(int)
std::vector<HostAddr> MetaServiceUtils::parsePartValV1(folly::StringPiece val) {
    std::vector<HostAddr> hosts;
    static const size_t unitSize = sizeof(int32_t) * 2;
    auto hostsNum = val.size() / unitSize;
    hosts.reserve(hostsNum);
    VLOG(3) << "Total size:" << val.size() << ", host size:" << unitSize
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
    LOG(ERROR) << "deprecated function\n" << boost::stacktrace::stacktrace();
    return leaderKeyV2(addr, port);
}

std::string MetaServiceUtils::leaderKeyV2(std::string addr, Port port) {
    LOG(ERROR) << "deprecated function\n" << boost::stacktrace::stacktrace();
    std::string key;
    HostAddr h(addr, port);

    key.reserve(kLeadersTable.size() + kMaxIpAddrLen + sizeof(Port));
    key.append(kLeadersTable.data(), kLeadersTable.size())
       .append(MetaServiceUtils::serializeHostAddr(h));
    return key;
}

std::string MetaServiceUtils::leaderKey(GraphSpaceID spaceId, PartitionID partId) {
    std::string key;
    key.reserve(kLeaderTermsTable.size() + sizeof(GraphSpaceID) + sizeof(PartitionID));
    key.append(kLeaderTermsTable.data(), kLeaderTermsTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
    return key;
}

std::string MetaServiceUtils::leaderVal(const LeaderParts& leaderParts) {
    LOG(ERROR) << "deprecated function\n" << boost::stacktrace::stacktrace();
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

// v3: dataVer(int) + lenOfHost(8) + HostAddr(varchar) + term(int64_t)
std::string MetaServiceUtils::leaderValV3(const HostAddr& h, int64_t term) {
    std::string leaderVal;
    leaderVal.reserve(256);

    int dataVersion = 3;

    auto sHost = serializeHostAddr(h);
    auto lenOfHost = sHost.size();

    leaderVal.append(reinterpret_cast<const char*>(&dataVersion), sizeof(dataVersion))
             .append(reinterpret_cast<const char*>(&lenOfHost), sizeof(lenOfHost))
             .append(sHost)
             .append(reinterpret_cast<const char*>(&term), sizeof(term));
    return leaderVal;
}

// v3: dataVer(int) + lenOfHost(8) + HostAddr(varchar) + term(int64_t)
std::tuple<HostAddr, TermID, nebula::cpp2::ErrorCode>
MetaServiceUtils::parseLeaderValV3(folly::StringPiece val) {
    std::tuple<HostAddr, TermID, nebula::cpp2::ErrorCode> ret;
    std::get<2>(ret) = nebula::cpp2::ErrorCode::SUCCEEDED;
    int dataVer = *reinterpret_cast<const int*>(val.data());
    if (dataVer != 3) {
        std::get<2>(ret) = nebula::cpp2::ErrorCode::E_INVALID_PARM;
        return ret;
    }

    CHECK_GE(val.size(), sizeof(int));
    val.advance(sizeof(int));
    auto lenOfHost = *reinterpret_cast<const size_t*>(val.data());

    val.advance(sizeof(size_t));
    CHECK_GE(val.size(), lenOfHost);
    std::get<0>(ret) = MetaServiceUtils::deserializeHostAddr(val.subpiece(0, lenOfHost));

    val.advance(lenOfHost);
    std::get<1>(ret) = *reinterpret_cast<const TermID*>(val.data());
    return ret;
}

const std::string& MetaServiceUtils::leaderPrefix() {
    return kLeaderTermsTable;
}

std::string MetaServiceUtils::leaderPrefix(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kLeaderTermsTable.size() + sizeof(GraphSpaceID));
    key.append(kLeaderTermsTable.data(), kLeaderTermsTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return key;
}

HostAddr MetaServiceUtils::parseLeaderKey(folly::StringPiece key) {
    LOG(ERROR) << "deprecated function\n" << boost::stacktrace::stacktrace();
    if (key.size() == kLeadersTable.size() + sizeof(int64_t)) {
        return parseLeaderKeyV1(key);
    }
    return parseLeaderKeyV2(key);
}

// input should be a pair of int32_t
HostAddr MetaServiceUtils::parseLeaderKeyV1(folly::StringPiece key) {
    LOG(ERROR) << "deprecated function\n" << boost::stacktrace::stacktrace();
    HostAddr host;
    CHECK_EQ(key.size(), kLeadersTable.size() + sizeof(int64_t));
    key.advance(kLeadersTable.size());
    auto ip = *reinterpret_cast<const uint32_t*>(key.begin());
    host.host = network::NetworkUtils::intToIPv4(ip);
    host.port = *reinterpret_cast<const uint32_t*>(key.begin() + sizeof(ip));
    return host;
}

HostAddr MetaServiceUtils::parseLeaderKeyV2(folly::StringPiece key) {
    LOG(ERROR) << "deprecated function\n" << boost::stacktrace::stacktrace();
    key.advance(kLeadersTable.size());
    return MetaServiceUtils::deserializeHostAddr(key);
}

std::pair<GraphSpaceID, PartitionID> MetaServiceUtils::parseLeaderKeyV3(folly::StringPiece key) {
    std::pair<GraphSpaceID, PartitionID> ret;
    ret.first = *reinterpret_cast<const GraphSpaceID*>(key.data() + kLeaderTermsTable.size());
    ret.second = *reinterpret_cast<const PartitionID*>(key.data() + kLeaderTermsTable.size() +
                                                                    sizeof(GraphSpaceID));
    return ret;
}

LeaderParts MetaServiceUtils::parseLeaderValV1(folly::StringPiece val) {
    LOG(ERROR) << "deprecated function\n" << boost::stacktrace::stacktrace();
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

std::string MetaServiceUtils::schemaVal(const std::string& name, const cpp2::Schema& schema) {
    auto len = name.size();
    std::string val, sval;
    apache::thrift::CompactSerializer::serialize(schema, &sval);
    val.reserve(sizeof(int32_t) + name.size() + sval.size());
    val.append(reinterpret_cast<const char*>(&len), sizeof(int32_t)).append(name).append(sval);
    return val;
}

EdgeType MetaServiceUtils::parseEdgeType(folly::StringPiece key) {
    return *reinterpret_cast<const EdgeType*>(key.data() + kEdgesTable.size() +
                                              sizeof(GraphSpaceID));
}

SchemaVer MetaServiceUtils::parseEdgeVersion(folly::StringPiece key) {
    auto offset = kEdgesTable.size() + sizeof(GraphSpaceID) + sizeof(EdgeType);
    return std::numeric_limits<SchemaVer>::max() -
           *reinterpret_cast<const SchemaVer*>(key.begin() + offset);
}

GraphSpaceID MetaServiceUtils::parseEdgesKeySpaceID(folly::StringPiece key) {
    return *reinterpret_cast<const GraphSpaceID*>(key.data() + kEdgesTable.size());
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

TagID MetaServiceUtils::parseTagId(folly::StringPiece key) {
    return *reinterpret_cast<const TagID*>(key.data() + kTagsTable.size() + sizeof(GraphSpaceID));
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

GraphSpaceID MetaServiceUtils::parseTagsKeySpaceID(folly::StringPiece key) {
    return *reinterpret_cast<const GraphSpaceID*>(key.data() + kTagsTable.size());
}

cpp2::Schema MetaServiceUtils::parseSchema(folly::StringPiece rawData) {
    cpp2::Schema schema;
    int32_t offset = sizeof(int32_t) + *reinterpret_cast<const int32_t*>(rawData.begin());
    auto schval = rawData.subpiece(offset, rawData.size() - offset);
    apache::thrift::CompactSerializer::deserialize(schval, schema);
    return schema;
}

std::string MetaServiceUtils::indexKey(GraphSpaceID spaceID, IndexID indexID) {
    std::string key;
    key.reserve(kIndexesTable.size() + sizeof(GraphSpaceID) + sizeof(IndexID));
    key.append(kIndexesTable.data(), kIndexesTable.size())
       .append(reinterpret_cast<const char*>(&spaceID), sizeof(GraphSpaceID))
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

GraphSpaceID MetaServiceUtils::parseIndexesKeySpaceID(folly::StringPiece key) {
    return *reinterpret_cast<const GraphSpaceID*>(key.data() + kIndexesTable.size());
}

IndexID MetaServiceUtils::parseIndexesKeyIndexID(folly::StringPiece key) {
    return *reinterpret_cast<const IndexID*>(key.data() + kIndexesTable.size() +
                                             sizeof(GraphSpaceID));
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
std::string MetaServiceUtils::rebuildIndexStatusPrefix(GraphSpaceID space, char type) {
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

GraphSpaceID MetaServiceUtils::parseIndexStatusKeySpaceID(folly::StringPiece key) {
    return *reinterpret_cast<const GraphSpaceID*>(key.data() + kIndexStatusTable.size());
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

std::string MetaServiceUtils::indexTagKey(GraphSpaceID spaceId, const std::string& name) {
    EntryType type = EntryType::TAG;
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size())
       .append(reinterpret_cast<const char*>(&type), sizeof(type))
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(name);
    return key;
}

std::string MetaServiceUtils::indexEdgeKey(GraphSpaceID spaceId, const std::string& name) {
    EntryType type = EntryType::EDGE;
    std::string key;
    key.reserve(128);
    key.append(kIndexTable.data(), kIndexTable.size())
       .append(reinterpret_cast<const char*>(&type), sizeof(type))
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(name);
    return key;
}

GraphSpaceID MetaServiceUtils::parseIndexKeySpaceID(folly::StringPiece key) {
    return *reinterpret_cast<const GraphSpaceID*>(key.data() + kIndexTable.size() +
                                                  sizeof(EntryType));
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
       .append(reinterpret_cast<const char*>(&type), sizeof(EntryType))
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
    segmentKey.append(segment).append(key.data(), key.size());
    return segmentKey;
}

nebula::cpp2::ErrorCode
MetaServiceUtils::alterColumnDefs(std::vector<cpp2::ColumnDef>& cols,
                                  cpp2::SchemaProp& prop,
                                  const cpp2::ColumnDef col,
                                  const cpp2::AlterSchemaOp op,
                                  bool isEdge) {
    switch (op) {
        case cpp2::AlterSchemaOp::ADD:
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                if (it->get_name() == col.get_name()) {
                    LOG(ERROR) << "Column existing: " << col.get_name();
                    return nebula::cpp2::ErrorCode::E_EXISTED;
                }
            }
            cols.emplace_back(std::move(col));
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        case cpp2::AlterSchemaOp::CHANGE:
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                auto colName = col.get_name();
                if (colName == it->get_name()) {
                    // If this col is ttl_col, change not allowed
                    if (prop.get_ttl_col() && (*prop.get_ttl_col() == colName)) {
                        LOG(ERROR) << "Column: " << colName << " as ttl_col, change not allowed";
                        return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
                    }
                    *it = col;
                    return nebula::cpp2::ErrorCode::SUCCEEDED;
                }
            }
            LOG(ERROR) << "Column not found: " << col.get_name();
            if (isEdge) {
                return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            }
            return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
        case cpp2::AlterSchemaOp::DROP:
            for (auto it = cols.begin(); it != cols.end(); ++it) {
                auto colName = col.get_name();
                if (colName == it->get_name()) {
                    if (prop.get_ttl_col() && (*prop.get_ttl_col() == colName)) {
                        prop.set_ttl_duration(0);
                        prop.set_ttl_col("");
                    }
                    cols.erase(it);
                    return nebula::cpp2::ErrorCode::SUCCEEDED;
                }
            }
            LOG(ERROR) << "Column not found: " << col.get_name();
            if (isEdge) {
                return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            }
            return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
        default:
            LOG(ERROR) << "Alter schema operator not supported";
            return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
    }
}

nebula::cpp2::ErrorCode
MetaServiceUtils::alterSchemaProp(std::vector<cpp2::ColumnDef>& cols,
                                  cpp2::SchemaProp& schemaProp,
                                  cpp2::SchemaProp alterSchemaProp,
                                  bool existIndex,
                                  bool isEdge) {
    if (existIndex && (alterSchemaProp.ttl_duration_ref().has_value() ||
                alterSchemaProp.ttl_col_ref().has_value())) {
        LOG(ERROR) << "Has index, can't change ttl";
        return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
    }
    if (alterSchemaProp.ttl_duration_ref().has_value()) {
        // Graph check  <=0 to = 0
        schemaProp.set_ttl_duration(*alterSchemaProp.ttl_duration_ref());
    }
    if (alterSchemaProp.ttl_col_ref().has_value()) {
        auto ttlCol = *alterSchemaProp.ttl_col_ref();
        // Disable ttl, ttl_col is empty, ttl_duration is 0
        if (ttlCol.empty()) {
            schemaProp.set_ttl_duration(0);
            schemaProp.set_ttl_col(ttlCol);
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }

        auto existed = false;
        for (auto& col : cols) {
            if (col.get_name() == ttlCol) {
                auto colType = col.get_type().get_type();
                // Only integer and timestamp columns can be used as ttl_col
                if (colType != cpp2::PropertyType::INT64 &&
                    colType != cpp2::PropertyType::TIMESTAMP) {
                    LOG(ERROR) << "TTL column type illegal";
                    return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
                }
                existed = true;
                schemaProp.set_ttl_col(ttlCol);
                break;
            }
        }

        if (!existed) {
            LOG(ERROR) << "TTL column not found: " << ttlCol;
            if (isEdge) {
                return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
            }
            return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
        }
    }

    // Disable implicit TTL mode
    if ((schemaProp.get_ttl_duration() && (*schemaProp.get_ttl_duration() != 0)) &&
        (!schemaProp.get_ttl_col() ||
         (schemaProp.get_ttl_col() && schemaProp.get_ttl_col()->empty()))) {
        LOG(WARNING) << "Implicit ttl_col not support";
        return nebula::cpp2::ErrorCode::E_UNSUPPORTED;
    }

    if (alterSchemaProp.comment_ref().has_value()) {
        schemaProp.set_comment(*alterSchemaProp.comment_ref());
    }

    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

std::string MetaServiceUtils::userPrefix() {
    return kUsersTable;
}

std::string MetaServiceUtils::userKey(const std::string& account) {
    std::string key;
    key.reserve(kUsersTable.size() + account.size());
    key.append(kUsersTable.data(), kUsersTable.size()).append(account);
    return key;
}

std::string MetaServiceUtils::userVal(const std::string& val) {
    std::string key;
    auto pwdLen = val.size();
    key.reserve(sizeof(int64_t) + pwdLen);
    key.append(reinterpret_cast<const char*>(&pwdLen), sizeof(size_t)).append(val);
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
        case cpp2::RoleType::GOD: {
            role = "GOD";
            break;
        }
        case cpp2::RoleType::ADMIN: {
            role = "ADMIN";
            break;
        }
        case cpp2::RoleType::DBA: {
            role = "DBA";
            break;
        }
        case cpp2::RoleType::USER: {
            role = "USER";
            break;
        }
        case cpp2::RoleType::GUEST: {
            role = "GUEST";
            break;
        }
    }
    return role;
}

std::string MetaServiceUtils::configKey(const cpp2::ConfigModule& module, const std::string& name) {
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

std::string MetaServiceUtils::configValue(const cpp2::ConfigMode& valueMode, const Value& value) {
    std::string val, cVal;
    apache::thrift::CompactSerializer::serialize(value, &cVal);
    val.reserve(sizeof(cpp2::ConfigMode) + cVal.size());
    val.append(reinterpret_cast<const char*>(&valueMode), sizeof(cpp2::ConfigMode)).append(cVal);
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
    key.append(kSnapshotsTable.data(), kSnapshotsTable.size()).append(name);
    return key;
}

std::string MetaServiceUtils::snapshotVal(const cpp2::SnapshotStatus& status,
                                          const std::string& hosts) {
    std::string val;
    val.reserve(sizeof(cpp2::SnapshotStatus) + sizeof(hosts));
    val.append(reinterpret_cast<const char*>(&status), sizeof(cpp2::SnapshotStatus)).append(hosts);
    return val;
}

cpp2::SnapshotStatus MetaServiceUtils::parseSnapshotStatus(folly::StringPiece rawData) {
    return *reinterpret_cast<const cpp2::SnapshotStatus*>(rawData.data());
}

std::string MetaServiceUtils::parseSnapshotHosts(folly::StringPiece rawData) {
    return rawData
        .subpiece(sizeof(cpp2::SnapshotStatus), rawData.size() - sizeof(cpp2::SnapshotStatus))
        .str();
}

std::string MetaServiceUtils::parseSnapshotName(folly::StringPiece rawData) {
    int32_t offset = kSnapshotsTable.size();
    return rawData.subpiece(offset, rawData.size() - offset).str();
}

const std::string& MetaServiceUtils::snapshotPrefix() {
    return kSnapshotsTable;
}

std::string MetaServiceUtils::serializeHostAddr(const HostAddr& host) {
    std::string ret;
    ret.reserve(sizeof(size_t) + 15 + sizeof(Port));   // 255.255.255.255
    size_t len = host.host.size();
    ret.append(reinterpret_cast<char*>(&len), sizeof(size_t))
       .append(host.host.data(), len)
       .append(reinterpret_cast<const char*>(&host.port), sizeof(Port));
    return ret;
}

HostAddr MetaServiceUtils::deserializeHostAddr(folly::StringPiece raw) {
    HostAddr addr;
    CHECK_GE(raw.size(), sizeof(size_t) + sizeof(Port));   // host may be ""
    size_t offset = 0;
    size_t len = *reinterpret_cast<const size_t*>(raw.begin() + offset);
    offset += sizeof(size_t);
    addr.host = std::string(raw.begin() + offset, len);
    offset += len;
    addr.port = *reinterpret_cast<const Port*>(raw.begin() + offset);
    return addr;
}

std::string MetaServiceUtils::genTimestampStr() {
    char ch[60];
    std::time_t t = std::time(nullptr);
    std::strftime(ch, sizeof(ch), "%Y_%m_%d_%H_%M_%S", localtime(&t));
    return ch;
}

std::string MetaServiceUtils::idKey() {
    return kIdKey;
}

std::function<bool(const folly::StringPiece& key)>
MetaServiceUtils::spaceFilter(const std::unordered_set<GraphSpaceID>& spaces,
                              std::function<GraphSpaceID(folly::StringPiece rawKey)> parseSpace) {
    auto sf = [spaces, parseSpace](const folly::StringPiece& key) -> bool {
        if (spaces.empty()) {
            return false;
        }

        auto id = parseSpace(key);
        auto it = spaces.find(id);
        if (it == spaces.end()) {
            return true;
        }

        return false;
    };

    return sf;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> MetaServiceUtils::backupIndex(
    kvstore::KVStore* kvstore,
    const std::unordered_set<GraphSpaceID>& spaces,
    const std::string& backupName,
    const std::vector<std::string>* spaceName) {
    return kvstore->backupTable(
        kDefaultSpaceId,
        backupName,
        kIndexTable,
        [spaces, spaceName](const folly::StringPiece& key) -> bool {
            if (spaces.empty()) {
                return false;
            }

            auto type = *reinterpret_cast<const EntryType*>(key.data() + kIndexTable.size());
            if (type == EntryType::SPACE) {
                if (spaceName == nullptr) {
                    return false;
                }
                auto sn = key.subpiece(kIndexTable.size() + sizeof(EntryType),
                                       key.size() - kIndexTable.size() - sizeof(EntryType))
                              .str();
                LOG(INFO) << "sn was " << sn;
                auto it = std::find_if(spaceName->cbegin(), spaceName->cend(), [&sn](auto& name) {
                    return sn == name;
                });

                if (it == spaceName->cend()) {
                    return true;
                }
                return false;
            }

            auto id = parseIndexKeySpaceID(key);
            auto it = spaces.find(id);
            if (it == spaces.end()) {
                return true;
            }

            return false;
        });
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<std::string>> MetaServiceUtils::backupSpaces(
    kvstore::KVStore* kvstore,
    const std::unordered_set<GraphSpaceID>& spaces,
    const std::string& backupName,
    const std::vector<std::string>* spaceNames) {
    std::vector<std::string> files;
    files.reserve(tableMaps.size());

    for (const auto& table : tableMaps) {
        if (table.second.second == nullptr) {
            LOG(INFO) << table.first << " table skipped";
            continue;
        }
        auto result = backupTable(kvstore,
                                  backupName,
                                  table.second.first,
                                  files,
                                  spaceFilter(spaces, table.second.second));
        if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
            return result;
        }
        LOG(INFO) << table.first << " table backup successed";
    }

    if (spaceNames == nullptr) {
        for (const auto& table : systemTableMaps) {
            if (!table.second.second) {
                LOG(INFO) << table.first << " table skipped";
                continue;
            }
            auto result = backupTable(kvstore, backupName, table.second.first, files, nullptr);
            if (result != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return result;
            }
            LOG(INFO) << table.first << " table backup successed";
        }
    }

    // The mapping of space name and space id needs to be handled separately.
    auto ret = backupIndex(kvstore, spaces, backupName, spaceNames);
    if (!ok(ret)) {
        auto result = error(ret);
        if (result == nebula::cpp2::ErrorCode::E_BACKUP_EMPTY_TABLE) {
            return files;
        }
        return result;
    }

    files.insert(files.end(),
                 std::make_move_iterator(value(ret).begin()),
                 std::make_move_iterator(value(ret).end()));

    return files;
}

std::string MetaServiceUtils::balanceTaskKey(BalanceID balanceId,
                                             GraphSpaceID spaceId,
                                             PartitionID partId,
                                             HostAddr src,
                                             HostAddr dst) {
    std::string str;
    str.reserve(64);
    str.append(reinterpret_cast<const char*>(kBalanceTaskTable.data()), kBalanceTaskTable.size())
       .append(reinterpret_cast<const char*>(&balanceId), sizeof(BalanceID))
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
       .append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
       .append(serializeHostAddr(src))
       .append(serializeHostAddr(dst));
    return str;
}

std::string MetaServiceUtils::balanceTaskVal(BalanceTaskStatus status,
                                             BalanceTaskResult retult,
                                             int64_t startTime,
                                             int64_t endTime) {
    std::string val;
    val.reserve(32);
    val.append(reinterpret_cast<const char*>(&status), sizeof(BalanceTaskStatus))
       .append(reinterpret_cast<const char*>(&retult), sizeof(BalanceTaskResult))
       .append(reinterpret_cast<const char*>(&startTime), sizeof(int64_t))
       .append(reinterpret_cast<const char*>(&endTime), sizeof(int64_t));
    return val;
}

std::string MetaServiceUtils::balanceTaskPrefix(BalanceID balanceId) {
    std::string prefix;
    prefix.reserve(32);
    prefix.append(reinterpret_cast<const char*>(kBalanceTaskTable.data()),
                  kBalanceTaskTable.size())
          .append(reinterpret_cast<const char*>(&balanceId), sizeof(BalanceID));
    return prefix;
}

std::string MetaServiceUtils::balancePlanKey(BalanceID id) {
    CHECK_GE(id, 0);
    // make the balance id is stored in decend order
    auto encode = folly::Endian::big(std::numeric_limits<BalanceID>::max() - id);
    std::string key;
    key.reserve(sizeof(BalanceID) + kBalancePlanTable.size());
    key.append(reinterpret_cast<const char*>(kBalancePlanTable.data()), kBalancePlanTable.size())
       .append(reinterpret_cast<const char*>(&encode), sizeof(BalanceID));
    return key;
}

std::string MetaServiceUtils::balancePlanVal(BalanceStatus status) {
    std::string val;
    val.reserve(sizeof(BalanceStatus));
    val.append(reinterpret_cast<const char*>(&status), sizeof(BalanceStatus));
    return val;
}

std::string MetaServiceUtils::balancePlanPrefix() {
    return kBalancePlanTable;
}

std::tuple<BalanceID, GraphSpaceID, PartitionID, HostAddr, HostAddr>
MetaServiceUtils::parseBalanceTaskKey(const folly::StringPiece& rawKey) {
    uint32_t offset = kBalanceTaskTable.size();
    auto balanceId = *reinterpret_cast<const BalanceID*>(rawKey.begin() + offset);
    offset += sizeof(balanceId);
    auto spaceId = *reinterpret_cast<const GraphSpaceID*>(rawKey.begin() + offset);
    offset += sizeof(GraphSpaceID);
    auto partId = *reinterpret_cast<const PartitionID*>(rawKey.begin() + offset);
    offset += sizeof(PartitionID);
    auto src = MetaServiceUtils::deserializeHostAddr({rawKey, offset});
    offset += src.host.size() + sizeof(size_t) + sizeof(uint32_t);
    auto dst = MetaServiceUtils::deserializeHostAddr({rawKey, offset});
    return std::make_tuple(balanceId, spaceId, partId, src, dst);
}

std::tuple<BalanceTaskStatus, BalanceTaskResult, int64_t, int64_t>
MetaServiceUtils::parseBalanceTaskVal(const folly::StringPiece& rawVal) {
    int32_t offset = 0;
    auto status = *reinterpret_cast<const BalanceTaskStatus*>(rawVal.begin() + offset);
    offset += sizeof(BalanceTaskStatus);
    auto ret = *reinterpret_cast<const BalanceTaskResult*>(rawVal.begin() + offset);
    offset += sizeof(BalanceTaskResult);
    auto start = *reinterpret_cast<const int64_t*>(rawVal.begin() + offset);
    offset += sizeof(int64_t);
    auto end = *reinterpret_cast<const int64_t*>(rawVal.begin() + offset);
    return std::make_tuple(status, ret, start, end);
}

std::string MetaServiceUtils::groupKey(const std::string& group) {
    std::string key;
    key.reserve(kGroupsTable.size() + group.size());
    key.append(kGroupsTable.data(), kGroupsTable.size())
       .append(group);
    return key;
}

BalanceID MetaServiceUtils::parseBalanceID(const folly::StringPiece& rawKey) {
    auto decode = *reinterpret_cast<const BalanceID*>(rawKey.begin() + kBalancePlanTable.size());
    auto id = std::numeric_limits<BalanceID>::max() - folly::Endian::big(decode);
    CHECK_GE(id, 0);
    return id;
}

BalanceStatus MetaServiceUtils::parseBalanceStatus(const folly::StringPiece& rawVal) {
    return static_cast<BalanceStatus>(*rawVal.begin());
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

std::string MetaServiceUtils::statisKey(GraphSpaceID spaceId) {
    std::string key;
    key.reserve(kStatisTable.size() + sizeof(GraphSpaceID));
    key.append(kStatisTable.data(), kStatisTable.size())
       .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
    return key;
}

std::string MetaServiceUtils::statisVal(const cpp2::StatisItem& statisItem) {
    std::string val;
    apache::thrift::CompactSerializer::serialize(statisItem, &val);
    return val;
}

cpp2::StatisItem MetaServiceUtils::parseStatisVal(folly::StringPiece rawData) {
    cpp2::StatisItem statisItem;
    apache::thrift::CompactSerializer::deserialize(rawData, statisItem);
    return statisItem;
}

GraphSpaceID MetaServiceUtils::parseStatisSpace(folly::StringPiece rawData) {
    auto offset = kStatisTable.size();
    return *reinterpret_cast<const GraphSpaceID*>(rawData.data() + offset);
}

const std::string& MetaServiceUtils::statisKeyPrefix() {
    return kStatisTable;
}

std::string MetaServiceUtils::fulltextServiceKey() {
    std::string key;
    key.reserve(kFTServiceTable.size());
    key.append(kFTServiceTable.data(), kFTServiceTable.size());
    return key;
}

std::string MetaServiceUtils::fulltextServiceVal(cpp2::FTServiceType type,
                                                 const std::vector<cpp2::FTClient>& clients) {
    std::string val, cval;
    apache::thrift::CompactSerializer::serialize(clients, &cval);
    val.reserve(sizeof(cpp2::FTServiceType) + cval.size());
    val.append(reinterpret_cast<const char*>(&type), sizeof(cpp2::FTServiceType))
       .append(cval);
    return val;
}

std::vector<cpp2::FTClient> MetaServiceUtils::parseFTClients(folly::StringPiece rawData) {
    std::vector<cpp2::FTClient> clients;
    int32_t offset = sizeof(cpp2::FTServiceType);
    auto clientsRaw = rawData.subpiece(offset, rawData.size() - offset);
    apache::thrift::CompactSerializer::deserialize(clientsRaw, clients);
    return clients;
}

const std::string& MetaServiceUtils::sessionPrefix() {
    return kSessionsTable;
}

std::string MetaServiceUtils::sessionKey(SessionID sessionId) {
    std::string key;
    key.reserve(kSessionsTable.size() + sizeof(sessionId));
    key.append(kSessionsTable.data(), kSessionsTable.size())
       .append(reinterpret_cast<const char*>(&sessionId), sizeof(SessionID));
    return key;
}

std::string MetaServiceUtils::sessionVal(const meta::cpp2::Session &session) {
    std::string val;
    apache::thrift::CompactSerializer::serialize(session, &val);
    return val;
}

SessionID MetaServiceUtils::getSessionId(const folly::StringPiece &key) {
    return *reinterpret_cast<const SessionID*>(key.data() + kSessionsTable.size());
}

meta::cpp2::Session MetaServiceUtils::parseSessionVal(const folly::StringPiece &val) {
    meta::cpp2::Session session;
    apache::thrift::CompactSerializer::deserialize(val, session);
    return session;
}

std::string MetaServiceUtils::fulltextIndexKey(const std::string& indexName) {
    std::string key;
    key.reserve(kFTIndexTable.size() + indexName.size());
    key.append(kFTIndexTable.data(), kFTIndexTable.size())
       .append(indexName);
    return key;
}

std::string MetaServiceUtils::fulltextIndexVal(const cpp2::FTIndex& index) {
    std::string val;
    apache::thrift::CompactSerializer::serialize(index, &val);
    return val;
}

std::string MetaServiceUtils::parsefulltextIndexName(folly::StringPiece key) {
    return key.subpiece(kFTIndexTable.size(), key.size()).toString();
}

cpp2::FTIndex MetaServiceUtils::parsefulltextIndex(folly::StringPiece val) {
   cpp2::FTIndex ftIndex;
   apache::thrift::CompactSerializer::deserialize(val, ftIndex);
   return ftIndex;
}

std::string MetaServiceUtils::fulltextIndexPrefix() {
    return kFTIndexTable;
}

}  // namespace meta
}  // namespace nebula
