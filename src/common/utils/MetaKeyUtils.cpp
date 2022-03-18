/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/utils/MetaKeyUtils.h"

#include <thrift/lib/cpp/util/EnumUtils.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

#include <boost/stacktrace.hpp>

#include "common/network/NetworkUtils.h"

namespace nebula {

// Systemtable means that it does not contain any space information(id).false
// means that the backup should be skipped.
static const std::unordered_map<std::string, std::pair<std::string, bool>> systemTableMaps = {
    {"users", {"__users__", true}},
    {"hosts", {"__hosts__", false}},
    {"versions", {"__versions__", false}},
    {"machines", {"__machines__", true}},
    {"host_dirs", {"__host_dirs__", false}},
    {"snapshots", {"__snapshots__", false}},
    {"configs", {"__configs__", true}},
    {"groups", {"__groups__", true}},
    {"zones", {"__zones__", true}},
    {"services", {"__services__", false}},
    {"sessions", {"__sessions__", true}}};

// SystemInfo will always be backed up
static const std::unordered_map<std::string, std::pair<std::string, bool>> systemInfoMaps{
    {"autoIncrementId", {"__id__", true}}, {"lastUpdateTime", {"__last_update_time__", true}}};

// name => {prefix, parseSpaceid}, nullptr means that the backup should be skipped.
static const std::unordered_map<
    std::string,
    std::pair<std::string, std::function<decltype(MetaKeyUtils::spaceId)>>>
    tableMaps = {{"spaces", {"__spaces__", MetaKeyUtils::spaceId}},
                 {"parts", {"__parts__", MetaKeyUtils::parsePartKeySpaceId}},
                 {"tags", {"__tags__", MetaKeyUtils::parseTagsKeySpaceID}},
                 {"edges", {"__edges__", MetaKeyUtils::parseEdgesKeySpaceID}},
                 {"indexes", {"__indexes__", MetaKeyUtils::parseIndexesKeySpaceID}},
                 // Index tables are handled separately.
                 {"index", {"__index__", nullptr}},
                 {"index_status", {"__index_status__", MetaKeyUtils::parseIndexStatusKeySpaceID}},
                 {"roles", {"__roles__", MetaKeyUtils::parseRoleSpace}},
                 {"leaders", {"__leaders__", nullptr}},
                 {"leader_terms", {"__leader_terms__", nullptr}},
                 {"listener", {"__listener__", nullptr}},
                 {"stats", {"__stats__", MetaKeyUtils::parseStatsSpace}},
                 {"balance_task", {"__balance_task__", nullptr}},
                 {"balance_plan", {"__balance_plan__", nullptr}},
                 {"ft_index", {"__ft_index__", nullptr}},
                 {"local_id", {"__local_id__", MetaKeyUtils::parseLocalIdSpace}},
                 {"disk_parts", {"__disk_parts__", MetaKeyUtils::parseDiskPartsSpace}}};

// clang-format off
static const std::string kSpacesTable         = tableMaps.at("spaces").first;         // NOLINT
static const std::string kPartsTable          = tableMaps.at("parts").first;          // NOLINT
static const std::string kVersionsTable       = systemTableMaps.at("versions").first; // NOLINT
static const std::string kHostsTable          = systemTableMaps.at("hosts").first;    // NOLINT
static const std::string kMachinesTable       = systemTableMaps.at("machines").first; // NOLINT
static const std::string kHostDirsTable       = systemTableMaps.at("host_dirs").first;// NOLINT
static const std::string kTagsTable           = tableMaps.at("tags").first;           // NOLINT
static const std::string kEdgesTable          = tableMaps.at("edges").first;          // NOLINT
static const std::string kIndexesTable        = tableMaps.at("indexes").first;        // NOLINT
static const std::string kIndexTable          = tableMaps.at("index").first;          // NOLINT
static const std::string kIndexStatusTable    = tableMaps.at("index_status").first;   // NOLINT
static const std::string kUsersTable          = systemTableMaps.at("users").first;    // NOLINT
static const std::string kRolesTable          = tableMaps.at("roles").first;          // NOLINT
static const std::string kConfigsTable        = systemTableMaps.at("configs").first;  // NOLINT
static const std::string kSnapshotsTable      = systemTableMaps.at("snapshots").first;// NOLINT
static const std::string kLeadersTable        = tableMaps.at("leaders").first;        // NOLINT
static const std::string kLeaderTermsTable    = tableMaps.at("leader_terms").first;   // NOLINT
static const std::string kGroupsTable         = systemTableMaps.at("groups").first;   // NOLINT
static const std::string kZonesTable          = systemTableMaps.at("zones").first;    // NOLINT
static const std::string kListenerTable       = tableMaps.at("listener").first;       // NOLINT
static const std::string kDiskPartsTable      = tableMaps.at("disk_parts").first;     // NOLINT

// Used to record the number of vertices and edges in the space
// The number of vertices of each tag in the space
// The number of edges of each edgetype in the space
static const std::string kStatsTable          = tableMaps.at("stats").first;            // NOLINT
static const std::string kBalanceTaskTable    = tableMaps.at("balance_task").first;     // NOLINT
static const std::string kBalancePlanTable    = tableMaps.at("balance_plan").first;     // NOLINT
static const std::string kLocalIdTable        = tableMaps.at("local_id").first;         // NOLINT

const std::string kFTIndexTable        = tableMaps.at("ft_index").first;         // NOLINT
const std::string kServicesTable  = systemTableMaps.at("services").first;        // NOLINT
const std::string kSessionsTable = systemTableMaps.at("sessions").first;         // NOLINT

const std::string kIdKey = systemInfoMaps.at("autoIncrementId").first;                // NOLINT
const std::string kLastUpdateTimeTable = systemInfoMaps.at("lastUpdateTime").first;   // NOLINT

// clang-format on

const int kMaxIpAddrLen = 15;  // '255.255.255.255'

std::string MetaKeyUtils::getIndexTable() {
  return kIndexTable;
}

std::unordered_map<std::string,
                   std::pair<std::string, std::function<decltype(MetaKeyUtils::spaceId)>>>
MetaKeyUtils::getTableMaps() {
  return tableMaps;
}

std::unordered_map<std::string, std::pair<std::string, bool>> MetaKeyUtils::getSystemInfoMaps() {
  return systemInfoMaps;
}

std::unordered_map<std::string, std::pair<std::string, bool>> MetaKeyUtils::getSystemTableMaps() {
  return systemTableMaps;
}

std::string MetaKeyUtils::lastUpdateTimeKey() {
  std::string key;
  key.reserve(kLastUpdateTimeTable.size());
  key.append(kLastUpdateTimeTable.data(), kLastUpdateTimeTable.size());
  return key;
}

std::string MetaKeyUtils::lastUpdateTimeVal(const int64_t timeInMilliSec) {
  std::string val;
  val.reserve(sizeof(int64_t));
  val.append(reinterpret_cast<const char*>(&timeInMilliSec), sizeof(int64_t));
  return val;
}

std::string MetaKeyUtils::spaceKey(GraphSpaceID spaceId) {
  std::string key;
  key.reserve(kSpacesTable.size() + sizeof(GraphSpaceID));
  key.append(kSpacesTable.data(), kSpacesTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
  return key;
}

std::string MetaKeyUtils::spaceVal(const meta::cpp2::SpaceDesc& spaceDesc) {
  std::string val;
  apache::thrift::CompactSerializer::serialize(spaceDesc, &val);
  return val;
}

meta::cpp2::SpaceDesc MetaKeyUtils::parseSpace(folly::StringPiece rawData) {
  meta::cpp2::SpaceDesc spaceDesc;
  apache::thrift::CompactSerializer::deserialize(rawData, spaceDesc);
  return spaceDesc;
}

const std::string& MetaKeyUtils::spacePrefix() {
  return kSpacesTable;
}

GraphSpaceID MetaKeyUtils::spaceId(folly::StringPiece rawKey) {
  return *reinterpret_cast<const GraphSpaceID*>(rawKey.data() + kSpacesTable.size());
}

std::string MetaKeyUtils::spaceName(folly::StringPiece rawVal) {
  return parseSpace(rawVal).get_space_name();
}

std::string MetaKeyUtils::partKey(GraphSpaceID spaceId, PartitionID partId) {
  std::string key;
  key.reserve(kPartsTable.size() + sizeof(GraphSpaceID) + sizeof(PartitionID));
  key.append(kPartsTable.data(), kPartsTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
      .append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
  return key;
}

GraphSpaceID MetaKeyUtils::parsePartKeySpaceId(folly::StringPiece key) {
  return *reinterpret_cast<const GraphSpaceID*>(key.data() + kPartsTable.size());
}

PartitionID MetaKeyUtils::parsePartKeyPartId(folly::StringPiece key) {
  return *reinterpret_cast<const PartitionID*>(key.data() + kPartsTable.size() +
                                               sizeof(GraphSpaceID));
}

std::string MetaKeyUtils::partVal(const std::vector<HostAddr>& hosts) {
  return partValV2(hosts);
}

// dataVer(int) + vectorSize(size_t) + vector of (strIp(string) + port(int))
std::string MetaKeyUtils::partValV2(const std::vector<HostAddr>& hosts) {
  std::string encodedVal;
  int dataVersion = 2;
  encodedVal.append(reinterpret_cast<const char*>(&dataVersion), sizeof(int))
      .append(network::NetworkUtils::toHostsStr(hosts));
  return encodedVal;
}

std::string MetaKeyUtils::partPrefix() {
  std::string prefix;
  prefix.reserve(kPartsTable.size() + sizeof(GraphSpaceID));
  prefix.append(kPartsTable.data(), kPartsTable.size());
  return prefix;
}

std::string MetaKeyUtils::partPrefix(GraphSpaceID spaceId) {
  std::string prefix;
  prefix.reserve(kPartsTable.size() + sizeof(GraphSpaceID));
  prefix.append(kPartsTable.data(), kPartsTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
  return prefix;
}

std::vector<HostAddr> MetaKeyUtils::parsePartVal(folly::StringPiece val, int partNum) {
  static const size_t unitSizeV1 = sizeof(int64_t);
  if (unitSizeV1 * partNum == val.size()) {
    return parsePartValV1(val);
  }
  int dataVer = *reinterpret_cast<const int32_t*>(val.data());
  UNUSED(dataVer);  // currently if not ver1, it must be v2
  val.advance(sizeof(int));
  return parsePartValV2(val);
}

// partition val is ip(int) + port(int)
std::vector<HostAddr> MetaKeyUtils::parsePartValV1(folly::StringPiece val) {
  std::vector<HostAddr> hosts;
  static const size_t unitSize = sizeof(int32_t) * 2;
  auto hostsNum = val.size() / unitSize;
  hosts.reserve(hostsNum);
  VLOG(3) << "Total size:" << val.size() << ", host size:" << unitSize << ", host num:" << hostsNum;
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
std::vector<HostAddr> MetaKeyUtils::parsePartValV2(folly::StringPiece val) {
  std::vector<HostAddr> ret;
  auto hostsOrErr = network::NetworkUtils::toHosts(val.str());
  if (hostsOrErr.ok()) {
    ret = std::move(hostsOrErr.value());
  } else {
    LOG(ERROR) << "invalid input for parsePartValV2()";
  }
  return ret;
}

std::string MetaKeyUtils::machineKey(std::string addr, Port port) {
  std::string key;
  HostAddr h(addr, port);
  key.append(kMachinesTable.data(), kMachinesTable.size())
      .append(MetaKeyUtils::serializeHostAddr(h));
  return key;
}

const std::string& MetaKeyUtils::machinePrefix() {
  return kMachinesTable;
}

HostAddr MetaKeyUtils::parseMachineKey(folly::StringPiece key) {
  key.advance(kMachinesTable.size());
  return MetaKeyUtils::deserializeHostAddr(key);
}

const std::string& MetaKeyUtils::hostDirPrefix() {
  return kHostDirsTable;
}

const std::string MetaKeyUtils::hostDirHostPrefix(std::string host) {
  return kHostDirsTable + host;
}

std::string MetaKeyUtils::hostDirKey(std::string host, Port port) {
  std::string key;
  key.reserve(kHostDirsTable.size() + host.size() + sizeof(port));
  key.append(kHostDirsTable.data(), kHostDirsTable.size()).append(host);
  key.append(reinterpret_cast<const char*>(&port), sizeof(Port));
  return key;
}

HostAddr MetaKeyUtils::parseHostDirKey(folly::StringPiece key) {
  HostAddr addr;
  auto hostSize = key.size() - kHostDirsTable.size() - sizeof(Port);
  addr.host = key.subpiece(kHostDirsTable.size(), hostSize).toString();
  key.advance(kHostDirsTable.size() + hostSize);
  addr.port = *reinterpret_cast<const Port*>(key.begin());
  return addr;
}

std::string MetaKeyUtils::hostDirVal(cpp2::DirInfo dir) {
  std::string val;
  apache::thrift::CompactSerializer::serialize(dir, &val);
  return val;
}

cpp2::DirInfo MetaKeyUtils::parseHostDir(folly::StringPiece val) {
  cpp2::DirInfo dir;
  apache::thrift::CompactSerializer::deserialize(val, dir);
  return dir;
}

std::string MetaKeyUtils::hostKey(std::string addr, Port port) {
  return hostKeyV2(addr, port);
}

std::string MetaKeyUtils::hostKeyV2(std::string addr, Port port) {
  std::string key;
  HostAddr h(addr, port);
  key.append(kHostsTable.data(), kHostsTable.size()).append(MetaKeyUtils::serializeHostAddr(h));
  return key;
}

const std::string& MetaKeyUtils::hostPrefix() {
  return kHostsTable;
}

HostAddr MetaKeyUtils::parseHostKey(folly::StringPiece key) {
  if (key.size() == kHostsTable.size() + sizeof(int64_t)) {
    return parseHostKeyV1(key);
  }
  return parseHostKeyV2(key);
}

HostAddr MetaKeyUtils::parseHostKeyV1(folly::StringPiece key) {
  HostAddr host;
  key.advance(kHostsTable.size());
  uint32_t ip = *reinterpret_cast<const uint32_t*>(key.begin());
  host.host = network::NetworkUtils::intToIPv4(ip);
  host.port = *reinterpret_cast<const int32_t*>(key.begin() + sizeof(uint32_t));
  return host;
}

HostAddr MetaKeyUtils::parseHostKeyV2(folly::StringPiece key) {
  key.advance(kHostsTable.size());
  return MetaKeyUtils::deserializeHostAddr(key);
}

std::string MetaKeyUtils::versionKey(const HostAddr& h) {
  std::string key;
  key.append(kVersionsTable.data(), kVersionsTable.size())
      .append(MetaKeyUtils::serializeHostAddr(h));
  return key;
}

std::string MetaKeyUtils::versionVal(const std::string& version) {
  std::string val;
  auto versionLen = version.size();
  val.reserve(sizeof(int64_t) + versionLen);
  val.append(reinterpret_cast<const char*>(&version), sizeof(int64_t)).append(version);
  return val;
}

std::string MetaKeyUtils::parseVersion(folly::StringPiece val) {
  auto len = *reinterpret_cast<const size_t*>(val.data());
  return val.subpiece(sizeof(size_t), len).str();
}

std::string MetaKeyUtils::leaderKey(std::string addr, Port port) {
  LOG(ERROR) << "deprecated function\n" << boost::stacktrace::stacktrace();
  return leaderKeyV2(addr, port);
}

std::string MetaKeyUtils::leaderKeyV2(std::string addr, Port port) {
  LOG(ERROR) << "deprecated function\n" << boost::stacktrace::stacktrace();
  std::string key;
  HostAddr h(addr, port);

  key.reserve(kLeadersTable.size() + kMaxIpAddrLen + sizeof(Port));
  key.append(kLeadersTable.data(), kLeadersTable.size()).append(MetaKeyUtils::serializeHostAddr(h));
  return key;
}

std::string MetaKeyUtils::leaderKey(GraphSpaceID spaceId, PartitionID partId) {
  std::string key;
  key.reserve(kLeaderTermsTable.size() + sizeof(GraphSpaceID) + sizeof(PartitionID));
  key.append(kLeaderTermsTable.data(), kLeaderTermsTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
      .append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
  return key;
}

std::string MetaKeyUtils::leaderVal(const LeaderParts& leaderParts) {
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
std::string MetaKeyUtils::leaderValV3(const HostAddr& h, int64_t term) {
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
std::tuple<HostAddr, TermID, nebula::cpp2::ErrorCode> MetaKeyUtils::parseLeaderValV3(
    folly::StringPiece val) {
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
  std::get<0>(ret) = MetaKeyUtils::deserializeHostAddr(val.subpiece(0, lenOfHost));

  val.advance(lenOfHost);
  std::get<1>(ret) = *reinterpret_cast<const TermID*>(val.data());
  return ret;
}

const std::string& MetaKeyUtils::leaderPrefix() {
  return kLeaderTermsTable;
}

std::string MetaKeyUtils::leaderPrefix(GraphSpaceID spaceId) {
  std::string key;
  key.reserve(kLeaderTermsTable.size() + sizeof(GraphSpaceID));
  key.append(kLeaderTermsTable.data(), kLeaderTermsTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
  return key;
}

HostAddr MetaKeyUtils::parseLeaderKey(folly::StringPiece key) {
  LOG(ERROR) << "deprecated function\n" << boost::stacktrace::stacktrace();
  if (key.size() == kLeadersTable.size() + sizeof(int64_t)) {
    return parseLeaderKeyV1(key);
  }
  return parseLeaderKeyV2(key);
}

// input should be a pair of int32_t
HostAddr MetaKeyUtils::parseLeaderKeyV1(folly::StringPiece key) {
  LOG(ERROR) << "deprecated function\n" << boost::stacktrace::stacktrace();
  HostAddr host;
  CHECK_EQ(key.size(), kLeadersTable.size() + sizeof(int64_t));
  key.advance(kLeadersTable.size());
  auto ip = *reinterpret_cast<const uint32_t*>(key.begin());
  host.host = network::NetworkUtils::intToIPv4(ip);
  host.port = *reinterpret_cast<const uint32_t*>(key.begin() + sizeof(ip));
  return host;
}

HostAddr MetaKeyUtils::parseLeaderKeyV2(folly::StringPiece key) {
  LOG(ERROR) << "deprecated function\n" << boost::stacktrace::stacktrace();
  key.advance(kLeadersTable.size());
  return MetaKeyUtils::deserializeHostAddr(key);
}

std::pair<GraphSpaceID, PartitionID> MetaKeyUtils::parseLeaderKeyV3(folly::StringPiece key) {
  std::pair<GraphSpaceID, PartitionID> ret;
  ret.first = *reinterpret_cast<const GraphSpaceID*>(key.data() + kLeaderTermsTable.size());
  ret.second = *reinterpret_cast<const PartitionID*>(key.data() + kLeaderTermsTable.size() +
                                                     sizeof(GraphSpaceID));
  return ret;
}

LeaderParts MetaKeyUtils::parseLeaderValV1(folly::StringPiece val) {
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

std::string MetaKeyUtils::schemaEdgePrefix(GraphSpaceID spaceId, EdgeType edgeType) {
  std::string key;
  key.reserve(kEdgesTable.size() + sizeof(GraphSpaceID) + sizeof(edgeType));
  key.append(kEdgesTable.data(), kEdgesTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
      .append(reinterpret_cast<const char*>(&edgeType), sizeof(edgeType));
  return key;
}

std::string MetaKeyUtils::schemaEdgesPrefix(GraphSpaceID spaceId) {
  std::string key;
  key.reserve(kEdgesTable.size() + sizeof(GraphSpaceID));
  key.append(kEdgesTable.data(), kEdgesTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
  return key;
}

const std::string& MetaKeyUtils::schemaEdgesPrefix() {
  return kEdgesTable;
}

std::string MetaKeyUtils::schemaEdgeKey(GraphSpaceID spaceId,
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

std::string MetaKeyUtils::schemaVal(const std::string& name, const meta::cpp2::Schema& schema) {
  auto len = name.size();
  std::string val, sval;
  apache::thrift::CompactSerializer::serialize(schema, &sval);
  val.reserve(sizeof(int32_t) + name.size() + sval.size());
  val.append(reinterpret_cast<const char*>(&len), sizeof(int32_t)).append(name).append(sval);
  return val;
}

EdgeType MetaKeyUtils::parseEdgeType(folly::StringPiece key) {
  return *reinterpret_cast<const EdgeType*>(key.data() + kEdgesTable.size() + sizeof(GraphSpaceID));
}

SchemaVer MetaKeyUtils::parseEdgeVersion(folly::StringPiece key) {
  auto offset = kEdgesTable.size() + sizeof(GraphSpaceID) + sizeof(EdgeType);
  return std::numeric_limits<SchemaVer>::max() -
         *reinterpret_cast<const SchemaVer*>(key.begin() + offset);
}

SchemaVer MetaKeyUtils::getLatestEdgeScheInfo(kvstore::KVIterator* iter, folly::StringPiece& val) {
  SchemaVer maxVer = MetaKeyUtils::parseEdgeVersion(iter->key());
  val = iter->val();
  iter->next();
  while (iter->valid()) {
    SchemaVer curVer = MetaKeyUtils::parseEdgeVersion(iter->key());
    if (curVer > maxVer) {
      maxVer = curVer;
      val = iter->val();
    }
    iter->next();
  }
  return maxVer;
}

GraphSpaceID MetaKeyUtils::parseEdgesKeySpaceID(folly::StringPiece key) {
  return *reinterpret_cast<const GraphSpaceID*>(key.data() + kEdgesTable.size());
}

std::string MetaKeyUtils::schemaTagKey(GraphSpaceID spaceId, TagID tagId, SchemaVer version) {
  auto storageVer = std::numeric_limits<SchemaVer>::max() - version;
  std::string key;
  key.reserve(kTagsTable.size() + sizeof(GraphSpaceID) + sizeof(TagID) + sizeof(SchemaVer));
  key.append(kTagsTable.data(), kTagsTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
      .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID))
      .append(reinterpret_cast<const char*>(&storageVer), sizeof(SchemaVer));
  return key;
}

TagID MetaKeyUtils::parseTagId(folly::StringPiece key) {
  return *reinterpret_cast<const TagID*>(key.data() + kTagsTable.size() + sizeof(GraphSpaceID));
}

SchemaVer MetaKeyUtils::parseTagVersion(folly::StringPiece key) {
  auto offset = kTagsTable.size() + sizeof(GraphSpaceID) + sizeof(TagID);
  return std::numeric_limits<SchemaVer>::max() -
         *reinterpret_cast<const SchemaVer*>(key.begin() + offset);
}

SchemaVer MetaKeyUtils::getLatestTagScheInfo(kvstore::KVIterator* iter, folly::StringPiece& val) {
  SchemaVer maxVer = MetaKeyUtils::parseTagVersion(iter->key());
  val = iter->val();
  iter->next();
  while (iter->valid()) {
    SchemaVer curVer = MetaKeyUtils::parseTagVersion(iter->key());
    if (curVer > maxVer) {
      maxVer = curVer;
      val = iter->val();
    }
    iter->next();
  }
  return maxVer;
}

std::string MetaKeyUtils::schemaTagPrefix(GraphSpaceID spaceId, TagID tagId) {
  std::string key;
  key.reserve(kTagsTable.size() + sizeof(GraphSpaceID) + sizeof(TagID));
  key.append(kTagsTable.data(), kTagsTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
      .append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
  return key;
}

const std::string& MetaKeyUtils::schemaTagsPrefix() {
  return kTagsTable;
}

std::string MetaKeyUtils::schemaTagsPrefix(GraphSpaceID spaceId) {
  std::string key;
  key.reserve(kTagsTable.size() + sizeof(GraphSpaceID));
  key.append(kTagsTable.data(), kTagsTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
  return key;
}

GraphSpaceID MetaKeyUtils::parseTagsKeySpaceID(folly::StringPiece key) {
  return *reinterpret_cast<const GraphSpaceID*>(key.data() + kTagsTable.size());
}

meta::cpp2::Schema MetaKeyUtils::parseSchema(folly::StringPiece rawData) {
  meta::cpp2::Schema schema;
  int32_t offset = sizeof(int32_t) + *reinterpret_cast<const int32_t*>(rawData.begin());
  auto schval = rawData.subpiece(offset, rawData.size() - offset);
  apache::thrift::CompactSerializer::deserialize(schval, schema);
  return schema;
}

std::string MetaKeyUtils::indexKey(GraphSpaceID spaceID, IndexID indexID) {
  std::string key;
  key.reserve(kIndexesTable.size() + sizeof(GraphSpaceID) + sizeof(IndexID));
  key.append(kIndexesTable.data(), kIndexesTable.size())
      .append(reinterpret_cast<const char*>(&spaceID), sizeof(GraphSpaceID))
      .append(reinterpret_cast<const char*>(&indexID), sizeof(IndexID));
  return key;
}

std::string MetaKeyUtils::indexVal(const nebula::meta::cpp2::IndexItem& item) {
  std::string value;
  apache::thrift::CompactSerializer::serialize(item, &value);
  return value;
}

const std::string& MetaKeyUtils::indexPrefix() {
  return kIndexesTable;
}

std::string MetaKeyUtils::indexPrefix(GraphSpaceID spaceId) {
  std::string key;
  key.reserve(kIndexesTable.size() + sizeof(GraphSpaceID));
  key.append(kIndexesTable.data(), kIndexesTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
  return key;
}

GraphSpaceID MetaKeyUtils::parseIndexesKeySpaceID(folly::StringPiece key) {
  return *reinterpret_cast<const GraphSpaceID*>(key.data() + kIndexesTable.size());
}

IndexID MetaKeyUtils::parseIndexesKeyIndexID(folly::StringPiece key) {
  return *reinterpret_cast<const IndexID*>(key.data() + kIndexesTable.size() +
                                           sizeof(GraphSpaceID));
}

nebula::meta::cpp2::IndexItem MetaKeyUtils::parseIndex(const folly::StringPiece& rawData) {
  nebula::meta::cpp2::IndexItem item;
  apache::thrift::CompactSerializer::deserialize(rawData, item);
  return item;
}

// This method should replace with JobManager when it ready.
std::string MetaKeyUtils::rebuildIndexStatus(GraphSpaceID space,
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
std::string MetaKeyUtils::rebuildIndexStatusPrefix(GraphSpaceID space, char type) {
  std::string key;
  key.reserve(kIndexStatusTable.size() + sizeof(GraphSpaceID) + sizeof(char));
  key.append(kIndexStatusTable.data(), kIndexStatusTable.size())
      .append(reinterpret_cast<const char*>(&space), sizeof(GraphSpaceID))
      .append(1, type);
  return key;
}

std::string MetaKeyUtils::rebuildIndexStatusPrefix() {
  std::string key;
  key.reserve(kIndexStatusTable.size());
  key.append(kIndexStatusTable.data(), kIndexStatusTable.size());
  return key;
}

GraphSpaceID MetaKeyUtils::parseIndexStatusKeySpaceID(folly::StringPiece key) {
  return *reinterpret_cast<const GraphSpaceID*>(key.data() + kIndexStatusTable.size());
}

std::string MetaKeyUtils::indexSpaceKey(const std::string& name) {
  EntryType type = EntryType::SPACE;
  std::string key;
  key.reserve(64);
  key.append(kIndexTable.data(), kIndexTable.size())
      .append(reinterpret_cast<const char*>(&type), sizeof(type))
      .append(name);
  return key;
}

std::string MetaKeyUtils::parseIndexSpaceKey(folly::StringPiece key) {
  auto nameSize = key.size() - kIndexTable.size() - sizeof(EntryType);
  return key.subpiece(kIndexTable.size() + sizeof(EntryType), nameSize).str();
}

EntryType MetaKeyUtils::parseIndexType(folly::StringPiece key) {
  auto type = *reinterpret_cast<const EntryType*>(key.data() + kIndexTable.size());
  return type;
}

std::string MetaKeyUtils::indexTagKey(GraphSpaceID spaceId, const std::string& name) {
  EntryType type = EntryType::TAG;
  std::string key;
  key.reserve(128);
  key.append(kIndexTable.data(), kIndexTable.size())
      .append(reinterpret_cast<const char*>(&type), sizeof(type))
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
      .append(name);
  return key;
}

std::string MetaKeyUtils::indexEdgeKey(GraphSpaceID spaceId, const std::string& name) {
  EntryType type = EntryType::EDGE;
  std::string key;
  key.reserve(128);
  key.append(kIndexTable.data(), kIndexTable.size())
      .append(reinterpret_cast<const char*>(&type), sizeof(type))
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
      .append(name);
  return key;
}

GraphSpaceID MetaKeyUtils::parseIndexKeySpaceID(folly::StringPiece key) {
  return *reinterpret_cast<const GraphSpaceID*>(key.data() + kIndexTable.size() +
                                                sizeof(EntryType));
}

std::string MetaKeyUtils::indexIndexKey(GraphSpaceID spaceID, const std::string& indexName) {
  EntryType type = EntryType::INDEX;
  std::string key;
  key.reserve(128);
  key.append(kIndexTable.data(), kIndexTable.size())
      .append(reinterpret_cast<const char*>(&type), sizeof(type))
      .append(reinterpret_cast<const char*>(&spaceID), sizeof(GraphSpaceID))
      .append(indexName);
  return key;
}

std::string MetaKeyUtils::indexZoneKey(const std::string& name) {
  EntryType type = EntryType::ZONE;
  std::string key;
  key.reserve(128);
  key.append(kIndexTable.data(), kIndexTable.size())
      .append(reinterpret_cast<const char*>(&type), sizeof(type))
      .append(name);
  return key;
}

std::string MetaKeyUtils::userPrefix() {
  return kUsersTable;
}

std::string MetaKeyUtils::userKey(const std::string& account) {
  std::string key;
  key.reserve(kUsersTable.size() + account.size());
  key.append(kUsersTable.data(), kUsersTable.size()).append(account);
  return key;
}

std::string MetaKeyUtils::userVal(const std::string& val) {
  std::string key;
  auto pwdLen = val.size();
  key.reserve(sizeof(int64_t) + pwdLen);
  key.append(reinterpret_cast<const char*>(&pwdLen), sizeof(size_t)).append(val);
  return key;
}

std::string MetaKeyUtils::parseUser(folly::StringPiece key) {
  return key.subpiece(kUsersTable.size(), key.size() - kUsersTable.size()).str();
}

std::string MetaKeyUtils::parseUserPwd(folly::StringPiece val) {
  auto len = *reinterpret_cast<const size_t*>(val.data());
  return val.subpiece(sizeof(size_t), len).str();
}

std::string MetaKeyUtils::roleKey(GraphSpaceID spaceId, const std::string& account) {
  std::string key;
  key.reserve(kRolesTable.size() + sizeof(GraphSpaceID) + account.size());
  key.append(kRolesTable.data(), kRolesTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
      .append(account);
  return key;
}

std::string MetaKeyUtils::roleVal(meta::cpp2::RoleType roleType) {
  std::string val;
  val.reserve(sizeof(meta::cpp2::RoleType));
  val.append(reinterpret_cast<const char*>(&roleType), sizeof(meta::cpp2::RoleType));
  return val;
}

std::string MetaKeyUtils::parseRoleUser(folly::StringPiece key) {
  auto offset = kRolesTable.size() + sizeof(GraphSpaceID);
  return key.subpiece(offset, key.size() - offset).str();
}

GraphSpaceID MetaKeyUtils::parseRoleSpace(folly::StringPiece key) {
  return *reinterpret_cast<const GraphSpaceID*>(key.data() + kRolesTable.size());
}

std::string MetaKeyUtils::rolesPrefix() {
  return kRolesTable;
}

std::string MetaKeyUtils::roleSpacePrefix(GraphSpaceID spaceId) {
  std::string key;
  key.reserve(kRolesTable.size() + sizeof(GraphSpaceID));
  key.append(kRolesTable.data(), kRolesTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
  return key;
}

std::string MetaKeyUtils::parseRoleStr(folly::StringPiece key) {
  auto* c = reinterpret_cast<const char*>(&key);
  auto type = *reinterpret_cast<const meta::cpp2::RoleType*>(c);
  std::string role;
  switch (type) {
    case meta::cpp2::RoleType::GOD: {
      role = "GOD";
      break;
    }
    case meta::cpp2::RoleType::ADMIN: {
      role = "ADMIN";
      break;
    }
    case meta::cpp2::RoleType::DBA: {
      role = "DBA";
      break;
    }
    case meta::cpp2::RoleType::USER: {
      role = "USER";
      break;
    }
    case meta::cpp2::RoleType::GUEST: {
      role = "GUEST";
      break;
    }
  }
  return role;
}

std::string MetaKeyUtils::configKey(const meta::cpp2::ConfigModule& module,
                                    const std::string& name) {
  int32_t nSize = name.size();
  std::string key;
  key.reserve(128);
  key.append(kConfigsTable.data(), kConfigsTable.size())
      .append(reinterpret_cast<const char*>(&module), sizeof(meta::cpp2::ConfigModule))
      .append(reinterpret_cast<const char*>(&nSize), sizeof(int32_t))
      .append(name);
  return key;
}

std::string MetaKeyUtils::configKeyPrefix(const meta::cpp2::ConfigModule& module) {
  std::string key;
  key.reserve(128);
  key.append(kConfigsTable.data(), kConfigsTable.size());
  if (module != meta::cpp2::ConfigModule::ALL) {
    key.append(reinterpret_cast<const char*>(&module), sizeof(meta::cpp2::ConfigModule));
  }
  return key;
}

std::string MetaKeyUtils::configValue(const meta::cpp2::ConfigMode& valueMode, const Value& value) {
  std::string val, cVal;
  apache::thrift::CompactSerializer::serialize(value, &cVal);
  val.reserve(sizeof(meta::cpp2::ConfigMode) + cVal.size());
  val.append(reinterpret_cast<const char*>(&valueMode), sizeof(meta::cpp2::ConfigMode))
      .append(cVal);
  return val;
}

ConfigName MetaKeyUtils::parseConfigKey(folly::StringPiece rawKey) {
  std::string key;
  auto offset = kConfigsTable.size();
  auto module = *reinterpret_cast<const meta::cpp2::ConfigModule*>(rawKey.data() + offset);
  offset += sizeof(meta::cpp2::ConfigModule);
  int32_t nSize = *reinterpret_cast<const int32_t*>(rawKey.data() + offset);
  offset += sizeof(int32_t);
  auto name = rawKey.subpiece(offset, nSize);
  return {module, name.str()};
}

meta::cpp2::ConfigItem MetaKeyUtils::parseConfigValue(folly::StringPiece rawData) {
  int32_t offset = 0;
  meta::cpp2::ConfigMode mode =
      *reinterpret_cast<const meta::cpp2::ConfigMode*>(rawData.data() + offset);
  offset += sizeof(meta::cpp2::ConfigMode);
  Value value;
  apache::thrift::CompactSerializer::deserialize(rawData.subpiece(offset, rawData.size() - offset),
                                                 value);

  meta::cpp2::ConfigItem item;
  item.mode_ref() = mode;
  item.value_ref() = value;
  return item;
}

std::string MetaKeyUtils::snapshotKey(const std::string& name) {
  std::string key;
  key.reserve(kSnapshotsTable.size() + name.size());
  key.append(kSnapshotsTable.data(), kSnapshotsTable.size()).append(name);
  return key;
}

std::string MetaKeyUtils::snapshotVal(const meta::cpp2::SnapshotStatus& status,
                                      const std::string& hosts) {
  std::string val;
  val.reserve(sizeof(meta::cpp2::SnapshotStatus) + sizeof(hosts));
  val.append(reinterpret_cast<const char*>(&status), sizeof(meta::cpp2::SnapshotStatus))
      .append(hosts);
  return val;
}

meta::cpp2::SnapshotStatus MetaKeyUtils::parseSnapshotStatus(folly::StringPiece rawData) {
  return *reinterpret_cast<const meta::cpp2::SnapshotStatus*>(rawData.data());
}

std::string MetaKeyUtils::parseSnapshotHosts(folly::StringPiece rawData) {
  return rawData
      .subpiece(sizeof(meta::cpp2::SnapshotStatus),
                rawData.size() - sizeof(meta::cpp2::SnapshotStatus))
      .str();
}

std::string MetaKeyUtils::parseSnapshotName(folly::StringPiece rawData) {
  int32_t offset = kSnapshotsTable.size();
  return rawData.subpiece(offset, rawData.size() - offset).str();
}

const std::string& MetaKeyUtils::snapshotPrefix() {
  return kSnapshotsTable;
}

std::string MetaKeyUtils::serializeHostAddr(const HostAddr& host) {
  std::string ret;
  ret.reserve(sizeof(size_t) + 15 + sizeof(Port));  // 255.255.255.255
  size_t len = host.host.size();
  ret.append(reinterpret_cast<char*>(&len), sizeof(size_t))
      .append(host.host.data(), len)
      .append(reinterpret_cast<const char*>(&host.port), sizeof(Port));
  return ret;
}

HostAddr MetaKeyUtils::deserializeHostAddr(folly::StringPiece raw) {
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

std::string MetaKeyUtils::genTimestampStr() {
  char ch[60];
  std::time_t t = std::time(nullptr);
  std::strftime(ch, sizeof(ch), "%Y_%m_%d_%H_%M_%S", localtime(&t));
  return ch;
}

std::string MetaKeyUtils::idKey() {
  return kIdKey;
}

std::string MetaKeyUtils::balanceTaskKey(
    JobID jobId, GraphSpaceID spaceId, PartitionID partId, HostAddr src, HostAddr dst) {
  std::string key;
  key.reserve(64);
  key.append(reinterpret_cast<const char*>(kBalanceTaskTable.data()), kBalanceTaskTable.size())
      .append(reinterpret_cast<const char*>(&jobId), sizeof(JobID))
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
      .append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID))
      .append(serializeHostAddr(src))
      .append(serializeHostAddr(dst));
  return key;
}

std::string MetaKeyUtils::balanceTaskVal(BalanceTaskStatus status,
                                         BalanceTaskResult result,
                                         int64_t startTime,
                                         int64_t endTime) {
  std::string val;
  val.reserve(32);
  val.append(reinterpret_cast<const char*>(&status), sizeof(BalanceTaskStatus))
      .append(reinterpret_cast<const char*>(&result), sizeof(BalanceTaskResult))
      .append(reinterpret_cast<const char*>(&startTime), sizeof(int64_t))
      .append(reinterpret_cast<const char*>(&endTime), sizeof(int64_t));
  return val;
}

std::string MetaKeyUtils::balanceTaskPrefix(JobID jobId) {
  std::string prefix;
  prefix.reserve(32);
  prefix.append(reinterpret_cast<const char*>(kBalanceTaskTable.data()), kBalanceTaskTable.size())
      .append(reinterpret_cast<const char*>(&jobId), sizeof(JobID));
  return prefix;
}

std::tuple<BalanceID, GraphSpaceID, PartitionID, HostAddr, HostAddr>
MetaKeyUtils::parseBalanceTaskKey(const folly::StringPiece& rawKey) {
  uint32_t offset = kBalanceTaskTable.size();
  auto jobId = *reinterpret_cast<const JobID*>(rawKey.begin() + offset);
  offset += sizeof(jobId);
  auto spaceId = *reinterpret_cast<const GraphSpaceID*>(rawKey.begin() + offset);
  offset += sizeof(GraphSpaceID);
  auto partId = *reinterpret_cast<const PartitionID*>(rawKey.begin() + offset);
  offset += sizeof(PartitionID);
  auto src = MetaKeyUtils::deserializeHostAddr({rawKey, offset});
  offset += src.host.size() + sizeof(size_t) + sizeof(uint32_t);
  auto dst = MetaKeyUtils::deserializeHostAddr({rawKey, offset});
  return std::make_tuple(jobId, spaceId, partId, src, dst);
}

std::tuple<BalanceTaskStatus, BalanceTaskResult, int64_t, int64_t>
MetaKeyUtils::parseBalanceTaskVal(const folly::StringPiece& rawVal) {
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

std::string MetaKeyUtils::zoneKey(const std::string& zone) {
  std::string key;
  key.reserve(kZonesTable.size() + zone.size());
  key.append(kZonesTable.data(), kZonesTable.size()).append(zone);
  return key;
}

std::string MetaKeyUtils::zoneVal(const std::vector<HostAddr>& hosts) {
  std::string value;
  value.append(network::NetworkUtils::toHostsStr(hosts));
  return value;
}

const std::string& MetaKeyUtils::zonePrefix() {
  return kZonesTable;
}

std::string MetaKeyUtils::parseZoneName(folly::StringPiece rawData) {
  return rawData.subpiece(kZonesTable.size(), rawData.size()).toString();
}

std::vector<HostAddr> MetaKeyUtils::parseZoneHosts(folly::StringPiece rawData) {
  std::vector<HostAddr> addresses;
  auto hostsOrErr = network::NetworkUtils::toHosts(rawData.str());
  if (hostsOrErr.ok()) {
    addresses = std::move(hostsOrErr.value());
  } else {
    LOG(ERROR) << "invalid input for parseZoneHosts()";
  }
  return addresses;
}

std::string MetaKeyUtils::listenerKey(GraphSpaceID spaceId,
                                      PartitionID partId,
                                      meta::cpp2::ListenerType type) {
  std::string key;
  key.reserve(kListenerTable.size() + sizeof(GraphSpaceID) + sizeof(meta::cpp2::ListenerType) +
              sizeof(PartitionID));
  key.append(kListenerTable.data(), kListenerTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
      .append(reinterpret_cast<const char*>(&type), sizeof(meta::cpp2::ListenerType))
      .append(reinterpret_cast<const char*>(&partId), sizeof(PartitionID));
  return key;
}

std::string MetaKeyUtils::listenerPrefix(GraphSpaceID spaceId) {
  std::string key;
  key.reserve(kListenerTable.size() + sizeof(GraphSpaceID));
  key.append(kListenerTable.data(), kListenerTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
  return key;
}

std::string MetaKeyUtils::listenerPrefix(GraphSpaceID spaceId, meta::cpp2::ListenerType type) {
  std::string key;
  key.reserve(kListenerTable.size() + sizeof(GraphSpaceID) + sizeof(meta::cpp2::ListenerType));
  key.append(kListenerTable.data(), kListenerTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID))
      .append(reinterpret_cast<const char*>(&type), sizeof(meta::cpp2::ListenerType));
  return key;
}

meta::cpp2::ListenerType MetaKeyUtils::parseListenerType(folly::StringPiece rawData) {
  auto offset = kListenerTable.size() + sizeof(GraphSpaceID);
  return *reinterpret_cast<const meta::cpp2::ListenerType*>(rawData.data() + offset);
}

GraphSpaceID MetaKeyUtils::parseListenerSpace(folly::StringPiece rawData) {
  auto offset = kListenerTable.size();
  return *reinterpret_cast<const GraphSpaceID*>(rawData.data() + offset);
}

PartitionID MetaKeyUtils::parseListenerPart(folly::StringPiece rawData) {
  auto offset = kListenerTable.size() + sizeof(meta::cpp2::ListenerType) + sizeof(GraphSpaceID);
  return *reinterpret_cast<const PartitionID*>(rawData.data() + offset);
}

std::string MetaKeyUtils::statsKey(GraphSpaceID spaceId) {
  std::string key;
  key.reserve(kStatsTable.size() + sizeof(GraphSpaceID));
  key.append(kStatsTable.data(), kStatsTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
  return key;
}

std::string MetaKeyUtils::statsVal(const meta::cpp2::StatsItem& statsItem) {
  std::string val;
  apache::thrift::CompactSerializer::serialize(statsItem, &val);
  return val;
}

meta::cpp2::StatsItem MetaKeyUtils::parseStatsVal(folly::StringPiece rawData) {
  meta::cpp2::StatsItem statsItem;
  apache::thrift::CompactSerializer::deserialize(rawData, statsItem);
  return statsItem;
}

GraphSpaceID MetaKeyUtils::parseStatsSpace(folly::StringPiece rawData) {
  auto offset = kStatsTable.size();
  return *reinterpret_cast<const GraphSpaceID*>(rawData.data() + offset);
}

const std::string& MetaKeyUtils::statsKeyPrefix() {
  return kStatsTable;
}

std::string MetaKeyUtils::serviceKey(const meta::cpp2::ExternalServiceType& type) {
  std::string key;
  key.reserve(kServicesTable.size() + sizeof(meta::cpp2::ExternalServiceType));
  key.append(kServicesTable.data(), kServicesTable.size())
      .append(reinterpret_cast<const char*>(&type), sizeof(meta::cpp2::ExternalServiceType));
  return key;
}

const std::string& MetaKeyUtils::servicePrefix() {
  return kServicesTable;
}

meta::cpp2::ExternalServiceType MetaKeyUtils::parseServiceType(folly::StringPiece rawData) {
  auto offset = kServicesTable.size();
  return *reinterpret_cast<const meta::cpp2::ExternalServiceType*>(rawData.data() + offset);
}

std::string MetaKeyUtils::serviceVal(const std::vector<meta::cpp2::ServiceClient>& clients) {
  std::string val;
  apache::thrift::CompactSerializer::serialize(clients, &val);
  return val;
}

std::vector<meta::cpp2::ServiceClient> MetaKeyUtils::parseServiceClients(
    folly::StringPiece rawData) {
  std::vector<meta::cpp2::ServiceClient> clients;
  apache::thrift::CompactSerializer::deserialize(rawData, clients);
  return clients;
}

const std::string& MetaKeyUtils::sessionPrefix() {
  return kSessionsTable;
}

std::string MetaKeyUtils::sessionKey(SessionID sessionId) {
  std::string key;
  key.reserve(kSessionsTable.size() + sizeof(sessionId));
  key.append(kSessionsTable.data(), kSessionsTable.size())
      .append(reinterpret_cast<const char*>(&sessionId), sizeof(SessionID));
  return key;
}

std::string MetaKeyUtils::sessionVal(const meta::cpp2::Session& session) {
  std::string val;
  apache::thrift::CompactSerializer::serialize(session, &val);
  return val;
}

SessionID MetaKeyUtils::getSessionId(const folly::StringPiece& key) {
  return *reinterpret_cast<const SessionID*>(key.data() + kSessionsTable.size());
}

meta::cpp2::Session MetaKeyUtils::parseSessionVal(const folly::StringPiece& val) {
  meta::cpp2::Session session;
  apache::thrift::CompactSerializer::deserialize(val, session);
  return session;
}

std::string MetaKeyUtils::fulltextIndexKey(const std::string& indexName) {
  std::string key;
  key.reserve(kFTIndexTable.size() + indexName.size());
  key.append(kFTIndexTable.data(), kFTIndexTable.size()).append(indexName);
  return key;
}

std::string MetaKeyUtils::fulltextIndexVal(const meta::cpp2::FTIndex& index) {
  std::string val;
  apache::thrift::CompactSerializer::serialize(index, &val);
  return val;
}

std::string MetaKeyUtils::parsefulltextIndexName(folly::StringPiece key) {
  return key.subpiece(kFTIndexTable.size(), key.size()).toString();
}

meta::cpp2::FTIndex MetaKeyUtils::parsefulltextIndex(folly::StringPiece val) {
  meta::cpp2::FTIndex ftIndex;
  apache::thrift::CompactSerializer::deserialize(val, ftIndex);
  return ftIndex;
}

std::string MetaKeyUtils::fulltextIndexPrefix() {
  return kFTIndexTable;
}

std::string MetaKeyUtils::localIdKey(GraphSpaceID spaceId) {
  std::string key;
  key.reserve(kLocalIdTable.size() + sizeof(GraphSpaceID));
  key.append(kLocalIdTable.data(), kLocalIdTable.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
  return key;
}

GraphSpaceID MetaKeyUtils::parseLocalIdSpace(folly::StringPiece rawData) {
  auto offset = kLocalIdTable.size();
  return *reinterpret_cast<const GraphSpaceID*>(rawData.data() + offset);
}

/**
 * diskPartsKey = kDiskPartsTable +
 *                len(serialized(hostAddr)) + serialized(hostAddr) +
 *                space id +
 *                disk path
 */

HostAddr MetaKeyUtils::parseDiskPartsHost(const folly::StringPiece& rawData) {
  auto offset = kDiskPartsTable.size();
  auto hostAddrLen = *reinterpret_cast<const size_t*>(rawData.begin() + offset);
  offset += sizeof(size_t);
  std::string hostAddrStr(rawData.data() + offset, hostAddrLen);
  return deserializeHostAddr(hostAddrStr);
}

GraphSpaceID MetaKeyUtils::parseDiskPartsSpace(const folly::StringPiece& rawData) {
  auto offset = kDiskPartsTable.size();
  size_t hostAddrLen = *reinterpret_cast<const size_t*>(rawData.begin() + offset);
  offset += sizeof(size_t) + hostAddrLen;
  return *reinterpret_cast<const GraphSpaceID*>(rawData.begin() + offset);
}

std::string MetaKeyUtils::parseDiskPartsPath(const folly::StringPiece& rawData) {
  auto offset = kDiskPartsTable.size();
  size_t hostAddrLen = *reinterpret_cast<const size_t*>(rawData.begin() + offset);
  offset += sizeof(size_t) + hostAddrLen + sizeof(GraphSpaceID);
  std::string path(rawData.begin() + offset, rawData.size() - offset);
  return path;
}

std::string MetaKeyUtils::diskPartsPrefix() {
  return kDiskPartsTable;
}

std::string MetaKeyUtils::diskPartsPrefix(HostAddr addr) {
  std::string key;
  std::string hostStr = serializeHostAddr(addr);
  size_t hostAddrLen = hostStr.size();
  key.reserve(kDiskPartsTable.size() + sizeof(size_t) + hostStr.size());
  key.append(kDiskPartsTable.data(), kDiskPartsTable.size())
      .append(reinterpret_cast<const char*>(&hostAddrLen), sizeof(size_t))
      .append(hostStr.data(), hostStr.size());
  return key;
}

std::string MetaKeyUtils::diskPartsPrefix(HostAddr addr, GraphSpaceID spaceId) {
  std::string key;
  std::string prefix = diskPartsPrefix(addr);
  key.reserve(prefix.size() + sizeof(GraphSpaceID));
  key.append(prefix.data(), prefix.size())
      .append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
  return key;
}

std::string MetaKeyUtils::diskPartsKey(HostAddr addr,
                                       GraphSpaceID spaceId,
                                       const std::string& path) {
  std::string key;
  std::string prefix = diskPartsPrefix(addr, spaceId);
  key.reserve(prefix.size() + path.size());
  key.append(prefix.data(), prefix.size()).append(path.data(), path.size());
  return key;
}

std::string MetaKeyUtils::diskPartsVal(const meta::cpp2::PartitionList& partList) {
  std::string val;
  apache::thrift::CompactSerializer::serialize(partList, &val);
  return val;
}

meta::cpp2::PartitionList MetaKeyUtils::parseDiskPartsVal(const folly::StringPiece& rawData) {
  meta::cpp2::PartitionList partList;
  apache::thrift::CompactSerializer::deserialize(rawData, partList);
  return partList;
}

}  // namespace nebula
