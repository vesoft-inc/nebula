/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/MetaCache.h"

#include "common/utils/MetaKeyUtils.h"
#include "meta/MetaVersionMan.h"
#include "meta/ActiveHostsMan.h"

namespace nebula {
namespace meta {

/********************************************************************
 *
 * HostInfo methods
 *
 *******************************************************************/
bool HostInfo::operator==(const HostInfo& rhs) const {
  return this->role_ == rhs.role_ &&
         this->status_ == rhs.status_ &&
         this->execVer_ == rhs.execVer_ &&
         this->gitSha_ == rhs.gitSha_;
}


const cpp2::HostRole& HostInfo::getRole() const {
  return role_;
}


HostInfo& HostInfo::setRole(cpp2::HostRole role) {
  role_ = role;
  return *this;
}


const cpp2::HostStatus& HostInfo::getStatus() const {
  return status_;
}


HostInfo& HostInfo::setStatus(cpp2::HostStatus status) {
  status_ = status;
  return *this;
}


const std::string& HostInfo::getExecVer() const {
  return execVer_;
}


HostInfo& HostInfo::setExecVer(const std::string& ver) {
  execVer_ = ver;
  return *this;
}


const std::string& HostInfo::getGitSha() const {
  return gitSha_;
}


HostInfo& HostInfo::setGitSha(const std::string& sha) {
  gitSha_ = sha;
  return *this;
}


int64_t HostInfo::getLastHBTimeInMilliSec() const {
  return lastHBTimeInMilliSec_;
}


HostInfo& HostInfo::setLastHBTimeInMilliSec(int64_t ms) {
  lastHBTimeInMilliSec_ = ms;
  return *this;
}


// static
std::string HostInfo::encode(const HostInfo& info) {
  //
  // int8_t           codec version (3)
  // int8_t           status
  // sizeof(HostRole) hostRole
  // size_t           length of git sha
  // string           git sha
  // size_t           length of executable version
  // string           Executable version
  //

  std::string encode;
  // codec version
  int8_t codecVer = 3;
  encode.append(reinterpret_cast<const char*>(&codecVer), sizeof(int8_t));
  // status
  encode.append(reinterpret_cast<const char*>(&info.status_), sizeof(cpp2::HostStatus));
  // Role
  encode.append(reinterpret_cast<const char*>(&info.role_), sizeof(cpp2::HostRole));

  // Length of git sha
  size_t len = info.gitSha_.size();
  encode.append(reinterpret_cast<const char*>(&len), sizeof(size_t));
  // Git sha
  if (len > 0) {
    encode.append(info.gitSha_.data(), len);
  }

  // Executable version
  len = info.execVer_.size();
  encode.append(reinterpret_cast<const char*>(&len), sizeof(std::size_t));
  if (len > 0) {
    encode.append(info.execVer_.data(), len);
  }

  return encode;
}


// static
HostInfo HostInfo::decode(const folly::StringPiece& data) {
  if (data.size() == sizeof(int64_t)) {
    return decodeV1(data);
  } else if (*reinterpret_cast<const int8_t*>(data.data()) == 2) {
    return decodeV2(data);
  } else {
    return decodeV3(data);
  }
}


// static
HostInfo HostInfo::decodeV1(const folly::StringPiece& data) {
  HostInfo info;
  info.lastHBTimeInMilliSec_ = *reinterpret_cast<const int64_t*>(data.data());
  info.role_ = cpp2::HostRole::STORAGE;
  return info;
}


// static
HostInfo HostInfo::decodeV2(const folly::StringPiece& data) {
  HostInfo info;
  size_t offset = sizeof(int8_t);
  // Verify it's ver 2.0
  CHECK(*reinterpret_cast<const int8_t*>(data.data()) == 2);

  //
  // int8_t           codec version (2)
  // int64_t          timestamp
  // sizeof(HostRole) hostRole
  // size_t           length of git sha
  // string           git sha
  // size_t           length of execuable version
  // string           executable version
  //
  info.lastHBTimeInMilliSec_ = *reinterpret_cast<const int64_t*>(data.data() + offset);
  offset += sizeof(int64_t);

  if (data.size() - offset < sizeof(cpp2::HostRole)) {
    FLOG_FATAL("decode out of range, offset=%zu, actual=%zu", offset, data.size());
  }
  info.role_ = *reinterpret_cast<const cpp2::HostRole*>(data.data() + offset);
  offset += sizeof(cpp2::HostRole);

  if (offset + sizeof(size_t) > data.size()) {
    FLOG_FATAL("decode out of range, offset=%zu, actual=%zu", offset, data.size());
  }
  size_t len = *reinterpret_cast<const size_t*>(data.data() + offset);
  offset += sizeof(size_t);

  if (offset + len > data.size()) {
    FLOG_FATAL("decode out of range, offset=%zu, actual=%zu", offset, data.size());
  }

  info.gitSha_ = std::string(data.data() + offset, len);
  offset += len;

  if (offset == data.size()) {
    return info;
  }

  len = *reinterpret_cast<const size_t*>(data.data() + offset);
  offset += sizeof(size_t);

  if (offset + len > data.size()) {
    FLOG_FATAL("decode out of range, offset=%zu, actual=%zu", offset, data.size());
  }

  info.execVer_ = std::string(data.data() + offset, len);
  return info;
}


// static
HostInfo HostInfo::decodeV3(const folly::StringPiece& data) {
  HostInfo info;
  size_t offset = sizeof(int8_t);
  // Verify it's ver 3.0
  CHECK(*reinterpret_cast<const int8_t*>(data.data()) == 3);

  //
  // int8_t           codec version (3)
  // int8_t           status
  // sizeof(HostRole) hostRole
  // size_t           length of git sha
  // string           git sha
  // size_t           length of execuable version
  // string           executable version
  //

  // Status
  info.status_ = *reinterpret_cast<const cpp2::HostStatus*>(data.data() + offset);
  offset += sizeof(cpp2::HostStatus);

  // Role
  if (offset + sizeof(cpp2::HostRole) > data.size()) {
    FLOG_FATAL("decode out of range, offset=%zu, actual=%zu", offset, data.size());
  }
  info.role_ = *reinterpret_cast<const cpp2::HostRole*>(data.data() + offset);
  offset += sizeof(cpp2::HostRole);

  // git sha length
  if (offset + sizeof(size_t) > data.size()) {
    FLOG_FATAL("decode out of range, offset=%zu, actual=%zu", offset, data.size());
  }
  size_t len = *reinterpret_cast<const size_t*>(data.data() + offset);
  offset += sizeof(size_t);

  // git sha
  if (offset + len > data.size()) {
    FLOG_FATAL("decode out of range, offset=%zu, actual=%zu", offset, data.size());
  }
  info.gitSha_ = std::string(data.data() + offset, len);
  offset += len;

  // Executable version length
  if (offset + sizeof(size_t) > data.size()) {
    FLOG_FATAL("decode out of range, offset=%zu, actual=%zu", offset, data.size());
  }
  len = *reinterpret_cast<const size_t*>(data.data() + offset);
  offset += sizeof(size_t);

  // Executable version
  if (offset + len > data.size()) {
    FLOG_FATAL("decode out of range, offset=%zu, actual=%zu", offset, data.size());
  }
  info.execVer_ = std::string(data.data() + offset, len);

  return info;
}


/********************************************************************
 *
 * MetaCache methods
 *
 ********************************************************************/
ErrorOr<nebula::cpp2::ErrorCode, std::unique_ptr<kvstore::KVIterator>>
MetaCache::iterByPrefix(const std::string& prefix) const {
  std::unique_ptr<kvstore::KVIterator> iter;
  auto res = store_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
  if (res != nebula::cpp2::ErrorCode::SUCCEEDED) {
    VLOG(2) << "Failed to seek the prefix";
    return res;
  }
  return iter;
}


void MetaCache::loadGraphSpaces() {
  const auto& prefix = MetaKeyUtils::spacePrefix();
  auto res = iterByPrefix(prefix);
  if (!nebula::ok(res)) {
    LOG(ERROR) << "Failed to load spaces: "
               << apache::thrift::util::enumNameSafe(nebula::error(res));
    return;
  }
  auto iter = nebula::value(res).get();

  // Iterating through all spaces
  while (iter->valid()) {
    auto spaceId = MetaKeyUtils::spaceId(iter->key());
    auto spaceName = MetaKeyUtils::spaceName(iter->val());
    VLOG(2) << "Loaded space (id=" << spaceId << ", name=\"" << spaceName << "\")";
    spaceNameIdMap_.emplace(std::make_pair(spaceName, spaceId));
    metaInfo_->spaces_ref()->emplace(std::make_pair(spaceId, spaceName));
    iter->next();
  }
  VLOG(2) << "Loaded " << metaInfo_->get_spaces().size() << " graph spaces";
}


void MetaCache::loadTagSchemas() {
  for (const auto& space : metaInfo_->get_spaces()) {
    auto prefix = MetaKeyUtils::schemaTagsPrefix(space.first);
    auto res = iterByPrefix(prefix);
    if (!nebula::ok(res)) {
      LOG(ERROR) << "Failed to load tag schemas from space \"" << space.second << "\": "
                 << apache::thrift::util::enumNameSafe(nebula::error(res));
      continue;
    }

    auto& tags = metaInfo_->tags_ref()[space.first];
    auto iter = nebula::value(res).get();
    while (iter->valid()) {
      auto key = iter->key();
      auto val = iter->val();
      auto tagId = *reinterpret_cast<const TagID *>(key.data() + prefix.size());
      auto version = MetaKeyUtils::parseTagVersion(key);
      auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
      auto tagName = val.subpiece(sizeof(int32_t), nameLen).str();
      auto schema = MetaKeyUtils::parseSchema(val);
      VLOG(2) << "Loaded Tag Schema \"" << tagName << "\"";

      cpp2::TagItem item;
      item.tag_id_ref() = tagId;
      item.tag_name_ref() = std::move(tagName);
      item.version_ref() = version;
      item.schema_ref() = std::move(schema);
      tags.emplace(std::make_pair(tagId, std::move(item)));
      iter->next();
    }

    VLOG(2) << "Loaded " << tags.size()
            << " tag schemas from graph space \"" << space.second << "\"";
  }
}


void MetaCache::loadEdgeSchemas() {
  for (const auto& space : metaInfo_->get_spaces()) {
    auto prefix = MetaKeyUtils::schemaEdgesPrefix(space.first);
    auto res = iterByPrefix(prefix);
    if (!nebula::ok(res)) {
      LOG(ERROR) << "Failed to load edge schemas from space \"" << space.second << " \": "
                 << apache::thrift::util::enumNameSafe(nebula::error(res));
      continue;
    }

    auto iter = nebula::value(res).get();
    auto& edges = metaInfo_->edges_ref()[space.first];
    while (iter->valid()) {
      auto key = iter->key();
      auto val = iter->val();
      auto edgeType = *reinterpret_cast<const EdgeType *>(key.data() + prefix.size());
      auto version = MetaKeyUtils::parseEdgeVersion(key);
      auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
      auto edgeName = val.subpiece(sizeof(int32_t), nameLen).str();
      auto schema = MetaKeyUtils::parseSchema(val);
      VLOG(2) << "Loaded Edge Schema \"" << edgeName << "\"";

      cpp2::EdgeItem edge;
      edge.edge_type_ref() = edgeType;
      edge.edge_name_ref() = std::move(edgeName);
      edge.version_ref() = version;
      edge.schema_ref() = std::move(schema);
      edges.emplace(std::make_pair(edgeType, std::move(edge)));
      iter->next();
    }

    VLOG(2) << "Loaded " << edges.size()
            << " edge schemas from graph space \"" << space.second << "\"";
  }
}


void MetaCache::loadTagIndices() {
  for (const auto& space : metaInfo_->get_spaces()) {
    const auto& prefix = MetaKeyUtils::indexPrefix(space.first);
    auto res = iterByPrefix(prefix);
    if (!nebula::ok(res)) {
      LOG(ERROR) << "Failed to load tag Indices from space \"" << space.second << "\": "
                 << apache::thrift::util::enumNameSafe(nebula::error(res));
      continue;
    }

    auto iter = nebula::value(res).get();
    auto& indices = metaInfo_->tag_indices_ref()[space.first];
    auto& nameIdMap = indexNameIdMap_[space.first];
    while (iter->valid()) {
      auto idx = MetaKeyUtils::parseIndex(iter->val());
      if (idx.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::tag_id) {
        VLOG(2) << "Loaded tag index \"" << idx.get_index_name() << "\"";
        nameIdMap.emplace(std::make_pair(idx.get_index_name(), idx.get_index_id()));
        indices.emplace(std::make_pair(idx.get_index_id(), std::move(idx)));
      }
      iter->next();
    }

    VLOG(2) << "Loaded " << indices.size()
            << " tag indices from graph space \"" << space.second << "\"";
  }
}


void MetaCache::loadEdgeIndices() {
  for (const auto& space : metaInfo_->get_spaces()) {
    const auto& prefix = MetaKeyUtils::indexPrefix(space.first);
    auto res = iterByPrefix(prefix);
    if (!nebula::ok(res)) {
      LOG(ERROR) << "Failed to load edge indices from space \"" << space.second << "\": "
                 << apache::thrift::util::enumNameSafe(nebula::error(res));
      continue;
    }

    auto iter = nebula::value(res).get();
    auto& indices = metaInfo_->edge_indices_ref()[space.first];
    auto& nameIdMap = indexNameIdMap_[space.first];
    while (iter->valid()) {
      auto idx = MetaKeyUtils::parseIndex(iter->val());
      if (idx.get_schema_id().getType() == nebula::cpp2::SchemaID::Type::edge_type) {
        VLOG(2) << "Loaded edge index \"" << idx.get_index_name() << "\"";
        nameIdMap.emplace(std::make_pair(idx.get_index_name(), idx.get_index_id()));
        indices.emplace(std::make_pair(idx.get_index_id(), std::move(idx)));
      }
      iter->next();
    }

    VLOG(2) << "Loaded " << indices.size()
            << " edge indices from graph space \"" << space.second << "\"";
  }
}


void MetaCache::loadFullTextIndices() {
  const auto& prefix = MetaKeyUtils::fulltextIndexPrefix();
  auto res = iterByPrefix(prefix);
  if (!nebula::ok(res)) {
    LOG(ERROR) << "Failed to load full-text indices: "
               << apache::thrift::util::enumNameSafe(nebula::error(res));
    return;
  }

  auto iter = nebula::value(res).get();
  auto& indices = *(metaInfo_->fulltext_indices_ref());
  while (iter->valid()) {
    auto name = MetaKeyUtils::parsefulltextIndexName(iter->key());
    auto idx = MetaKeyUtils::parsefulltextIndex(iter->val());
    VLOG(2) << "Loaded full-text index \"" << name << "\"";
    indices.emplace(std::make_pair(std::move(name), std::move(idx)));
    iter->next();
  }

  VLOG(2) << "Loaded " << indices.size() << " full-text indices";
}


void MetaCache::loadFullTextClients() {
  const auto& prefix = MetaKeyUtils::fulltextServiceKey();
  auto res = iterByPrefix(prefix);
  if (!nebula::ok(res)) {
    LOG(ERROR) << "Failed to load the full-text index client configuration: "
               << apache::thrift::util::enumNameSafe(nebula::error(res));
    return;
  }

  auto iter = nebula::value(res).get();
  auto& clients = *(metaInfo_->fulltext_index_clients_ref());
  if (iter->valid()) {
    clients = MetaKeyUtils::parseFTClients(iter->val());
  }

  VLOG(2) << "Loaded " << clients.size() << " full-text index client configuration";
}


void MetaCache::loadUsers() {
  std::string prefix = "__users__";
  auto res = iterByPrefix(prefix);
  if (!nebula::ok(res)) {
    LOG(ERROR) << "Failed to load users: "
               << apache::thrift::util::enumNameSafe(nebula::error(res));
    return;
  }

  auto iter = nebula::value(res).get();
  auto& users = *(metaInfo_->users_ref());
  while (iter->valid()) {
    auto uId = MetaKeyUtils::parseUser(iter->key());
    VLOG(2) << "Loaded user \"" << uId << "\"";
    users.emplace(std::move(uId));
    iter->next();
  }

  VLOG(2) << "Loaded " << users.size() << " users";
}


void MetaCache::loadUserRoles() {
  for (const auto& space : metaInfo_->get_spaces()) {
    auto prefix = MetaKeyUtils::roleSpacePrefix(space.first);
    auto res = iterByPrefix(prefix);
    if (!nebula::ok(res)) {
      LOG(ERROR) << "Failed to load roles from space \"" << space.second << "\": "
                 << apache::thrift::util::enumNameSafe(nebula::error(res));
      continue;
    }

    auto iter = nebula::value(res).get();
    auto& roles = metaInfo_->user_roles_ref()[space.first];
    while (iter->valid()) {
      auto uId = MetaKeyUtils::parseRoleUser(iter->key());
      auto val = iter->val();
      VLOG(2) << "Loaded a role for user \"" << uId << "\"";

      auto role = *reinterpret_cast<const cpp2::RoleType*>(val.begin());
      roles.emplace(std::make_pair(uId, role));
      iter->next();
    }

    VLOG(2) << "Loaded " << roles.size()
            << " user roles from graph space \"" << space.second << "\"";
  }
}


void MetaCache::loadPartAlloc() {
  for (const auto& space : metaInfo_->get_spaces()) {
    const auto& prefix = MetaKeyUtils::partPrefix(space.first);
    auto res = iterByPrefix(prefix);
    if (!nebula::ok(res)) {
      LOG(ERROR) << "Failed to load partitions from space \"" << space.second << "\": "
                 << apache::thrift::util::enumNameSafe(nebula::error(res));
      continue;
    }

    auto iter = nebula::value(res).get();
    auto& partAlloc = metaInfo_->parts_ref()[space.first];
    while (iter->valid()) {
      auto key = iter->key();
      PartitionID partId;
      memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
      std::vector<HostAddr> hosts = MetaKeyUtils::parsePartVal(iter->val());
      partAlloc.emplace(std::make_pair(partId, std::move(hosts)));
      iter->next();
    }

    VLOG(2) << "Loaded " << partAlloc.size()
            << " partitions from graph space \"" << space.second << "\"";
  }
}


void MetaCache::loadConfig() {
  static const std::vector<cpp2::ConfigModule> modules = {
    cpp2::ConfigModule::UNKNOWN,
    cpp2::ConfigModule::ALL,
    cpp2::ConfigModule::GRAPH,
    cpp2::ConfigModule::META,
    cpp2::ConfigModule::STORAGE
  };

  for (auto mod : modules) {
    const auto& prefix = MetaKeyUtils::configKeyPrefix(mod);
    auto res = iterByPrefix(prefix);
    if (!nebula::ok(res)) {
      LOG(ERROR) << "Failed to load configurations for module "
                 << apache::thrift::util::enumNameSafe(mod) << ": "
                 << apache::thrift::util::enumNameSafe(nebula::error(res));
      continue;
    }

    auto iter = nebula::value(res).get();
    auto& configSet = metaInfo_->config_ref()[mod];
    while (iter->valid()) {
      auto key = iter->key();
      auto value = iter->val();
      auto conf = MetaKeyUtils::parseConfigValue(value);
      auto configName = MetaKeyUtils::parseConfigKey(key);
      VLOG(2) << "Loaded a configuration \"" << configName.second << "\"";

      conf.module_ref() = configName.first;
      conf.name_ref() = configName.second;
      configSet.emplace(std::make_pair(conf.get_name(), std::move(conf)));
      iter->next();
    }

    VLOG(2) << "Loaded " << configSet.size() << " configuration for module "
            << apache::thrift::util::enumNameSafe(mod);
  }
}


void MetaCache::loadListeners() {
  for (const auto& space : metaInfo_->get_spaces()) {
    const auto& prefix = MetaKeyUtils::listenerPrefix(space.first);
    auto res = iterByPrefix(prefix);
    if (!nebula::ok(res)) {
      LOG(ERROR) << "Failed to load listeners from space \"" << space.second << "\": "
                 << apache::thrift::util::enumNameSafe(nebula::error(res));
      continue;
    }

    auto iter = nebula::value(res).get();
    auto& listeners = metaInfo_->listeners_ref()[space.first];
    while (iter->valid()) {
      cpp2::ListenerInfo listener;
      listener.type_ref() = MetaKeyUtils::parseListenerType(iter->key());
      listener.host_ref() = MetaKeyUtils::deserializeHostAddr(iter->val());
      listener.part_id_ref() = MetaKeyUtils::parseListenerPart(iter->key());
      listeners.emplace_back(std::move(listener));
      iter->next();
    }

    VLOG(2) << "Loaded " << listeners.size()
            << " listeners from graph space \"" << space.second << "\"";
  }
}


void MetaCache::loadLeaderStatus() {
  const auto& prefix = MetaKeyUtils::leaderPrefix();
  auto res = iterByPrefix(prefix);
  if (!nebula::ok(res)) {
    LOG(ERROR) << "Failed to load leader information: "
               << apache::thrift::util::enumNameSafe(nebula::error(res));
    return;
  }

  auto iter = nebula::value(res).get();
  std::vector<std::string> leaderKeysToRemove;
  int32_t count = 0;
  for (; iter->valid(); iter->next()) {
    auto spaceIdAndPartId = MetaKeyUtils::parseLeaderKeyV3(iter->key());
    auto spaceId = spaceIdAndPartId.first;
    auto partId = spaceIdAndPartId.second;
    VLOG(2) << "Found leader information (spaceId = " << spaceId
            << ", partId = " << partId << ")";

    // If the space doesn't exist, remove leader key
    auto& spaces = metaInfo_->get_spaces();
    if (spaces.find(spaceId) == spaces.end()) {
      // Space does not exist any more
      leaderKeysToRemove.emplace_back(iter->key());
      continue;
    }

    HostAddr host;
    TermID term;
    nebula::cpp2::ErrorCode code;
    std::tie(host, term, code) = MetaKeyUtils::parseLeaderValV3(iter->val());
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
      LOG(ERROR) << "Failed to parse the value of the leader information (spaceId = "
                 << spaceIdAndPartId.first << ", partId = " << spaceIdAndPartId.second << ")";
      continue;
    }

    cpp2::LeaderAndTerm lt;
    lt.leader_ref() = std::move(host);
    lt.term_ref() = term;
    statusInfo_->leader_status_ref()[spaceId].emplace(std::make_pair(partId, std::move(lt)));
    count++;
  }
  VLOG(2) << "Found " << count << " valid leaders";

  if (!leaderKeysToRemove.empty()) {
    store_->asyncMultiRemove(
        kDefaultSpaceId,
        kDefaultPartId,
        std::move(leaderKeysToRemove),
        [](nebula::cpp2::ErrorCode code) {
          if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
             LOG(ERROR) << "Failed to remove the invalid leader information: "
                        << apache::thrift::util::enumNameSafe(code);
          }
        });
  }
}


void MetaCache::loadHostInfo() {
  const auto& prefix = MetaKeyUtils::hostPrefix();
  auto res = iterByPrefix(prefix);
  if (!nebula::ok(res)) {
    LOG(ERROR) << "Failed to load host infomation: "
               << apache::thrift::util::enumNameSafe(nebula::error(res));
    return;
  }

  auto iter = nebula::value(res).get();
  while (iter->valid()) {
    auto addr = MetaKeyUtils::parseHostKey(iter->key());
    HostInfo info = HostInfo::decode(iter->val());
    VLOG(2) << "Loaded a host \"" << addr << "\"";
    hostInfo_->emplace(std::make_pair(std::move(addr), std::move(info)));
    iter->next();
  }

  VLOG(2) << "Loaded " << hostInfo_->size() << " hosts";
}


void MetaCache::loadMetaData() {
  // Aquire write lock
  std::unique_lock lock(mutex_);
  CHECK_NOTNULL(store_);

  // Initialize the keyVersion_ and revision_ from the storage
  keyVersion_ = MetaVersionMan::getMetaVersionFromKV(store_);
  auto rev = LastUpdateTimeMan::get(store_);
  if (nebula::ok(rev)) {
    revision_ = nebula::value(rev);
  } else {
    LOG(WARNING) << "Could not load th elast meta revision from the storage";
    // We start from current time
    revision_ = time::WallClock::fastNowInMilliSec();
  }

  metaInfo_ = std::make_shared<cpp2::MetaInfo>();
  statusInfo_ = std::make_shared<cpp2::StatusInfo>();
  hostInfo_ = std::make_shared<std::unordered_map<HostAddr, HostInfo>>();

  loadGraphSpaces();
  loadTagSchemas();
  loadEdgeSchemas();
  loadTagIndices();
  loadEdgeIndices();
  loadFullTextIndices();
  loadFullTextClients();
  loadUsers();
  loadUserRoles();
  loadPartAlloc();
  loadConfig();
  loadListeners();

  loadLeaderStatus();

  loadHostInfo();
}


ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> MetaCache::toSpaceIdInternal(
    const std::string& name) {
  auto it = spaceNameIdMap_.find(name);
  if (it == spaceNameIdMap_.end()) {
    return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
  }

  return it->second;
}


ErrorOr<nebula::cpp2::ErrorCode, GraphSpaceID> MetaCache::toSpaceId(const std::string& name) {
  // Aquire read lock
  std::shared_lock lock(mutex_);
  return toSpaceIdInternal(name);
}


void MetaCache::addPublisher(
    ::apache::thrift::ServerStreamPublisher<cpp2::MetaData>&& publisher,
    folly::EventBase* evb,
    int64_t lastRevision) {
  // Aquire write lock
  std::unique_lock lock(mutex_);

  auto pub = pubMan_.addPublisher(std::move(publisher), evb);

  // Send the meta data if the last revision is old
  if (lastRevision < revision_) {
    cpp2::MetaData data;
    data.key_version_ref() = static_cast<int32_t>(keyVersion_);
    data.meta_revision_ref() = revision_;
    data.meta_info_ref() = metaInfo_;
    data.status_info_ref() = statusInfo_;

    pubMan_.publishToOne(pub, std::move(data));
  }
}

}  // namespace meta
}  // namespace nebula
