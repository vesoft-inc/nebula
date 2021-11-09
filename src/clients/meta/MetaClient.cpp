/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/meta/MetaClient.h"

#include <folly/ScopeGuard.h>
#include <folly/executors/Async.h>
#include <folly/futures/Future.h>
#include <folly/hash/Hash.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "clients/meta/FileBasedClusterIdMan.h"
#include "common/base/Base.h"
#include "common/base/MurmurHash2.h"
#include "common/conf/Configuration.h"
#include "common/http/HttpClient.h"
#include "common/meta/NebulaSchemaProvider.h"
#include "common/network/NetworkUtils.h"
#include "common/ssl/SSLConfig.h"
#include "common/stats/StatsManager.h"
#include "common/time/TimeUtils.h"
#include "version/Version.h"
#include "webservice/Common.h"

DECLARE_int32(ws_meta_http_port);
DECLARE_int32(ws_meta_h2_port);

DEFINE_uint32(expired_time_factor, 5, "The factor of expired time based on heart beat interval");
DEFINE_int32(heartbeat_interval_secs, 10, "Heartbeat interval in seconds");
DEFINE_int32(meta_client_retry_times, 3, "meta client retry times, 0 means no retry");
DEFINE_int32(meta_client_retry_interval_secs, 1, "meta client sleep interval between retry");
DEFINE_int32(meta_client_timeout_ms, 60 * 1000, "meta client timeout");
DEFINE_string(cluster_id_path, "cluster.id", "file path saved clusterId");
DEFINE_int32(check_plan_killed_frequency, 8, "check plan killed every 1<<n times");

namespace nebula {
namespace meta {

MetaClient::MetaClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                       std::vector<HostAddr> addrs,
                       const MetaClientOptions& options)
    : ioThreadPool_(ioThreadPool),
      addrs_(std::move(addrs)),
      options_(options),
      sessionMap_(new SessionMap{}),
      killedPlans_(new folly::F14FastSet<std::pair<SessionID, ExecutionPlanID>>{}) {
  CHECK(ioThreadPool_ != nullptr) << "IOThreadPool is required";
  CHECK(!addrs_.empty())
      << "No meta server address is specified or can be solved. Meta server is required";
  clientsMan_ = std::make_shared<thrift::ThriftClientManager<cpp2::MetaServiceAsyncClient>>(
      FLAGS_enable_ssl || FLAGS_enable_meta_ssl);
  updateActive();
  updateLeader();
  bgThread_ = std::make_unique<thread::GenericWorker>();
  LOG(INFO) << "Create meta client to " << active_;
}

MetaClient::~MetaClient() {
  stop();
  delete sessionMap_.load();
  delete killedPlans_.load();
  VLOG(3) << "~MetaClient";
}

bool MetaClient::isMetadReady() {
  auto ret = heartbeat().get();
  if (!ret.ok()) {
    LOG(ERROR) << "Heartbeat failed, status:" << ret.status();
    return ready_;
  } else if (options_.role_ == cpp2::HostRole::STORAGE &&
             metaServerVersion_ != EXPECT_META_VERSION) {
    LOG(ERROR) << "Expect meta version is " << EXPECT_META_VERSION << ", but actual is "
               << metaServerVersion_;
    return ready_;
  }

  // ready_ will be set in loadData
  bool ldRet = loadData();
  bool lcRet = true;
  if (!options_.skipConfig_) {
    lcRet = loadCfg();
  }
  if (ldRet && lcRet) {
    localLastUpdateTime_ = metadLastUpdateTime_;
  }
  return ready_;
}

bool MetaClient::waitForMetadReady(int count, int retryIntervalSecs) {
  auto status = verifyVersion();
  if (!status.ok()) {
    LOG(ERROR) << status;
    return false;
  }

  if (!options_.skipConfig_) {
    std::string gflagsJsonPath;
    GflagsManager::getGflagsModule(gflagsModule_);
    gflagsDeclared_ = GflagsManager::declareGflags(gflagsModule_);
  }
  isRunning_ = true;
  int tryCount = count;
  while (!isMetadReady() && ((count == -1) || (tryCount > 0)) && isRunning_) {
    LOG(INFO) << "Waiting for the metad to be ready!";
    --tryCount;
    ::sleep(retryIntervalSecs);
  }  // end while

  if (!isRunning_) {
    LOG(ERROR) << "Connect to the MetaServer Failed";
    return false;
  }

  CHECK(bgThread_->start());
  LOG(INFO) << "Register time task for heartbeat!";
  size_t delayMS = FLAGS_heartbeat_interval_secs * 1000 + folly::Random::rand32(900);
  bgThread_->addDelayTask(delayMS, &MetaClient::heartBeatThreadFunc, this);
  return ready_;
}

void MetaClient::stop() {
  if (bgThread_ != nullptr) {
    bgThread_->stop();
    bgThread_->wait();
    bgThread_.reset();
  }
  isRunning_ = false;
}

void MetaClient::heartBeatThreadFunc() {
  SCOPE_EXIT {
    bgThread_->addDelayTask(
        FLAGS_heartbeat_interval_secs * 1000, &MetaClient::heartBeatThreadFunc, this);
  };
  auto ret = heartbeat().get();
  if (!ret.ok()) {
    LOG(ERROR) << "Heartbeat failed, status:" << ret.status();
    return;
  }

  // if MetaServer has some changes, refesh the localCache_
  if (localLastUpdateTime_ < metadLastUpdateTime_) {
    bool ldRet = loadData();
    bool lcRet = true;
    if (!options_.skipConfig_) {
      lcRet = loadCfg();
    }
    if (ldRet && lcRet) {
      localLastUpdateTime_ = metadLastUpdateTime_;
    }
  }
}

bool MetaClient::loadUsersAndRoles() {
  auto userRoleRet = listUsers().get();
  if (!userRoleRet.ok()) {
    LOG(ERROR) << "List users failed, status:" << userRoleRet.status();
    return false;
  }
  decltype(userRolesMap_) userRolesMap;
  decltype(userPasswordMap_) userPasswordMap;
  for (auto& user : userRoleRet.value()) {
    auto rolesRet = getUserRoles(user.first).get();
    if (!rolesRet.ok()) {
      LOG(ERROR) << "List role by user failed, user : " << user.first;
      return false;
    }
    userRolesMap[user.first] = rolesRet.value();
    userPasswordMap[user.first] = user.second.get_encoded_pwd();
  }
  {
    folly::RWSpinLock::WriteHolder holder(localCacheLock_);
    userRolesMap_ = std::move(userRolesMap);
    userPasswordMap_ = std::move(userPasswordMap);
  }
  return true;
}

bool MetaClient::loadData() {
  if (ioThreadPool_->numThreads() <= 0) {
    LOG(ERROR) << "The threads number in ioThreadPool should be greater than 0";
    return false;
  }

  if (!loadUsersAndRoles()) {
    LOG(ERROR) << "Load roles Failed";
    return false;
  }

  if (!loadFulltextClients()) {
    LOG(ERROR) << "Load fulltext services Failed";
    return false;
  }

  if (!loadFulltextIndexes()) {
    LOG(ERROR) << "Load fulltext indexes Failed";
    return false;
  }

  if (!loadSessions()) {
    LOG(ERROR) << "Load sessions Failed";
    return false;
  }

  auto ret = listSpaces().get();
  if (!ret.ok()) {
    LOG(ERROR) << "List space failed, status:" << ret.status();
    return false;
  }

  decltype(localCache_) cache;
  decltype(spaceIndexByName_) spaceIndexByName;
  decltype(spaceTagIndexByName_) spaceTagIndexByName;
  decltype(spaceEdgeIndexByName_) spaceEdgeIndexByName;
  decltype(spaceNewestTagVerMap_) spaceNewestTagVerMap;
  decltype(spaceNewestEdgeVerMap_) spaceNewestEdgeVerMap;
  decltype(spaceEdgeIndexByType_) spaceEdgeIndexByType;
  decltype(spaceTagIndexById_) spaceTagIndexById;
  decltype(spaceAllEdgeMap_) spaceAllEdgeMap;

  for (auto space : ret.value()) {
    auto spaceId = space.first;
    MetaClient::PartTerms partTerms;
    auto r = getPartsAlloc(spaceId, &partTerms).get();
    if (!r.ok()) {
      LOG(ERROR) << "Get parts allocation failed for spaceId " << spaceId << ", status "
                 << r.status();
      return false;
    }

    auto spaceCache = std::make_shared<SpaceInfoCache>();
    auto partsAlloc = r.value();
    auto& spaceName = space.second;
    spaceCache->partsOnHost_ = reverse(partsAlloc);
    spaceCache->partsAlloc_ = std::move(partsAlloc);
    spaceCache->termOfPartition_ = std::move(partTerms);
    VLOG(2) << "Load space " << spaceId << ", parts num:" << spaceCache->partsAlloc_.size();

    // loadSchemas
    if (!loadSchemas(spaceId,
                     spaceCache,
                     spaceTagIndexByName,
                     spaceTagIndexById,
                     spaceEdgeIndexByName,
                     spaceEdgeIndexByType,
                     spaceNewestTagVerMap,
                     spaceNewestEdgeVerMap,
                     spaceAllEdgeMap)) {
      LOG(ERROR) << "Load Schemas Failed";
      return false;
    }

    if (!loadIndexes(spaceId, spaceCache)) {
      LOG(ERROR) << "Load Indexes Failed";
      return false;
    }

    if (!loadListeners(spaceId, spaceCache)) {
      LOG(ERROR) << "Load Listeners Failed";
      return false;
    }

    // get space properties
    auto resp = getSpace(spaceName).get();
    if (!resp.ok()) {
      LOG(ERROR) << "Get space properties failed for space " << spaceId;
      return false;
    }
    auto properties = resp.value().get_properties();
    spaceCache->spaceDesc_ = std::move(properties);

    cache.emplace(spaceId, spaceCache);
    spaceIndexByName.emplace(space.second, spaceId);
  }

  auto hostsRet = listHosts().get();
  if (!ret.ok()) {
    LOG(ERROR) << "List hosts failed, status:" << hostsRet.status();
    return false;
  }

  auto& hostItems = hostsRet.value();
  std::vector<HostAddr> hosts(hostItems.size());
  std::transform(hostItems.begin(), hostItems.end(), hosts.begin(), [](auto& hostItem) -> HostAddr {
    return *hostItem.hostAddr_ref();
  });

  loadLeader(hostItems, spaceIndexByName_);

  decltype(localCache_) oldCache;
  {
    folly::RWSpinLock::WriteHolder holder(localCacheLock_);
    oldCache = std::move(localCache_);
    localCache_ = std::move(cache);
    spaceIndexByName_ = std::move(spaceIndexByName);
    spaceTagIndexByName_ = std::move(spaceTagIndexByName);
    spaceEdgeIndexByName_ = std::move(spaceEdgeIndexByName);
    spaceNewestTagVerMap_ = std::move(spaceNewestTagVerMap);
    spaceNewestEdgeVerMap_ = std::move(spaceNewestEdgeVerMap);
    spaceEdgeIndexByType_ = std::move(spaceEdgeIndexByType);
    spaceTagIndexById_ = std::move(spaceTagIndexById);
    spaceAllEdgeMap_ = std::move(spaceAllEdgeMap);
    storageHosts_ = std::move(hosts);
  }

  diff(oldCache, localCache_);
  listenerDiff(oldCache, localCache_);
  loadRemoteListeners();
  ready_ = true;
  return true;
}

bool MetaClient::loadSchemas(GraphSpaceID spaceId,
                             std::shared_ptr<SpaceInfoCache> spaceInfoCache,
                             SpaceTagNameIdMap& tagNameIdMap,
                             SpaceTagIdNameMap& tagIdNameMap,
                             SpaceEdgeNameTypeMap& edgeNameTypeMap,
                             SpaceEdgeTypeNameMap& edgeTypeNameMap,
                             SpaceNewestTagVerMap& newestTagVerMap,
                             SpaceNewestEdgeVerMap& newestEdgeVerMap,
                             SpaceAllEdgeMap& allEdgeMap) {
  auto tagRet = listTagSchemas(spaceId).get();
  if (!tagRet.ok()) {
    LOG(ERROR) << "Get tag schemas failed for spaceId " << spaceId << ", " << tagRet.status();
    return false;
  }

  auto edgeRet = listEdgeSchemas(spaceId).get();
  if (!edgeRet.ok()) {
    LOG(ERROR) << "Get edge schemas failed for spaceId " << spaceId << ", " << edgeRet.status();
    return false;
  }

  auto tagItemVec = tagRet.value();
  auto edgeItemVec = edgeRet.value();
  allEdgeMap[spaceId] = {};
  TagSchemas tagSchemas;
  EdgeSchemas edgeSchemas;
  TagID lastTagId = -1;

  auto addSchemaField = [&spaceInfoCache](NebulaSchemaProvider* schema,
                                          const cpp2::ColumnDef& col) {
    bool hasDef = col.default_value_ref().has_value();
    auto& colType = col.get_type();
    size_t len = colType.type_length_ref().has_value() ? *colType.get_type_length() : 0;
    cpp2::GeoShape geoShape =
        colType.geo_shape_ref().has_value() ? *colType.get_geo_shape() : cpp2::GeoShape::ANY;
    bool nullable = col.nullable_ref().has_value() ? *col.get_nullable() : false;
    Expression* defaultValueExpr = nullptr;
    if (hasDef) {
      auto encoded = *col.get_default_value();
      defaultValueExpr = Expression::decode(&(spaceInfoCache->pool_),
                                            folly::StringPiece(encoded.data(), encoded.size()));

      if (defaultValueExpr == nullptr) {
        LOG(ERROR) << "Wrong expr default value for column name: " << col.get_name();
        hasDef = false;
      }
    }

    schema->addField(col.get_name(),
                     colType.get_type(),
                     len,
                     nullable,
                     hasDef ? defaultValueExpr : nullptr,
                     geoShape);
  };

  for (auto& tagIt : tagItemVec) {
    // meta will return the different version from new to old
    auto schema = std::make_shared<NebulaSchemaProvider>(tagIt.get_version());
    for (const auto& colIt : tagIt.get_schema().get_columns()) {
      addSchemaField(schema.get(), colIt);
    }
    // handle schema property
    schema->setProp(tagIt.get_schema().get_schema_prop());
    if (tagIt.get_tag_id() != lastTagId) {
      // init schema vector, since schema version is zero-based, need to add one
      tagSchemas[tagIt.get_tag_id()].resize(schema->getVersion() + 1);
      lastTagId = tagIt.get_tag_id();
    }
    tagSchemas[tagIt.get_tag_id()][schema->getVersion()] = std::move(schema);
    tagNameIdMap.emplace(std::make_pair(spaceId, tagIt.get_tag_name()), tagIt.get_tag_id());
    tagIdNameMap.emplace(std::make_pair(spaceId, tagIt.get_tag_id()), tagIt.get_tag_name());
    // get the latest tag version
    auto it = newestTagVerMap.find(std::make_pair(spaceId, tagIt.get_tag_id()));
    if (it != newestTagVerMap.end()) {
      if (it->second < tagIt.get_version()) {
        it->second = tagIt.get_version();
      }
    } else {
      newestTagVerMap.emplace(std::make_pair(spaceId, tagIt.get_tag_id()), tagIt.get_version());
    }
    VLOG(3) << "Load Tag Schema Space " << spaceId << ", ID " << tagIt.get_tag_id() << ", Name "
            << tagIt.get_tag_name() << ", Version " << tagIt.get_version() << " Successfully!";
  }

  std::unordered_set<std::pair<GraphSpaceID, EdgeType>> edges;
  EdgeType lastEdgeType = -1;
  for (auto& edgeIt : edgeItemVec) {
    // meta will return the different version from new to old
    auto schema = std::make_shared<NebulaSchemaProvider>(edgeIt.get_version());
    for (const auto& col : edgeIt.get_schema().get_columns()) {
      addSchemaField(schema.get(), col);
    }
    // handle shcem property
    schema->setProp(edgeIt.get_schema().get_schema_prop());
    if (edgeIt.get_edge_type() != lastEdgeType) {
      // init schema vector, since schema version is zero-based, need to add one
      edgeSchemas[edgeIt.get_edge_type()].resize(schema->getVersion() + 1);
      lastEdgeType = edgeIt.get_edge_type();
    }
    edgeSchemas[edgeIt.get_edge_type()][schema->getVersion()] = std::move(schema);
    edgeNameTypeMap.emplace(std::make_pair(spaceId, edgeIt.get_edge_name()),
                            edgeIt.get_edge_type());
    edgeTypeNameMap.emplace(std::make_pair(spaceId, edgeIt.get_edge_type()),
                            edgeIt.get_edge_name());
    if (edges.find({spaceId, edgeIt.get_edge_type()}) != edges.cend()) {
      continue;
    }
    edges.emplace(spaceId, edgeIt.get_edge_type());
    allEdgeMap[spaceId].emplace_back(edgeIt.get_edge_name());
    // get the latest edge version
    auto it2 = newestEdgeVerMap.find(std::make_pair(spaceId, edgeIt.get_edge_type()));
    if (it2 != newestEdgeVerMap.end()) {
      if (it2->second < edgeIt.get_version()) {
        it2->second = edgeIt.get_version();
      }
    } else {
      newestEdgeVerMap.emplace(std::make_pair(spaceId, edgeIt.get_edge_type()),
                               edgeIt.get_version());
    }
    VLOG(3) << "Load Edge Schema Space " << spaceId << ", Type " << edgeIt.get_edge_type()
            << ", Name " << edgeIt.get_edge_name() << ", Version " << edgeIt.get_version()
            << " Successfully!";
  }

  spaceInfoCache->tagSchemas_ = std::move(tagSchemas);
  spaceInfoCache->edgeSchemas_ = std::move(edgeSchemas);
  return true;
}

bool MetaClient::loadIndexes(GraphSpaceID spaceId, std::shared_ptr<SpaceInfoCache> cache) {
  auto tagIndexesRet = listTagIndexes(spaceId).get();
  if (!tagIndexesRet.ok()) {
    LOG(ERROR) << "Get tag indexes failed for spaceId " << spaceId << ", "
               << tagIndexesRet.status();
    return false;
  }

  auto edgeIndexesRet = listEdgeIndexes(spaceId).get();
  if (!edgeIndexesRet.ok()) {
    LOG(ERROR) << "Get edge indexes failed for spaceId " << spaceId << ", "
               << edgeIndexesRet.status();
    return false;
  }

  Indexes tagIndexes;
  for (auto tagIndex : tagIndexesRet.value()) {
    auto indexName = tagIndex.get_index_name();
    auto indexID = tagIndex.get_index_id();
    std::pair<GraphSpaceID, std::string> pair(spaceId, indexName);
    tagNameIndexMap_[pair] = indexID;
    auto tagIndexPtr = std::make_shared<cpp2::IndexItem>(tagIndex);
    tagIndexes.emplace(indexID, tagIndexPtr);
  }
  cache->tagIndexes_ = std::move(tagIndexes);

  Indexes edgeIndexes;
  for (auto& edgeIndex : edgeIndexesRet.value()) {
    auto indexName = edgeIndex.get_index_name();
    auto indexID = edgeIndex.get_index_id();
    std::pair<GraphSpaceID, std::string> pair(spaceId, indexName);
    edgeNameIndexMap_[pair] = indexID;
    auto edgeIndexPtr = std::make_shared<cpp2::IndexItem>(edgeIndex);
    edgeIndexes.emplace(indexID, edgeIndexPtr);
  }
  cache->edgeIndexes_ = std::move(edgeIndexes);
  return true;
}

bool MetaClient::loadListeners(GraphSpaceID spaceId, std::shared_ptr<SpaceInfoCache> cache) {
  auto listenerRet = listListener(spaceId).get();
  if (!listenerRet.ok()) {
    LOG(ERROR) << "Get listeners failed for spaceId " << spaceId << ", " << listenerRet.status();
    return false;
  }
  Listeners listeners;
  for (auto& listener : listenerRet.value()) {
    listeners[listener.get_host()].emplace_back(
        std::make_pair(listener.get_part_id(), listener.get_type()));
  }
  cache->listeners_ = std::move(listeners);
  return true;
}

bool MetaClient::loadFulltextClients() {
  auto ftRet = listFTClients().get();
  if (!ftRet.ok()) {
    LOG(ERROR) << "List fulltext services failed, status:" << ftRet.status();
    return false;
  }
  {
    folly::RWSpinLock::WriteHolder holder(localCacheLock_);
    fulltextClientList_ = std::move(ftRet).value();
  }
  return true;
}

bool MetaClient::loadFulltextIndexes() {
  auto ftRet = listFTIndexes().get();
  if (!ftRet.ok()) {
    LOG(ERROR) << "List fulltext indexes failed, status:" << ftRet.status();
    return false;
  }
  {
    folly::RWSpinLock::WriteHolder holder(localCacheLock_);
    fulltextIndexMap_ = std::move(ftRet).value();
  }
  return true;
}

Status MetaClient::checkTagIndexed(GraphSpaceID space, IndexID indexID) {
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = localCache_.find(space);
  if (it != localCache_.end()) {
    auto indexIt = it->second->tagIndexes_.find(indexID);
    if (indexIt != it->second->tagIndexes_.end()) {
      return Status::OK();
    } else {
      return Status::IndexNotFound();
    }
  }
  return Status::SpaceNotFound();
}

Status MetaClient::checkEdgeIndexed(GraphSpaceID space, IndexID indexID) {
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = localCache_.find(space);
  if (it != localCache_.end()) {
    auto indexIt = it->second->edgeIndexes_.find(indexID);
    if (indexIt != it->second->edgeIndexes_.end()) {
      return Status::OK();
    } else {
      return Status::IndexNotFound();
    }
  }
  return Status::SpaceNotFound();
}

std::unordered_map<HostAddr, std::vector<PartitionID>> MetaClient::reverse(
    const PartsAlloc& parts) {
  std::unordered_map<HostAddr, std::vector<PartitionID>> hosts;
  for (auto& partHost : parts) {
    for (auto& h : partHost.second) {
      hosts[h].emplace_back(partHost.first);
    }
  }
  return hosts;
}

template <typename Request,
          typename RemoteFunc,
          typename RespGenerator,
          typename RpcResponse,
          typename Response>
void MetaClient::getResponse(Request req,
                             RemoteFunc remoteFunc,
                             RespGenerator respGen,
                             folly::Promise<StatusOr<Response>> pro,
                             bool toLeader,
                             int32_t retry,
                             int32_t retryLimit) {
  auto* evb = ioThreadPool_->getEventBase();
  HostAddr host;
  {
    folly::RWSpinLock::ReadHolder holder(&hostLock_);
    host = toLeader ? leader_ : active_;
  }
  folly::via(
      evb,
      [host,
       evb,
       req = std::move(req),
       remoteFunc = std::move(remoteFunc),
       respGen = std::move(respGen),
       pro = std::move(pro),
       toLeader,
       retry,
       retryLimit,
       this]() mutable {
        auto client = clientsMan_->client(host, evb, false, FLAGS_meta_client_timeout_ms);
        VLOG(1) << "Send request to meta " << host;
        remoteFunc(client, req)
            .via(evb)
            .then([host,
                   req = std::move(req),
                   remoteFunc = std::move(remoteFunc),
                   respGen = std::move(respGen),
                   pro = std::move(pro),
                   toLeader,
                   retry,
                   retryLimit,
                   evb,
                   this](folly::Try<RpcResponse>&& t) mutable {
              // exception occurred during RPC
              if (t.hasException()) {
                if (toLeader) {
                  updateLeader();
                } else {
                  updateActive();
                }
                if (retry < retryLimit) {
                  evb->runAfterDelay(
                      [req = std::move(req),
                       remoteFunc = std::move(remoteFunc),
                       respGen = std::move(respGen),
                       pro = std::move(pro),
                       toLeader,
                       retry,
                       retryLimit,
                       this]() mutable {
                        getResponse(std::move(req),
                                    std::move(remoteFunc),
                                    std::move(respGen),
                                    std::move(pro),
                                    toLeader,
                                    retry + 1,
                                    retryLimit);
                      },
                      FLAGS_meta_client_retry_interval_secs * 1000);
                  return;
                } else {
                  LOG(ERROR) << "Send request to " << host << ", exceed retry limit";
                  pro.setValue(
                      Status::Error("RPC failure in MetaClient: %s", t.exception().what().c_str()));
                }
                return;
              }

              auto&& resp = t.value();
              if (resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED) {
                // succeeded
                pro.setValue(respGen(std::move(resp)));
                return;
              } else if (resp.get_code() == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
                updateLeader(resp.get_leader());
                if (retry < retryLimit) {
                  evb->runAfterDelay(
                      [req = std::move(req),
                       remoteFunc = std::move(remoteFunc),
                       respGen = std::move(respGen),
                       pro = std::move(pro),
                       toLeader,
                       retry,
                       retryLimit,
                       this]() mutable {
                        getResponse(std::move(req),
                                    std::move(remoteFunc),
                                    std::move(respGen),
                                    std::move(pro),
                                    toLeader,
                                    retry + 1,
                                    retryLimit);
                      },
                      FLAGS_meta_client_retry_interval_secs * 1000);
                  return;
                }
              } else if (resp.get_code() == nebula::cpp2::ErrorCode::E_CLIENT_SERVER_INCOMPATIBLE) {
                pro.setValue(respGen(std::move(resp)));
                return;
              }
              pro.setValue(this->handleResponse(resp));
            });  // then
      });        // via
}

std::vector<SpaceIdName> MetaClient::toSpaceIdName(const std::vector<cpp2::IdName>& tIdNames) {
  std::vector<SpaceIdName> idNames;
  idNames.resize(tIdNames.size());
  std::transform(tIdNames.begin(), tIdNames.end(), idNames.begin(), [](const auto& tin) {
    return SpaceIdName(tin.get_id().get_space_id(), tin.get_name());
  });
  return idNames;
}

template <typename RESP>
Status MetaClient::handleResponse(const RESP& resp) {
  switch (resp.get_code()) {
    case nebula::cpp2::ErrorCode::SUCCEEDED:
      return Status::OK();
    case nebula::cpp2::ErrorCode::E_DISCONNECTED:
      return Status::Error("Disconnected!");
    case nebula::cpp2::ErrorCode::E_FAIL_TO_CONNECT:
      return Status::Error("Fail to connect!");
    case nebula::cpp2::ErrorCode::E_RPC_FAILURE:
      return Status::Error("Rpc failure!");
    case nebula::cpp2::ErrorCode::E_LEADER_CHANGED:
      return Status::LeaderChanged("Leader changed!");
    case nebula::cpp2::ErrorCode::E_NO_HOSTS:
      return Status::Error("No hosts!");
    case nebula::cpp2::ErrorCode::E_EXISTED:
      return Status::Error("Existed!");
    case nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND:
      return Status::Error("Space not existed!");
    case nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND:
      return Status::Error("Tag not existed!");
    case nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND:
      return Status::Error("Edge not existed!");
    case nebula::cpp2::ErrorCode::E_INDEX_NOT_FOUND:
      return Status::Error("Index not existed!");
    case nebula::cpp2::ErrorCode::E_STATS_NOT_FOUND:
      return Status::Error(
          "There is no any stats info to show, please execute "
          "`submit job stats' firstly!");
    case nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND:
      return Status::Error("Edge prop not existed!");
    case nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND:
      return Status::Error("Tag prop not existed!");
    case nebula::cpp2::ErrorCode::E_ROLE_NOT_FOUND:
      return Status::Error("Role not existed!");
    case nebula::cpp2::ErrorCode::E_CONFIG_NOT_FOUND:
      return Status::Error("Conf not existed!");
    case nebula::cpp2::ErrorCode::E_PART_NOT_FOUND:
      return Status::Error("Part not existed!");
    case nebula::cpp2::ErrorCode::E_USER_NOT_FOUND:
      return Status::Error("User not existed!");
    case nebula::cpp2::ErrorCode::E_GROUP_NOT_FOUND:
      return Status::Error("Group not existed!");
    case nebula::cpp2::ErrorCode::E_ZONE_NOT_FOUND:
      return Status::Error("Zone not existed!");
    case nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND:
      return Status::Error("Key not existed!");
    case nebula::cpp2::ErrorCode::E_INVALID_HOST:
      return Status::Error("Invalid host!");
    case nebula::cpp2::ErrorCode::E_UNSUPPORTED:
      return Status::Error("Unsupported!");
    case nebula::cpp2::ErrorCode::E_NOT_DROP:
      return Status::Error("Not allowed to drop!");
    case nebula::cpp2::ErrorCode::E_BALANCER_RUNNING:
      return Status::Error("The balancer is running!");
    case nebula::cpp2::ErrorCode::E_CONFIG_IMMUTABLE:
      return Status::Error("Config immutable!");
    case nebula::cpp2::ErrorCode::E_CONFLICT:
      return Status::Error("Conflict!");
    case nebula::cpp2::ErrorCode::E_INVALID_PARM:
      return Status::Error("Invalid parm!");
    case nebula::cpp2::ErrorCode::E_WRONGCLUSTER:
      return Status::Error("Wrong cluster!");
    case nebula::cpp2::ErrorCode::E_STORE_FAILURE:
      return Status::Error("Store failure!");
    case nebula::cpp2::ErrorCode::E_STORE_SEGMENT_ILLEGAL:
      return Status::Error("Store segment illegal!");
    case nebula::cpp2::ErrorCode::E_BAD_BALANCE_PLAN:
      return Status::Error("Bad balance plan!");
    case nebula::cpp2::ErrorCode::E_BALANCED:
      return Status::Error("The cluster is balanced!");
    case nebula::cpp2::ErrorCode::E_NO_RUNNING_BALANCE_PLAN:
      return Status::Error("No running balance plan!");
    case nebula::cpp2::ErrorCode::E_NO_VALID_HOST:
      return Status::Error("No valid host hold the partition!");
    case nebula::cpp2::ErrorCode::E_CORRUPTTED_BALANCE_PLAN:
      return Status::Error("No corrupted blance plan!");
    case nebula::cpp2::ErrorCode::E_INVALID_PASSWORD:
      return Status::Error("Invalid password!");
    case nebula::cpp2::ErrorCode::E_IMPROPER_ROLE:
      return Status::Error("Improper role!");
    case nebula::cpp2::ErrorCode::E_INVALID_PARTITION_NUM:
      return Status::Error("No valid partition_num!");
    case nebula::cpp2::ErrorCode::E_INVALID_REPLICA_FACTOR:
      return Status::Error("No valid replica_factor!");
    case nebula::cpp2::ErrorCode::E_INVALID_CHARSET:
      return Status::Error("No valid charset!");
    case nebula::cpp2::ErrorCode::E_INVALID_COLLATE:
      return Status::Error("No valid collate!");
    case nebula::cpp2::ErrorCode::E_CHARSET_COLLATE_NOT_MATCH:
      return Status::Error("Charset and collate not match!");
    case nebula::cpp2::ErrorCode::E_SNAPSHOT_FAILURE:
      return Status::Error("Snapshot failure!");
    case nebula::cpp2::ErrorCode::E_BLOCK_WRITE_FAILURE:
      return Status::Error("Block write failure!");
    case nebula::cpp2::ErrorCode::E_REBUILD_INDEX_FAILED:
      return Status::Error("Rebuild index failed!");
    case nebula::cpp2::ErrorCode::E_INDEX_WITH_TTL:
      return Status::Error("Index with ttl!");
    case nebula::cpp2::ErrorCode::E_ADD_JOB_FAILURE:
      return Status::Error("Add job failure!");
    case nebula::cpp2::ErrorCode::E_STOP_JOB_FAILURE:
      return Status::Error("Stop job failure!");
    case nebula::cpp2::ErrorCode::E_SAVE_JOB_FAILURE:
      return Status::Error("Save job failure!");
    case nebula::cpp2::ErrorCode::E_BALANCER_FAILURE:
      return Status::Error("Balance failure!");
    case nebula::cpp2::ErrorCode::E_NO_INVALID_BALANCE_PLAN:
      return Status::Error("No invalid balance plan!");
    case nebula::cpp2::ErrorCode::E_JOB_NOT_FINISHED:
      return Status::Error("Job is not finished!");
    case nebula::cpp2::ErrorCode::E_TASK_REPORT_OUT_DATE:
      return Status::Error("Task report is out of date!");
    case nebula::cpp2::ErrorCode::E_BACKUP_FAILED:
      return Status::Error("Backup failure!");
    case nebula::cpp2::ErrorCode::E_BACKUP_BUILDING_INDEX:
      return Status::Error("Backup building indexes!");
    case nebula::cpp2::ErrorCode::E_BACKUP_SPACE_NOT_FOUND:
      return Status::Error("The space is not found when backup!");
    case nebula::cpp2::ErrorCode::E_RESTORE_FAILURE:
      return Status::Error("Restore failure!");
    case nebula::cpp2::ErrorCode::E_LIST_CLUSTER_FAILURE:
      return Status::Error("list cluster failure!");
    case nebula::cpp2::ErrorCode::E_LIST_CLUSTER_GET_ABS_PATH_FAILURE:
      return Status::Error("Failed to get the absolute path!");
    case nebula::cpp2::ErrorCode::E_GET_META_DIR_FAILURE:
      return Status::Error("Failed to get meta dir!");
    case nebula::cpp2::ErrorCode::E_INVALID_JOB:
      return Status::Error("No valid job!");
    case nebula::cpp2::ErrorCode::E_JOB_NOT_IN_SPACE:
      return Status::Error("Job not in chosen space!");
    case nebula::cpp2::ErrorCode::E_BACKUP_EMPTY_TABLE:
      return Status::Error("Backup empty table!");
    case nebula::cpp2::ErrorCode::E_BACKUP_TABLE_FAILED:
      return Status::Error("Backup table failure!");
    case nebula::cpp2::ErrorCode::E_SESSION_NOT_FOUND:
      return Status::Error("Session not existed!");
    default:
      return Status::Error("Unknown error!");
  }
}

PartsMap MetaClient::doGetPartsMap(const HostAddr& host, const LocalCache& localCache) {
  PartsMap partMap;
  for (auto it = localCache.begin(); it != localCache.end(); it++) {
    auto spaceId = it->first;
    auto& cache = it->second;
    auto partsIt = cache->partsOnHost_.find(host);
    if (partsIt != cache->partsOnHost_.end()) {
      for (auto& partId : partsIt->second) {
        auto partAllocIter = cache->partsAlloc_.find(partId);
        CHECK(partAllocIter != cache->partsAlloc_.end());
        auto& partM = partMap[spaceId][partId];
        partM.spaceId_ = spaceId;
        partM.partId_ = partId;
        partM.hosts_ = partAllocIter->second;
      }
    }
  }
  return partMap;
}

void MetaClient::diff(const LocalCache& oldCache, const LocalCache& newCache) {
  folly::RWSpinLock::WriteHolder holder(listenerLock_);
  if (listener_ == nullptr) {
    VLOG(3) << "Listener is null!";
    return;
  }
  auto newPartsMap = doGetPartsMap(options_.localHost_, newCache);
  auto oldPartsMap = doGetPartsMap(options_.localHost_, oldCache);
  VLOG(1) << "Let's check if any new parts added/updated for " << options_.localHost_;
  for (auto it = newPartsMap.begin(); it != newPartsMap.end(); it++) {
    auto spaceId = it->first;
    const auto& newParts = it->second;
    auto oldIt = oldPartsMap.find(spaceId);
    if (oldIt == oldPartsMap.end()) {
      VLOG(1) << "SpaceId " << spaceId << " was added!";
      listener_->onSpaceAdded(spaceId);
      for (auto partIt = newParts.begin(); partIt != newParts.end(); partIt++) {
        listener_->onPartAdded(partIt->second);
      }
    } else {
      const auto& oldParts = oldIt->second;
      for (auto partIt = newParts.begin(); partIt != newParts.end(); partIt++) {
        auto oldPartIt = oldParts.find(partIt->first);
        if (oldPartIt == oldParts.end()) {
          VLOG(1) << "SpaceId " << spaceId << ", partId " << partIt->first << " was added!";
          listener_->onPartAdded(partIt->second);
        } else {
          const auto& oldPartHosts = oldPartIt->second;
          const auto& newPartHosts = partIt->second;
          if (oldPartHosts != newPartHosts) {
            VLOG(1) << "SpaceId " << spaceId << ", partId " << partIt->first << " was updated!";
            listener_->onPartUpdated(newPartHosts);
          }
        }
      }
    }
  }
  VLOG(1) << "Let's check if any old parts removed....";
  for (auto it = oldPartsMap.begin(); it != oldPartsMap.end(); it++) {
    auto spaceId = it->first;
    const auto& oldParts = it->second;
    auto newIt = newPartsMap.find(spaceId);
    if (newIt == newPartsMap.end()) {
      VLOG(1) << "SpaceId " << spaceId << " was removed!";
      for (auto partIt = oldParts.begin(); partIt != oldParts.end(); partIt++) {
        listener_->onPartRemoved(spaceId, partIt->first);
      }
      listener_->onSpaceRemoved(spaceId);
    } else {
      const auto& newParts = newIt->second;
      for (auto partIt = oldParts.begin(); partIt != oldParts.end(); partIt++) {
        auto newPartIt = newParts.find(partIt->first);
        if (newPartIt == newParts.end()) {
          VLOG(1) << "SpaceId " << spaceId << ", partId " << partIt->first << " was removed!";
          listener_->onPartRemoved(spaceId, partIt->first);
        }
      }
    }
  }
}

void MetaClient::listenerDiff(const LocalCache& oldCache, const LocalCache& newCache) {
  folly::RWSpinLock::WriteHolder holder(listenerLock_);
  if (listener_ == nullptr) {
    VLOG(3) << "Listener is null!";
    return;
  }
  auto newMap = doGetListenersMap(options_.localHost_, newCache);
  auto oldMap = doGetListenersMap(options_.localHost_, oldCache);
  if (newMap == oldMap) {
    return;
  }

  VLOG(1) << "Let's check if any listeners parts added for " << options_.localHost_;
  for (auto& spaceEntry : newMap) {
    auto spaceId = spaceEntry.first;
    auto oldSpaceIter = oldMap.find(spaceId);
    if (oldSpaceIter == oldMap.end()) {
      // new space is added
      VLOG(1) << "[Listener] SpaceId " << spaceId << " was added!";
      listener_->onSpaceAdded(spaceId, true);
      for (const auto& partEntry : spaceEntry.second) {
        auto partId = partEntry.first;
        for (const auto& info : partEntry.second) {
          VLOG(1) << "[Listener] SpaceId " << spaceId << ", partId " << partId << " was added!";
          listener_->onListenerAdded(spaceId, partId, info);
        }
      }
    } else {
      // check if new part listener is added
      for (auto& partEntry : spaceEntry.second) {
        auto partId = partEntry.first;
        auto oldPartIter = oldSpaceIter->second.find(partId);
        if (oldPartIter == oldSpaceIter->second.end()) {
          for (const auto& info : partEntry.second) {
            VLOG(1) << "[Listener] SpaceId " << spaceId << ", partId " << partId << " was added!";
            listener_->onListenerAdded(spaceId, partId, info);
          }
        } else {
          std::sort(partEntry.second.begin(), partEntry.second.end());
          std::sort(oldPartIter->second.begin(), oldPartIter->second.end());
          std::vector<ListenerHosts> diff;
          std::set_difference(partEntry.second.begin(),
                              partEntry.second.end(),
                              oldPartIter->second.begin(),
                              oldPartIter->second.end(),
                              std::back_inserter(diff));
          for (const auto& info : diff) {
            VLOG(1) << "[Listener] SpaceId " << spaceId << ", partId " << partId << " was added!";
            listener_->onListenerAdded(spaceId, partId, info);
          }
        }
      }
    }
  }

  VLOG(1) << "Let's check if any old listeners removed....";
  for (auto& spaceEntry : oldMap) {
    auto spaceId = spaceEntry.first;
    auto newSpaceIter = newMap.find(spaceId);
    if (newSpaceIter == newMap.end()) {
      // remove old space
      for (const auto& partEntry : spaceEntry.second) {
        auto partId = partEntry.first;
        for (const auto& info : partEntry.second) {
          VLOG(1) << "SpaceId " << spaceId << ", partId " << partId << " was removed!";
          listener_->onListenerRemoved(spaceId, partId, info.type_);
        }
      }
      listener_->onSpaceRemoved(spaceId, true);
      VLOG(1) << "[Listener] SpaceId " << spaceId << " was removed!";
    } else {
      // check if part listener is removed
      for (auto& partEntry : spaceEntry.second) {
        auto partId = partEntry.first;
        auto newPartIter = newSpaceIter->second.find(partId);
        if (newPartIter == newSpaceIter->second.end()) {
          for (const auto& info : partEntry.second) {
            VLOG(1) << "[Listener] SpaceId " << spaceId << ", partId " << partId << " was removed!";
            listener_->onListenerRemoved(spaceId, partId, info.type_);
          }
        } else {
          std::sort(partEntry.second.begin(), partEntry.second.end());
          std::sort(newPartIter->second.begin(), newPartIter->second.end());
          std::vector<ListenerHosts> diff;
          std::set_difference(partEntry.second.begin(),
                              partEntry.second.end(),
                              newPartIter->second.begin(),
                              newPartIter->second.end(),
                              std::back_inserter(diff));
          for (const auto& info : diff) {
            VLOG(1) << "[Listener] SpaceId " << spaceId << ", partId " << partId << " was removed!";
            listener_->onListenerRemoved(spaceId, partId, info.type_);
          }
        }
      }
    }
  }
}

void MetaClient::loadRemoteListeners() {
  folly::RWSpinLock::WriteHolder holder(listenerLock_);
  if (listener_ == nullptr) {
    VLOG(3) << "Listener is null!";
    return;
  }
  auto partsMap = getPartsMapFromCache(options_.localHost_);
  for (const auto& spaceEntry : partsMap) {
    auto spaceId = spaceEntry.first;
    for (const auto& partEntry : spaceEntry.second) {
      auto partId = partEntry.first;
      auto listeners = getListenerHostTypeBySpacePartType(spaceId, partId);
      std::vector<HostAddr> remoteListeners;
      if (listeners.ok()) {
        for (const auto& listener : listeners.value()) {
          remoteListeners.emplace_back(listener.first);
        }
      }
      listener_->onCheckRemoteListeners(spaceId, partId, remoteListeners);
    }
  }
}

/// ================================== public methods =================================

PartitionID MetaClient::partId(int32_t numParts, const VertexID id) const {
  // If the length of the id is 8, we will treat it as int64_t to be compatible
  // with the version 1.0
  uint64_t vid = 0;
  if (id.size() == 8) {
    memcpy(static_cast<void*>(&vid), id.data(), 8);
  } else {
    MurmurHash2 hash;
    vid = hash(id.data());
  }
  PartitionID pId = vid % numParts + 1;
  CHECK_GT(pId, 0U);
  return pId;
}

folly::Future<StatusOr<cpp2::AdminJobResult>> MetaClient::submitJob(
    cpp2::AdminJobOp op, cpp2::AdminCmd cmd, std::vector<std::string> paras) {
  cpp2::AdminJobReq req;
  req.set_op(op);
  req.set_cmd(cmd);
  req.set_paras(std::move(paras));
  folly::Promise<StatusOr<cpp2::AdminJobResult>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_runAdminJob(request); },
      [](cpp2::AdminJobResp&& resp) -> decltype(auto) { return resp.get_result(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<GraphSpaceID>> MetaClient::createSpace(meta::cpp2::SpaceDesc spaceDesc,
                                                              bool ifNotExists) {
  cpp2::CreateSpaceReq req;
  req.set_properties(std::move(spaceDesc));
  req.set_if_not_exists(ifNotExists);
  folly::Promise<StatusOr<GraphSpaceID>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_createSpace(request); },
      [](cpp2::ExecResp&& resp) -> GraphSpaceID { return resp.get_id().get_space_id(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<GraphSpaceID>> MetaClient::createSpaceAs(const std::string& oldSpaceName,
                                                                const std::string& newSpaceName) {
  cpp2::CreateSpaceAsReq req;
  req.set_old_space_name(oldSpaceName);
  req.set_new_space_name(newSpaceName);
  folly::Promise<StatusOr<GraphSpaceID>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_createSpaceAs(request); },
      [](cpp2::ExecResp&& resp) -> GraphSpaceID { return resp.get_id().get_space_id(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<SpaceIdName>>> MetaClient::listSpaces() {
  cpp2::ListSpacesReq req;
  folly::Promise<StatusOr<std::vector<SpaceIdName>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listSpaces(request); },
      [this](cpp2::ListSpacesResp&& resp) -> decltype(auto) {
        return this->toSpaceIdName(resp.get_spaces());
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<cpp2::SpaceItem>> MetaClient::getSpace(std::string name) {
  cpp2::GetSpaceReq req;
  req.set_space_name(std::move(name));
  folly::Promise<StatusOr<cpp2::SpaceItem>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_getSpace(request); },
      [](cpp2::GetSpaceResp&& resp) -> decltype(auto) { return std::move(resp).get_item(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::dropSpace(std::string name, const bool ifExists) {
  cpp2::DropSpaceReq req;
  req.set_space_name(std::move(name));
  req.set_if_exists(ifExists);
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_dropSpace(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::HostItem>>> MetaClient::listHosts(cpp2::ListHostType tp) {
  cpp2::ListHostsReq req;
  req.set_type(tp);

  folly::Promise<StatusOr<std::vector<cpp2::HostItem>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listHosts(request); },
      [](cpp2::ListHostsResp&& resp) -> decltype(auto) { return resp.get_hosts(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::PartItem>>> MetaClient::listParts(
    GraphSpaceID spaceId, std::vector<PartitionID> partIds) {
  cpp2::ListPartsReq req;
  req.set_space_id(spaceId);
  req.set_part_ids(std::move(partIds));
  folly::Promise<StatusOr<std::vector<cpp2::PartItem>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listParts(request); },
      [](cpp2::ListPartsResp&& resp) -> decltype(auto) { return resp.get_parts(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::unordered_map<PartitionID, std::vector<HostAddr>>>>
MetaClient::getPartsAlloc(GraphSpaceID spaceId, PartTerms* partTerms) {
  cpp2::GetPartsAllocReq req;
  req.set_space_id(spaceId);
  folly::Promise<StatusOr<std::unordered_map<PartitionID, std::vector<HostAddr>>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_getPartsAlloc(request); },
      [=](cpp2::GetPartsAllocResp&& resp) -> decltype(auto) {
        std::unordered_map<PartitionID, std::vector<HostAddr>> parts;
        for (auto it = resp.get_parts().begin(); it != resp.get_parts().end(); it++) {
          parts.emplace(it->first, it->second);
        }
        if (partTerms && resp.terms_ref().has_value()) {
          for (auto& termOfPart : resp.terms_ref().value()) {
            (*partTerms)[termOfPart.first] = termOfPart.second;
          }
        }
        return parts;
      },
      std::move(promise));
  return future;
}

StatusOr<GraphSpaceID> MetaClient::getSpaceIdByNameFromCache(const std::string& name) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = spaceIndexByName_.find(name);
  if (it != spaceIndexByName_.end()) {
    return it->second;
  }
  return Status::SpaceNotFound();
}

StatusOr<std::string> MetaClient::getSpaceNameByIdFromCache(GraphSpaceID spaceId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceIt = localCache_.find(spaceId);
  if (spaceIt == localCache_.end()) {
    LOG(ERROR) << "Space " << spaceId << " not found!";
    return Status::Error("Space %d not found", spaceId);
  }
  return spaceIt->second->spaceDesc_.get_space_name();
}

StatusOr<TagID> MetaClient::getTagIDByNameFromCache(const GraphSpaceID& space,
                                                    const std::string& name) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = spaceTagIndexByName_.find(std::make_pair(space, name));
  if (it == spaceTagIndexByName_.end()) {
    return Status::Error("TagName `%s'  is nonexistent", name.c_str());
  }
  return it->second;
}

StatusOr<std::string> MetaClient::getTagNameByIdFromCache(const GraphSpaceID& space,
                                                          const TagID& tagId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = spaceTagIndexById_.find(std::make_pair(space, tagId));
  if (it == spaceTagIndexById_.end()) {
    return Status::Error("TagID `%d'  is nonexistent", tagId);
  }
  return it->second;
}

StatusOr<EdgeType> MetaClient::getEdgeTypeByNameFromCache(const GraphSpaceID& space,
                                                          const std::string& name) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = spaceEdgeIndexByName_.find(std::make_pair(space, name));
  if (it == spaceEdgeIndexByName_.end()) {
    return Status::Error("EdgeName `%s'  is nonexistent", name.c_str());
  }
  return it->second;
}

StatusOr<std::string> MetaClient::getEdgeNameByTypeFromCache(const GraphSpaceID& space,
                                                             const EdgeType edgeType) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = spaceEdgeIndexByType_.find(std::make_pair(space, edgeType));
  if (it == spaceEdgeIndexByType_.end()) {
    return Status::Error("EdgeType `%d'  is nonexistent", edgeType);
  }
  return it->second;
}

StatusOr<std::vector<std::string>> MetaClient::getAllEdgeFromCache(const GraphSpaceID& space) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = spaceAllEdgeMap_.find(space);
  if (it == spaceAllEdgeMap_.end()) {
    return Status::Error("SpaceId `%d'  is nonexistent", space);
  }
  return it->second;
}

folly::Future<StatusOr<bool>> MetaClient::multiPut(
    std::string segment, std::vector<std::pair<std::string, std::string>> pairs) {
  if (!nebula::meta::checkSegment(segment) || pairs.empty()) {
    return Status::Error("arguments invalid!");
  }

  cpp2::MultiPutReq req;
  std::vector<nebula::KeyValue> data;
  for (auto& element : pairs) {
    data.emplace_back(std::move(element));
  }
  req.set_segment(std::move(segment));
  req.set_pairs(std::move(data));
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_multiPut(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::string>> MetaClient::get(std::string segment, std::string key) {
  if (!nebula::meta::checkSegment(segment) || key.empty()) {
    return Status::Error("arguments invalid!");
  }

  cpp2::GetReq req;
  req.set_segment(std::move(segment));
  req.set_key(std::move(key));
  folly::Promise<StatusOr<std::string>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_get(request); },
      [](cpp2::GetResp&& resp) -> std::string { return resp.get_value(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<std::string>>> MetaClient::multiGet(
    std::string segment, std::vector<std::string> keys) {
  if (!nebula::meta::checkSegment(segment) || keys.empty()) {
    return Status::Error("arguments invalid!");
  }

  cpp2::MultiGetReq req;
  req.set_segment(std::move(segment));
  req.set_keys(std::move(keys));
  folly::Promise<StatusOr<std::vector<std::string>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_multiGet(request); },
      [](cpp2::MultiGetResp&& resp) -> std::vector<std::string> { return resp.get_values(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<std::string>>> MetaClient::scan(std::string segment,
                                                                   std::string start,
                                                                   std::string end) {
  if (!nebula::meta::checkSegment(segment) || start.empty() || end.empty()) {
    return Status::Error("arguments invalid!");
  }

  cpp2::ScanReq req;
  req.set_segment(std::move(segment));
  req.set_start(std::move(start));
  req.set_end(std::move(end));
  folly::Promise<StatusOr<std::vector<std::string>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_scan(request); },
      [](cpp2::ScanResp&& resp) -> std::vector<std::string> { return resp.get_values(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::remove(std::string segment, std::string key) {
  if (!nebula::meta::checkSegment(segment) || key.empty()) {
    return Status::Error("arguments invalid!");
  }

  cpp2::RemoveReq req;
  req.set_segment(std::move(segment));
  req.set_key(std::move(key));
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_remove(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::removeRange(std::string segment,
                                                      std::string start,
                                                      std::string end) {
  if (!nebula::meta::checkSegment(segment) || start.empty() || end.empty()) {
    return Status::Error("arguments invalid!");
  }

  cpp2::RemoveRangeReq req;
  req.set_segment(std::move(segment));
  req.set_start(std::move(start));
  req.set_end(std::move(end));
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_removeRange(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

PartsMap MetaClient::getPartsMapFromCache(const HostAddr& host) {
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  return doGetPartsMap(host, localCache_);
}

StatusOr<PartHosts> MetaClient::getPartHostsFromCache(GraphSpaceID spaceId, PartitionID partId) {
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = localCache_.find(spaceId);
  if (it == localCache_.end()) {
    return Status::Error("Space not found, spaceid: %d", spaceId);
  }
  auto& cache = it->second;
  auto partAllocIter = cache->partsAlloc_.find(partId);
  if (partAllocIter == cache->partsAlloc_.end()) {
    return Status::Error("Part not found in cache, spaceid: %d, partid: %d", spaceId, partId);
  }
  PartHosts ph;
  ph.spaceId_ = spaceId;
  ph.partId_ = partId;
  ph.hosts_ = partAllocIter->second;
  return ph;
}

Status MetaClient::checkPartExistInCache(const HostAddr& host,
                                         GraphSpaceID spaceId,
                                         PartitionID partId) {
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = localCache_.find(spaceId);
  if (it != localCache_.end()) {
    auto partsIt = it->second->partsOnHost_.find(host);
    if (partsIt != it->second->partsOnHost_.end()) {
      for (auto& pId : partsIt->second) {
        if (pId == partId) {
          return Status::OK();
        }
      }
      return Status::PartNotFound();
    } else {
      return Status::HostNotFound();
    }
  }
  return Status::SpaceNotFound();
}

Status MetaClient::checkSpaceExistInCache(const HostAddr& host, GraphSpaceID spaceId) {
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = localCache_.find(spaceId);
  if (it != localCache_.end()) {
    auto partsIt = it->second->partsOnHost_.find(host);
    if (partsIt != it->second->partsOnHost_.end() && !partsIt->second.empty()) {
      return Status::OK();
    } else {
      return Status::PartNotFound();
    }
  }
  return Status::SpaceNotFound();
}

StatusOr<int32_t> MetaClient::partsNum(GraphSpaceID spaceId) const {
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = localCache_.find(spaceId);
  if (it == localCache_.end()) {
    return Status::Error("Space not found, spaceid: %d", spaceId);
  }
  return it->second->partsAlloc_.size();
}

folly::Future<StatusOr<TagID>> MetaClient::createTagSchema(GraphSpaceID spaceId,
                                                           std::string name,
                                                           cpp2::Schema schema,
                                                           bool ifNotExists) {
  cpp2::CreateTagReq req;
  req.set_space_id(spaceId);
  req.set_tag_name(std::move(name));
  req.set_schema(std::move(schema));
  req.set_if_not_exists(ifNotExists);
  folly::Promise<StatusOr<TagID>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_createTag(request); },
      [](cpp2::ExecResp&& resp) -> TagID { return resp.get_id().get_tag_id(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::alterTagSchema(GraphSpaceID spaceId,
                                                         std::string name,
                                                         std::vector<cpp2::AlterSchemaItem> items,
                                                         cpp2::SchemaProp schemaProp) {
  cpp2::AlterTagReq req;
  req.set_space_id(spaceId);
  req.set_tag_name(std::move(name));
  req.set_tag_items(std::move(items));
  req.set_schema_prop(std::move(schemaProp));
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_alterTag(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::TagItem>>> MetaClient::listTagSchemas(
    GraphSpaceID spaceId) {
  cpp2::ListTagsReq req;
  req.set_space_id(spaceId);
  folly::Promise<StatusOr<std::vector<cpp2::TagItem>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listTags(request); },
      [](cpp2::ListTagsResp&& resp) -> decltype(auto) { return std::move(resp).get_tags(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::dropTagSchema(GraphSpaceID spaceId,
                                                        std::string tagName,
                                                        const bool ifExists) {
  cpp2::DropTagReq req;
  req.set_space_id(spaceId);
  req.set_tag_name(std::move(tagName));
  req.set_if_exists(ifExists);
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_dropTag(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<cpp2::Schema>> MetaClient::getTagSchema(GraphSpaceID spaceId,
                                                               std::string name,
                                                               int64_t version) {
  cpp2::GetTagReq req;
  req.set_space_id(spaceId);
  req.set_tag_name(std::move(name));
  req.set_version(version);
  folly::Promise<StatusOr<cpp2::Schema>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_getTag(request); },
      [](cpp2::GetTagResp&& resp) -> cpp2::Schema { return std::move(resp).get_schema(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<EdgeType>> MetaClient::createEdgeSchema(GraphSpaceID spaceId,
                                                               std::string name,
                                                               cpp2::Schema schema,
                                                               bool ifNotExists) {
  cpp2::CreateEdgeReq req;
  req.set_space_id(spaceId);
  req.set_edge_name(std::move(name));
  req.set_schema(schema);
  req.set_if_not_exists(ifNotExists);

  folly::Promise<StatusOr<EdgeType>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_createEdge(request); },
      [](cpp2::ExecResp&& resp) -> EdgeType { return resp.get_id().get_edge_type(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::alterEdgeSchema(GraphSpaceID spaceId,
                                                          std::string name,
                                                          std::vector<cpp2::AlterSchemaItem> items,
                                                          cpp2::SchemaProp schemaProp) {
  cpp2::AlterEdgeReq req;
  req.set_space_id(spaceId);
  req.set_edge_name(std::move(name));
  req.set_edge_items(std::move(items));
  req.set_schema_prop(std::move(schemaProp));
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_alterEdge(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::EdgeItem>>> MetaClient::listEdgeSchemas(
    GraphSpaceID spaceId) {
  cpp2::ListEdgesReq req;
  req.set_space_id(spaceId);
  folly::Promise<StatusOr<std::vector<cpp2::EdgeItem>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listEdges(request); },
      [](cpp2::ListEdgesResp&& resp) -> decltype(auto) { return std::move(resp).get_edges(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<cpp2::Schema>> MetaClient::getEdgeSchema(GraphSpaceID spaceId,
                                                                std::string name,
                                                                SchemaVer version) {
  cpp2::GetEdgeReq req;
  req.set_space_id(spaceId);
  req.set_edge_name(std::move(name));
  req.set_version(version);
  folly::Promise<StatusOr<cpp2::Schema>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_getEdge(request); },
      [](cpp2::GetEdgeResp&& resp) -> cpp2::Schema { return std::move(resp).get_schema(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::dropEdgeSchema(GraphSpaceID spaceId,
                                                         std::string name,
                                                         const bool ifExists) {
  cpp2::DropEdgeReq req;
  req.set_space_id(spaceId);
  req.set_edge_name(std::move(name));
  req.set_if_exists(ifExists);
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_dropEdge(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<IndexID>> MetaClient::createTagIndex(GraphSpaceID spaceID,
                                                            std::string indexName,
                                                            std::string tagName,
                                                            std::vector<cpp2::IndexFieldDef> fields,
                                                            bool ifNotExists,
                                                            const std::string* comment) {
  cpp2::CreateTagIndexReq req;
  req.set_space_id(spaceID);
  req.set_index_name(std::move(indexName));
  req.set_tag_name(std::move(tagName));
  req.set_fields(std::move(fields));
  req.set_if_not_exists(ifNotExists);
  if (comment != nullptr) {
    req.set_comment(*comment);
  }

  folly::Promise<StatusOr<IndexID>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_createTagIndex(request); },
      [](cpp2::ExecResp&& resp) -> IndexID { return resp.get_id().get_index_id(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::dropTagIndex(GraphSpaceID spaceID,
                                                       std::string name,
                                                       bool ifExists) {
  cpp2::DropTagIndexReq req;
  req.set_space_id(spaceID);
  req.set_index_name(std::move(name));
  req.set_if_exists(ifExists);

  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_dropTagIndex(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<cpp2::IndexItem>> MetaClient::getTagIndex(GraphSpaceID spaceID,
                                                                 std::string name) {
  cpp2::GetTagIndexReq req;
  req.set_space_id(spaceID);
  req.set_index_name(std::move(name));

  folly::Promise<StatusOr<cpp2::IndexItem>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_getTagIndex(request); },
      [](cpp2::GetTagIndexResp&& resp) -> cpp2::IndexItem { return std::move(resp).get_item(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::IndexItem>>> MetaClient::listTagIndexes(
    GraphSpaceID spaceId) {
  cpp2::ListTagIndexesReq req;
  req.set_space_id(spaceId);

  folly::Promise<StatusOr<std::vector<cpp2::IndexItem>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listTagIndexes(request); },
      [](cpp2::ListTagIndexesResp&& resp) -> decltype(auto) { return std::move(resp).get_items(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::rebuildTagIndex(GraphSpaceID spaceID, std::string name) {
  cpp2::RebuildIndexReq req;
  req.set_space_id(spaceID);
  req.set_index_name(std::move(name));

  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_rebuildTagIndex(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::IndexStatus>>> MetaClient::listTagIndexStatus(
    GraphSpaceID spaceID) {
  cpp2::ListIndexStatusReq req;
  req.set_space_id(spaceID);

  folly::Promise<StatusOr<std::vector<cpp2::IndexStatus>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listTagIndexStatus(request); },
      [](cpp2::ListIndexStatusResp&& resp) -> decltype(auto) {
        return std::move(resp).get_statuses();
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<IndexID>> MetaClient::createEdgeIndex(
    GraphSpaceID spaceID,
    std::string indexName,
    std::string edgeName,
    std::vector<cpp2::IndexFieldDef> fields,
    bool ifNotExists,
    const std::string* comment) {
  cpp2::CreateEdgeIndexReq req;
  req.set_space_id(spaceID);
  req.set_index_name(std::move(indexName));
  req.set_edge_name(std::move(edgeName));
  req.set_fields(std::move(fields));
  req.set_if_not_exists(ifNotExists);
  if (comment != nullptr) {
    req.set_comment(*comment);
  }

  folly::Promise<StatusOr<IndexID>> promise;
  auto future = promise.getFuture();

  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_createEdgeIndex(request); },
      [](cpp2::ExecResp&& resp) -> IndexID { return resp.get_id().get_index_id(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::dropEdgeIndex(GraphSpaceID spaceId,
                                                        std::string name,
                                                        bool ifExists) {
  cpp2::DropEdgeIndexReq req;
  req.set_space_id(spaceId);
  req.set_index_name(std::move(name));
  req.set_if_exists(ifExists);

  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_dropEdgeIndex(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<cpp2::IndexItem>> MetaClient::getEdgeIndex(GraphSpaceID spaceId,
                                                                  std::string name) {
  cpp2::GetEdgeIndexReq req;
  req.set_space_id(spaceId);
  req.set_index_name(std::move(name));

  folly::Promise<StatusOr<cpp2::IndexItem>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_getEdgeIndex(request); },
      [](cpp2::GetEdgeIndexResp&& resp) -> cpp2::IndexItem { return std::move(resp).get_item(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::IndexItem>>> MetaClient::listEdgeIndexes(
    GraphSpaceID spaceId) {
  cpp2::ListEdgeIndexesReq req;
  req.set_space_id(spaceId);

  folly::Promise<StatusOr<std::vector<cpp2::IndexItem>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listEdgeIndexes(request); },
      [](cpp2::ListEdgeIndexesResp&& resp) -> decltype(auto) {
        return std::move(resp).get_items();
      },
      std::move(promise));
  return future;
}

StatusOr<int32_t> MetaClient::getSpaceVidLen(const GraphSpaceID& spaceId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceIt = localCache_.find(spaceId);
  if (spaceIt == localCache_.end()) {
    LOG(ERROR) << "Space " << spaceId << " not found!";
    return Status::Error("Space %d not found", spaceId);
  }
  auto& vidType = spaceIt->second->spaceDesc_.get_vid_type();
  auto vIdLen = vidType.type_length_ref().has_value() ? *vidType.get_type_length() : 0;
  if (vIdLen <= 0) {
    return Status::Error("Space %d vertexId length invalid", spaceId);
  }
  return vIdLen;
}

StatusOr<nebula::cpp2::PropertyType> MetaClient::getSpaceVidType(const GraphSpaceID& spaceId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceIt = localCache_.find(spaceId);
  if (spaceIt == localCache_.end()) {
    LOG(ERROR) << "Space " << spaceId << " not found!";
    return Status::Error("Space %d not found", spaceId);
  }
  auto vIdType = spaceIt->second->spaceDesc_.get_vid_type().get_type();
  if (vIdType != nebula::cpp2::PropertyType::INT64 &&
      vIdType != nebula::cpp2::PropertyType::FIXED_STRING) {
    std::stringstream ss;
    ss << "Space " << spaceId
       << ", vertexId type invalid: " << apache::thrift::util::enumNameSafe(vIdType);
    return Status::Error(ss.str());
  }
  return vIdType;
}

StatusOr<cpp2::SpaceDesc> MetaClient::getSpaceDesc(const GraphSpaceID& space) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceIt = localCache_.find(space);
  if (spaceIt == localCache_.end()) {
    LOG(ERROR) << "Space " << space << " not found!";
    return Status::Error("Space %d not found", space);
  }
  return spaceIt->second->spaceDesc_;
}

StatusOr<meta::cpp2::IsolationLevel> MetaClient::getIsolationLevel(GraphSpaceID spaceId) {
  auto spaceDescStatus = getSpaceDesc(spaceId);
  if (!spaceDescStatus.ok()) {
    return spaceDescStatus.status();
  }
  using IsolationLevel = meta::cpp2::IsolationLevel;
  return spaceDescStatus.value().isolation_level_ref().value_or(IsolationLevel::DEFAULT);
}

StatusOr<std::shared_ptr<const NebulaSchemaProvider>> MetaClient::getTagSchemaFromCache(
    GraphSpaceID spaceId, TagID tagID, SchemaVer ver) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceIt = localCache_.find(spaceId);
  if (spaceIt != localCache_.end()) {
    auto tagIt = spaceIt->second->tagSchemas_.find(tagID);
    if (tagIt != spaceIt->second->tagSchemas_.end() && !tagIt->second.empty()) {
      size_t vNum = tagIt->second.size();
      if (static_cast<SchemaVer>(vNum) > ver) {
        return ver < 0 ? tagIt->second.back() : tagIt->second[ver];
      }
    }
  }
  LOG(ERROR) << "The tag schema " << tagID << " not found in space " << spaceId;
  return std::shared_ptr<const NebulaSchemaProvider>();
}

StatusOr<std::shared_ptr<const NebulaSchemaProvider>> MetaClient::getEdgeSchemaFromCache(
    GraphSpaceID spaceId, EdgeType edgeType, SchemaVer ver) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceIt = localCache_.find(spaceId);
  if (spaceIt != localCache_.end()) {
    auto edgeIt = spaceIt->second->edgeSchemas_.find(edgeType);
    if (edgeIt != spaceIt->second->edgeSchemas_.end() && !edgeIt->second.empty()) {
      size_t vNum = edgeIt->second.size();
      if (static_cast<SchemaVer>(vNum) > ver) {
        return ver < 0 ? edgeIt->second.back() : edgeIt->second[ver];
      }
    }
  }
  LOG(ERROR) << "The edge schema " << edgeType << " not found in space " << spaceId;
  return std::shared_ptr<const NebulaSchemaProvider>();
}

StatusOr<TagSchemas> MetaClient::getAllVerTagSchema(GraphSpaceID spaceId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto iter = localCache_.find(spaceId);
  if (iter == localCache_.end()) {
    return Status::Error("Space %d not found", spaceId);
  }
  return iter->second->tagSchemas_;
}

StatusOr<TagSchema> MetaClient::getAllLatestVerTagSchema(const GraphSpaceID& spaceId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto iter = localCache_.find(spaceId);
  if (iter == localCache_.end()) {
    return Status::Error("Space %d not found", spaceId);
  }
  TagSchema tagsSchema;
  tagsSchema.reserve(iter->second->tagSchemas_.size());
  // fetch all tagIds
  for (const auto& tagSchema : iter->second->tagSchemas_) {
    tagsSchema.emplace(tagSchema.first, tagSchema.second.back());
  }
  return tagsSchema;
}

StatusOr<EdgeSchemas> MetaClient::getAllVerEdgeSchema(GraphSpaceID spaceId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto iter = localCache_.find(spaceId);
  if (iter == localCache_.end()) {
    return Status::Error("Space %d not found", spaceId);
  }
  return iter->second->edgeSchemas_;
}

StatusOr<EdgeSchema> MetaClient::getAllLatestVerEdgeSchemaFromCache(const GraphSpaceID& spaceId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto iter = localCache_.find(spaceId);
  if (iter == localCache_.end()) {
    return Status::Error("Space %d not found", spaceId);
  }
  EdgeSchema edgesSchema;
  edgesSchema.reserve(iter->second->edgeSchemas_.size());
  // fetch all edgeTypes
  for (const auto& edgeSchema : iter->second->edgeSchemas_) {
    edgesSchema.emplace(edgeSchema.first, edgeSchema.second.back());
  }
  return edgesSchema;
}

folly::Future<StatusOr<bool>> MetaClient::rebuildEdgeIndex(GraphSpaceID spaceID, std::string name) {
  cpp2::RebuildIndexReq req;
  req.set_space_id(spaceID);
  req.set_index_name(std::move(name));

  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_rebuildEdgeIndex(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::IndexStatus>>> MetaClient::listEdgeIndexStatus(
    GraphSpaceID spaceID) {
  cpp2::ListIndexStatusReq req;
  req.set_space_id(spaceID);

  folly::Promise<StatusOr<std::vector<cpp2::IndexStatus>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listEdgeIndexStatus(request); },
      [](cpp2::ListIndexStatusResp&& resp) -> decltype(auto) {
        return std::move(resp).get_statuses();
      },
      std::move(promise));
  return future;
}

StatusOr<std::shared_ptr<cpp2::IndexItem>> MetaClient::getTagIndexByNameFromCache(
    const GraphSpaceID space, const std::string& name) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  std::pair<GraphSpaceID, std::string> key(space, name);
  auto iter = tagNameIndexMap_.find(key);
  if (iter == tagNameIndexMap_.end()) {
    return Status::IndexNotFound();
  }
  auto indexID = iter->second;
  auto itemStatus = getTagIndexFromCache(space, indexID);
  if (!itemStatus.ok()) {
    return itemStatus.status();
  }
  return itemStatus.value();
}

StatusOr<std::shared_ptr<cpp2::IndexItem>> MetaClient::getEdgeIndexByNameFromCache(
    const GraphSpaceID space, const std::string& name) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  std::pair<GraphSpaceID, std::string> key(space, name);
  auto iter = edgeNameIndexMap_.find(key);
  if (iter == edgeNameIndexMap_.end()) {
    return Status::IndexNotFound();
  }
  auto indexID = iter->second;
  auto itemStatus = getEdgeIndexFromCache(space, indexID);
  if (!itemStatus.ok()) {
    return itemStatus.status();
  }
  return itemStatus.value();
}

StatusOr<std::shared_ptr<cpp2::IndexItem>> MetaClient::getTagIndexFromCache(GraphSpaceID spaceId,
                                                                            IndexID indexID) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }

  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceIt = localCache_.find(spaceId);
  if (spaceIt == localCache_.end()) {
    VLOG(3) << "Space " << spaceId << " not found!";
    return Status::SpaceNotFound();
  } else {
    auto iter = spaceIt->second->tagIndexes_.find(indexID);
    if (iter == spaceIt->second->tagIndexes_.end()) {
      VLOG(3) << "Space " << spaceId << ", Tag Index " << indexID << " not found!";
      return Status::IndexNotFound();
    } else {
      return iter->second;
    }
  }
}

StatusOr<TagID> MetaClient::getRelatedTagIDByIndexNameFromCache(const GraphSpaceID space,
                                                                const std::string& indexName) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }

  auto indexRet = getTagIndexByNameFromCache(space, indexName);
  if (!indexRet.ok()) {
    LOG(ERROR) << "Index " << indexName << " Not Found";
    return indexRet.status();
  }

  return indexRet.value()->get_schema_id().get_tag_id();
}

StatusOr<std::shared_ptr<cpp2::IndexItem>> MetaClient::getEdgeIndexFromCache(GraphSpaceID spaceId,
                                                                             IndexID indexID) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }

  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceIt = localCache_.find(spaceId);
  if (spaceIt == localCache_.end()) {
    VLOG(3) << "Space " << spaceId << " not found!";
    return Status::SpaceNotFound();
  } else {
    auto iter = spaceIt->second->edgeIndexes_.find(indexID);
    if (iter == spaceIt->second->edgeIndexes_.end()) {
      VLOG(3) << "Space " << spaceId << ", Edge Index " << indexID << " not found!";
      return Status::IndexNotFound();
    } else {
      return iter->second;
    }
  }
}

StatusOr<EdgeType> MetaClient::getRelatedEdgeTypeByIndexNameFromCache(
    const GraphSpaceID space, const std::string& indexName) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }

  auto indexRet = getEdgeIndexByNameFromCache(space, indexName);
  if (!indexRet.ok()) {
    LOG(ERROR) << "Index " << indexName << " Not Found";
    return indexRet.status();
  }

  return indexRet.value()->get_schema_id().get_edge_type();
}

StatusOr<std::vector<std::shared_ptr<cpp2::IndexItem>>> MetaClient::getTagIndexesFromCache(
    GraphSpaceID spaceId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }

  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceIt = localCache_.find(spaceId);
  if (spaceIt == localCache_.end()) {
    VLOG(3) << "Space " << spaceId << " not found!";
    return Status::SpaceNotFound();
  } else {
    auto tagIndexes = spaceIt->second->tagIndexes_;
    auto iter = tagIndexes.begin();
    std::vector<std::shared_ptr<cpp2::IndexItem>> items;
    while (iter != tagIndexes.end()) {
      items.emplace_back(iter->second);
      iter++;
    }
    return items;
  }
}

StatusOr<std::vector<std::shared_ptr<cpp2::IndexItem>>> MetaClient::getEdgeIndexesFromCache(
    GraphSpaceID spaceId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }

  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceIt = localCache_.find(spaceId);
  if (spaceIt == localCache_.end()) {
    VLOG(3) << "Space " << spaceId << " not found!";
    return Status::SpaceNotFound();
  } else {
    auto edgeIndexes = spaceIt->second->edgeIndexes_;
    auto iter = edgeIndexes.begin();
    std::vector<std::shared_ptr<cpp2::IndexItem>> items;
    while (iter != edgeIndexes.end()) {
      items.emplace_back(iter->second);
      iter++;
    }
    return items;
  }
}

StatusOr<HostAddr> MetaClient::getStorageLeaderFromCache(GraphSpaceID spaceId, PartitionID partId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }

  {
    folly::RWSpinLock::ReadHolder holder(leadersLock_);
    auto iter = leadersInfo_.leaderMap_.find({spaceId, partId});
    if (iter != leadersInfo_.leaderMap_.end()) {
      return iter->second;
    }
  }
  {
    // no leader found, pick one in round-robin
    auto partHostsRet = getPartHostsFromCache(spaceId, partId);
    if (!partHostsRet.ok()) {
      return partHostsRet.status();
    }
    auto partHosts = partHostsRet.value();
    folly::RWSpinLock::WriteHolder wh(leadersLock_);
    VLOG(1) << "No leader exists. Choose one in round-robin.";
    auto index = (leadersInfo_.pickedIndex_[{spaceId, partId}] + 1) % partHosts.hosts_.size();
    auto picked = partHosts.hosts_[index];
    leadersInfo_.leaderMap_[{spaceId, partId}] = picked;
    leadersInfo_.pickedIndex_[{spaceId, partId}] = index;
    return picked;
  }
}

void MetaClient::updateStorageLeader(GraphSpaceID spaceId,
                                     PartitionID partId,
                                     const HostAddr& leader) {
  VLOG(1) << "Update the leader for [" << spaceId << ", " << partId << "] to " << leader;
  folly::RWSpinLock::WriteHolder holder(leadersLock_);
  leadersInfo_.leaderMap_[{spaceId, partId}] = leader;
}

void MetaClient::invalidStorageLeader(GraphSpaceID spaceId, PartitionID partId) {
  VLOG(1) << "Invalidate the leader for [" << spaceId << ", " << partId << "]";
  folly::RWSpinLock::WriteHolder holder(leadersLock_);
  leadersInfo_.leaderMap_.erase({spaceId, partId});
}

StatusOr<LeaderInfo> MetaClient::getLeaderInfo() {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(leadersLock_);
  return leadersInfo_;
}

const std::vector<HostAddr>& MetaClient::getAddresses() { return addrs_; }

std::vector<cpp2::RoleItem> MetaClient::getRolesByUserFromCache(const std::string& user) const {
  if (!ready_) {
    return std::vector<cpp2::RoleItem>(0);
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto iter = userRolesMap_.find(user);
  if (iter == userRolesMap_.end()) {
    return std::vector<cpp2::RoleItem>(0);
  }
  return iter->second;
}

bool MetaClient::authCheckFromCache(const std::string& account, const std::string& password) const {
  if (!ready_) {
    return false;
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto iter = userPasswordMap_.find(account);
  if (iter == userPasswordMap_.end()) {
    return false;
  }
  return iter->second == password;
}

bool MetaClient::checkShadowAccountFromCache(const std::string& account) const {
  if (!ready_) {
    return false;
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto iter = userPasswordMap_.find(account);
  if (iter != userPasswordMap_.end()) {
    return true;
  }
  return false;
}

StatusOr<TermID> MetaClient::getTermFromCache(GraphSpaceID spaceId, PartitionID partId) const {
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceInfo = localCache_.find(spaceId);
  if (spaceInfo == localCache_.end()) {
    return Status::Error("Term not found!");
  }

  auto termInfo = spaceInfo->second->termOfPartition_.find(partId);
  if (termInfo == spaceInfo->second->termOfPartition_.end()) {
    return Status::Error("Term not found!");
  }

  return termInfo->second;
}

StatusOr<std::vector<HostAddr>> MetaClient::getStorageHosts() const {
  if (!ready_) {
    return Status::Error("Not ready!");
  }

  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  return storageHosts_;
}

StatusOr<SchemaVer> MetaClient::getLatestTagVersionFromCache(const GraphSpaceID& space,
                                                             const TagID& tagId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = spaceNewestTagVerMap_.find(std::make_pair(space, tagId));
  if (it == spaceNewestTagVerMap_.end()) {
    return Status::TagNotFound();
  }
  return it->second;
}

StatusOr<SchemaVer> MetaClient::getLatestEdgeVersionFromCache(const GraphSpaceID& space,
                                                              const EdgeType& edgeType) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto it = spaceNewestEdgeVerMap_.find(std::make_pair(space, edgeType));
  if (it == spaceNewestEdgeVerMap_.end()) {
    return Status::EdgeNotFound();
  }
  return it->second;
}

folly::Future<StatusOr<bool>> MetaClient::heartbeat() {
  cpp2::HBReq req;
  req.set_host(options_.localHost_);
  req.set_role(options_.role_);
  req.set_git_info_sha(options_.gitInfoSHA_);
  req.set_version(getOriginVersion());
  if (options_.role_ == cpp2::HostRole::STORAGE) {
    if (options_.clusterId_.load() == 0) {
      options_.clusterId_ = FileBasedClusterIdMan::getClusterIdFromFile(FLAGS_cluster_id_path);
    }
    req.set_cluster_id(options_.clusterId_.load());
    std::unordered_map<GraphSpaceID, std::vector<cpp2::LeaderInfo>> leaderIds;
    if (listener_ != nullptr) {
      listener_->fetchLeaderInfo(leaderIds);
      if (leaderIds_ != leaderIds) {
        {
          folly::RWSpinLock::WriteHolder holder(leaderIdsLock_);
          leaderIds_.clear();
          leaderIds_ = leaderIds;
        }
      }
      req.set_leader_partIds(std::move(leaderIds));
    } else {
      req.set_leader_partIds(std::move(leaderIds));
    }
  }

  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  VLOG(1) << "Send heartbeat to " << leader_ << ", clusterId " << req.get_cluster_id();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_heartBeat(request); },
      [this](cpp2::HBResp&& resp) -> bool {
        if (options_.role_ == cpp2::HostRole::STORAGE && options_.clusterId_.load() == 0) {
          LOG(INFO) << "Persisit the cluster Id from metad " << resp.get_cluster_id();
          if (FileBasedClusterIdMan::persistInFile(resp.get_cluster_id(), FLAGS_cluster_id_path)) {
            options_.clusterId_.store(resp.get_cluster_id());
          } else {
            LOG(FATAL) << "Can't persist the clusterId in file " << FLAGS_cluster_id_path;
          }
        }
        heartbeatTime_ = time::WallClock::fastNowInMilliSec();
        metadLastUpdateTime_ = resp.get_last_update_time_in_ms();
        VLOG(1) << "Metad last update time: " << metadLastUpdateTime_;
        metaServerVersion_ = resp.get_meta_version();
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::createUser(std::string account,
                                                     std::string password,
                                                     bool ifNotExists) {
  cpp2::CreateUserReq req;
  req.set_account(std::move(account));
  req.set_encoded_pwd(std::move(password));
  req.set_if_not_exists(ifNotExists);
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_createUser(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::dropUser(std::string account, bool ifExists) {
  cpp2::DropUserReq req;
  req.set_account(std::move(account));
  req.set_if_exists(ifExists);
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_dropUser(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::alterUser(std::string account, std::string password) {
  cpp2::AlterUserReq req;
  req.set_account(std::move(account));
  req.set_encoded_pwd(std::move(password));
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_alterUser(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::grantToUser(cpp2::RoleItem roleItem) {
  cpp2::GrantRoleReq req;
  req.set_role_item(std::move(roleItem));
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_grantRole(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::revokeFromUser(cpp2::RoleItem roleItem) {
  cpp2::RevokeRoleReq req;
  req.set_role_item(std::move(roleItem));
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_revokeRole(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::map<std::string, cpp2::UserDescItem>>> MetaClient::listUsers() {
  cpp2::ListUsersReq req;
  folly::Promise<StatusOr<std::map<std::string, cpp2::UserDescItem>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listUsers(request); },
      [](cpp2::ListUsersResp&& resp) -> decltype(auto) { return std::move(resp).get_users(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::RoleItem>>> MetaClient::listRoles(GraphSpaceID space) {
  cpp2::ListRolesReq req;
  req.set_space_id(std::move(space));
  folly::Promise<StatusOr<std::vector<cpp2::RoleItem>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listRoles(request); },
      [](cpp2::ListRolesResp&& resp) -> decltype(auto) { return std::move(resp).get_roles(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::changePassword(std::string account,
                                                         std::string newPwd,
                                                         std::string oldPwd) {
  cpp2::ChangePasswordReq req;
  req.set_account(std::move(account));
  req.set_new_encoded_pwd(std::move(newPwd));
  req.set_old_encoded_pwd(std::move(oldPwd));
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_changePassword(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::RoleItem>>> MetaClient::getUserRoles(std::string account) {
  cpp2::GetUserRolesReq req;
  req.set_account(std::move(account));
  folly::Promise<StatusOr<std::vector<cpp2::RoleItem>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_getUserRoles(request); },
      [](cpp2::ListRolesResp&& resp) -> decltype(auto) { return std::move(resp).get_roles(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<int64_t>> MetaClient::balance(std::vector<HostAddr> hostDel,
                                                     bool isStop,
                                                     bool isReset) {
  cpp2::BalanceReq req;
  if (!hostDel.empty()) {
    req.set_host_del(std::move(hostDel));
  }
  if (isStop) {
    req.set_stop(isStop);
  }
  if (isReset) {
    req.set_reset(isReset);
  }

  folly::Promise<StatusOr<int64_t>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_balance(request); },
      [](cpp2::BalanceResp&& resp) -> int64_t { return resp.get_id(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::BalanceTask>>> MetaClient::showBalance(int64_t balanceId) {
  cpp2::BalanceReq req;
  req.set_id(balanceId);
  folly::Promise<StatusOr<std::vector<cpp2::BalanceTask>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_balance(request); },
      [](cpp2::BalanceResp&& resp) -> std::vector<cpp2::BalanceTask> { return resp.get_tasks(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::balanceLeader() {
  cpp2::LeaderBalanceReq req;
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_leaderBalance(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::string>> MetaClient::getTagDefaultValue(GraphSpaceID spaceId,
                                                                    TagID tagId,
                                                                    const std::string& field) {
  cpp2::GetReq req;
  static std::string defaultKey = "__default__";
  req.set_segment(defaultKey);
  std::string key;
  key.reserve(64);
  key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
  key.append(reinterpret_cast<const char*>(&tagId), sizeof(TagID));
  key.append(field);
  req.set_key(std::move(key));
  folly::Promise<StatusOr<std::string>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_get(request); },
      [](cpp2::GetResp&& resp) -> std::string { return resp.get_value(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::string>> MetaClient::getEdgeDefaultValue(GraphSpaceID spaceId,
                                                                     EdgeType edgeType,
                                                                     const std::string& field) {
  cpp2::GetReq req;
  static std::string defaultKey = "__default__";
  req.set_segment(defaultKey);
  std::string key;
  key.reserve(64);
  key.append(reinterpret_cast<const char*>(&spaceId), sizeof(GraphSpaceID));
  key.append(reinterpret_cast<const char*>(&edgeType), sizeof(EdgeType));
  key.append(field);
  req.set_key(std::move(key));
  folly::Promise<StatusOr<std::string>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_get(request); },
      [](cpp2::GetResp&& resp) -> std::string { return resp.get_value(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::regConfig(const std::vector<cpp2::ConfigItem>& items) {
  cpp2::RegConfigReq req;
  req.set_items(items);
  folly::Promise<StatusOr<int64_t>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_regConfig(request); },
      [](cpp2::ExecResp&& resp) -> decltype(auto) {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>> MetaClient::getConfig(
    const cpp2::ConfigModule& module, const std::string& name) {
  if (!configReady_) {
    return Status::Error("Not ready!");
  }
  cpp2::ConfigItem item;
  item.set_module(module);
  item.set_name(name);
  cpp2::GetConfigReq req;
  req.set_item(item);
  folly::Promise<StatusOr<std::vector<cpp2::ConfigItem>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_getConfig(request); },
      [](cpp2::GetConfigResp&& resp) -> decltype(auto) { return std::move(resp).get_items(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::setConfig(const cpp2::ConfigModule& module,
                                                    const std::string& name,
                                                    const Value& value) {
  cpp2::ConfigItem item;
  item.set_module(module);
  item.set_name(name);
  item.set_value(value);

  cpp2::SetConfigReq req;
  req.set_item(item);
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_setConfig(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>> MetaClient::listConfigs(
    const cpp2::ConfigModule& module) {
  cpp2::ListConfigsReq req;
  req.set_module(module);
  folly::Promise<StatusOr<std::vector<cpp2::ConfigItem>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listConfigs(request); },
      [](cpp2::ListConfigsResp&& resp) -> decltype(auto) { return std::move(resp).get_items(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::createSnapshot() {
  cpp2::CreateSnapshotReq req;
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_createSnapshot(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::dropSnapshot(const std::string& name) {
  cpp2::DropSnapshotReq req;
  req.set_name(name);
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_dropSnapshot(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::Snapshot>>> MetaClient::listSnapshots() {
  cpp2::ListSnapshotsReq req;
  folly::Promise<StatusOr<std::vector<cpp2::Snapshot>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listSnapshots(request); },
      [](cpp2::ListSnapshotsResp&& resp) -> decltype(auto) {
        return std::move(resp).get_snapshots();
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::addListener(GraphSpaceID spaceId,
                                                      cpp2::ListenerType type,
                                                      std::vector<HostAddr> hosts) {
  cpp2::AddListenerReq req;
  req.set_space_id(spaceId);
  req.set_type(type);
  req.set_hosts(std::move(hosts));
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_addListener(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::removeListener(GraphSpaceID spaceId,
                                                         cpp2::ListenerType type) {
  cpp2::RemoveListenerReq req;
  req.set_space_id(spaceId);
  req.set_type(type);
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_removeListener(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::ListenerInfo>>> MetaClient::listListener(
    GraphSpaceID spaceId) {
  cpp2::ListListenerReq req;
  req.set_space_id(spaceId);
  folly::Promise<StatusOr<std::vector<cpp2::ListenerInfo>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listListener(request); },
      [](cpp2::ListListenerResp&& resp) -> decltype(auto) {
        return std::move(resp).get_listeners();
      },
      std::move(promise));
  return future;
}

bool MetaClient::registerCfg() {
  auto ret = regConfig(gflagsDeclared_).get();
  if (ret.ok()) {
    LOG(INFO) << "Register gflags ok " << gflagsDeclared_.size();
    configReady_ = true;
  }
  return configReady_;
}

StatusOr<std::vector<std::pair<PartitionID, cpp2::ListenerType>>>
MetaClient::getListenersBySpaceHostFromCache(GraphSpaceID spaceId, const HostAddr& host) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceIt = localCache_.find(spaceId);
  if (spaceIt == localCache_.end()) {
    VLOG(3) << "Space " << spaceId << " not found!";
    return Status::SpaceNotFound();
  }
  auto iter = spaceIt->second->listeners_.find(host);
  if (iter == spaceIt->second->listeners_.end()) {
    VLOG(3) << "Space " << spaceId << ", Listener on host " << host.toString() << " not found!";
    return Status::ListenerNotFound();
  } else {
    return iter->second;
  }
}

StatusOr<ListenersMap> MetaClient::getListenersByHostFromCache(const HostAddr& host) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  return doGetListenersMap(host, localCache_);
}

ListenersMap MetaClient::doGetListenersMap(const HostAddr& host, const LocalCache& localCache) {
  ListenersMap listenersMap;
  for (const auto& space : localCache) {
    auto spaceId = space.first;
    for (const auto& listener : space.second->listeners_) {
      if (host != listener.first) {
        continue;
      }
      for (const auto& part : listener.second) {
        auto partId = part.first;
        auto type = part.second;
        auto partIter = space.second->partsAlloc_.find(partId);
        if (partIter != space.second->partsAlloc_.end()) {
          auto peers = partIter->second;
          listenersMap[spaceId][partId].emplace_back(std::move(type), std::move(peers));
        } else {
          FLOG_WARN("%s has listener of [%d, %d], but can't find part peers",
                    host.toString().c_str(),
                    spaceId,
                    partId);
        }
      }
    }
  }
  return listenersMap;
}

StatusOr<HostAddr> MetaClient::getListenerHostsBySpacePartType(GraphSpaceID spaceId,
                                                               PartitionID partId,
                                                               cpp2::ListenerType type) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceIt = localCache_.find(spaceId);
  if (spaceIt == localCache_.end()) {
    VLOG(3) << "Space " << spaceId << " not found!";
    return Status::SpaceNotFound();
  }
  for (const auto& host : spaceIt->second->listeners_) {
    for (const auto& l : host.second) {
      if (l.first == partId && l.second == type) {
        return host.first;
      }
    }
  }
  return Status::ListenerNotFound();
}

StatusOr<std::vector<RemoteListenerInfo>> MetaClient::getListenerHostTypeBySpacePartType(
    GraphSpaceID spaceId, PartitionID partId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  auto spaceIt = localCache_.find(spaceId);
  if (spaceIt == localCache_.end()) {
    VLOG(3) << "Space " << spaceId << " not found!";
    return Status::SpaceNotFound();
  }
  std::vector<RemoteListenerInfo> items;
  for (const auto& host : spaceIt->second->listeners_) {
    for (const auto& l : host.second) {
      if (l.first == partId) {
        items.emplace_back(std::make_pair(host.first, l.second));
      }
    }
  }
  if (items.empty()) {
    return Status::ListenerNotFound();
  }
  return items;
}

bool MetaClient::loadCfg() {
  if (!configReady_ && !registerCfg()) {
    return false;
  }
  // only load current module's config is enough
  auto ret = listConfigs(gflagsModule_).get();
  if (ret.ok()) {
    // if we load config from meta server successfully, update gflags and set configReady_
    auto items = ret.value();
    MetaConfigMap metaConfigMap;
    for (auto& item : items) {
      std::pair<cpp2::ConfigModule, std::string> key = {item.get_module(), item.get_name()};
      metaConfigMap.emplace(std::move(key), std::move(item));
    }
    {
      // For any configurations that is in meta, update in cache to replace previous value
      folly::RWSpinLock::WriteHolder holder(configCacheLock_);
      for (const auto& entry : metaConfigMap) {
        auto& key = entry.first;
        auto it = metaConfigMap_.find(key);
        if (it == metaConfigMap_.end() ||
            metaConfigMap[key].get_value() != it->second.get_value()) {
          updateGflagsValue(entry.second);
          metaConfigMap_[key] = entry.second;
        }
      }
    }
  } else {
    LOG(ERROR) << "Load configs failed: " << ret.status();
    return false;
  }
  return true;
}

void MetaClient::updateGflagsValue(const cpp2::ConfigItem& item) {
  if (item.get_mode() != cpp2::ConfigMode::MUTABLE) {
    return;
  }
  auto value = item.get_value();
  std::string curValue;
  if (!gflags::GetCommandLineOption(item.get_name().c_str(), &curValue)) {
    return;
  } else {
    auto gflagValue = GflagsManager::gflagsValueToValue(value.typeName(), curValue);
    if (gflagValue != value) {
      auto valueStr = GflagsManager::ValueToGflagString(value);
      if (value.isMap() && value.getMap().kvs.empty()) {
        // Be compatible with previous configuration
        valueStr = "{}";
      }
      gflags::SetCommandLineOption(item.get_name().c_str(), valueStr.c_str());
      // TODO: we simply judge the rocksdb by nested type for now
      if (listener_ != nullptr && value.isMap()) {
        updateNestedGflags(value.getMap().kvs);
      }
      LOG(INFO) << "Update config " << item.get_name() << " from " << curValue << " to "
                << valueStr;
    }
  }
}

void MetaClient::updateNestedGflags(const std::unordered_map<std::string, Value>& nameValues) {
  std::unordered_map<std::string, std::string> optionMap;
  for (const auto& value : nameValues) {
    optionMap.emplace(value.first, value.second.toString());
  }

  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  for (const auto& spaceEntry : localCache_) {
    listener_->onSpaceOptionUpdated(spaceEntry.first, optionMap);
  }
}

Status MetaClient::refreshCache() {
  auto ret = bgThread_->addTask(&MetaClient::loadData, this).get();
  return ret ? Status::OK() : Status::Error("Load data failed");
}

void MetaClient::loadLeader(const std::vector<cpp2::HostItem>& hostItems,
                            const SpaceNameIdMap& spaceIndexByName) {
  LeaderInfo leaderInfo;
  for (auto& item : hostItems) {
    for (auto& spaceEntry : item.get_leader_parts()) {
      auto spaceName = spaceEntry.first;
      // ready_ is still false, can't read from cache
      auto iter = spaceIndexByName.find(spaceName);
      if (iter == spaceIndexByName.end()) {
        continue;
      }
      auto spaceId = iter->second;
      for (const auto& partId : spaceEntry.second) {
        leaderInfo.leaderMap_[{spaceId, partId}] = item.get_hostAddr();
        auto partHosts = getPartHostsFromCache(spaceId, partId);
        size_t leaderIndex = 0;
        if (partHosts.ok()) {
          const auto& peers = partHosts.value().hosts_;
          for (size_t i = 0; i < peers.size(); i++) {
            if (peers[i] == item.get_hostAddr()) {
              leaderIndex = i;
              break;
            }
          }
        }
        leaderInfo.pickedIndex_[{spaceId, partId}] = leaderIndex;
      }
    }
    LOG(INFO) << "Load leader of " << item.get_hostAddr() << " in "
              << item.get_leader_parts().size() << " space";
  }
  {
    // todo(doodle): in worst case, storage and meta isolated, so graph may get a outdate
    // leader info. The problem could be solved if leader term are cached as well.
    LOG(INFO) << "Load leader ok";
    folly::RWSpinLock::WriteHolder wh(leadersLock_);
    leadersInfo_ = std::move(leaderInfo);
  }
}

folly::Future<StatusOr<bool>> MetaClient::addZone(std::string zoneName,
                                                  std::vector<HostAddr> nodes) {
  cpp2::AddZoneReq req;
  req.set_zone_name(std::move(zoneName));
  req.set_nodes(std::move(nodes));

  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_addZone(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::dropZone(std::string zoneName) {
  cpp2::DropZoneReq req;
  req.set_zone_name(std::move(zoneName));

  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_dropZone(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::addHostIntoZone(HostAddr node, std::string zoneName) {
  cpp2::AddHostIntoZoneReq req;
  req.set_node(node);
  req.set_zone_name(zoneName);

  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_addHostIntoZone(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::dropHostFromZone(HostAddr node, std::string zoneName) {
  cpp2::DropHostFromZoneReq req;
  req.set_node(node);
  req.set_zone_name(zoneName);

  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_dropHostFromZone(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<HostAddr>>> MetaClient::getZone(std::string zoneName) {
  cpp2::GetZoneReq req;
  req.set_zone_name(std::move(zoneName));

  folly::Promise<StatusOr<std::vector<HostAddr>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_getZone(request); },
      [](cpp2::GetZoneResp&& resp) -> decltype(auto) { return resp.get_hosts(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::Zone>>> MetaClient::listZones() {
  cpp2::ListZonesReq req;
  folly::Promise<StatusOr<std::vector<cpp2::Zone>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listZones(request); },
      [](cpp2::ListZonesResp&& resp) -> decltype(auto) { return resp.get_zones(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::addGroup(std::string groupName,
                                                   std::vector<std::string> zoneNames) {
  cpp2::AddGroupReq req;
  req.set_group_name(std::move(groupName));
  req.set_zone_names(std::move(zoneNames));

  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_addGroup(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::dropGroup(std::string groupName) {
  cpp2::DropGroupReq req;
  req.set_group_name(std::move(groupName));

  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_dropGroup(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::addZoneIntoGroup(std::string zoneName,
                                                           std::string groupName) {
  cpp2::AddZoneIntoGroupReq req;
  req.set_zone_name(zoneName);
  req.set_group_name(groupName);

  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_addZoneIntoGroup(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::dropZoneFromGroup(std::string zoneName,
                                                            std::string groupName) {
  cpp2::DropZoneFromGroupReq req;
  req.set_zone_name(zoneName);
  req.set_group_name(groupName);

  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_dropZoneFromGroup(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<std::string>>> MetaClient::getGroup(std::string groupName) {
  cpp2::GetGroupReq req;
  req.set_group_name(std::move(groupName));

  folly::Promise<StatusOr<std::vector<std::string>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_getGroup(request); },
      [](cpp2::GetGroupResp&& resp) -> decltype(auto) { return resp.get_zone_names(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::Group>>> MetaClient::listGroups() {
  cpp2::ListGroupsReq req;
  folly::Promise<StatusOr<std::vector<cpp2::Group>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listGroups(request); },
      [](cpp2::ListGroupsResp&& resp) -> decltype(auto) { return resp.get_groups(); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<cpp2::StatsItem>> MetaClient::getStats(GraphSpaceID spaceId) {
  cpp2::GetStatsReq req;
  req.set_space_id(spaceId);
  folly::Promise<StatusOr<cpp2::StatsItem>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_getStats(request); },
      [](cpp2::GetStatsResp&& resp) -> cpp2::StatsItem { return std::move(resp).get_stats(); },
      std::move(promise),
      true);
  return future;
}

folly::Future<StatusOr<nebula::cpp2::ErrorCode>> MetaClient::reportTaskFinish(
    int32_t jobId,
    int32_t taskId,
    nebula::cpp2::ErrorCode taskErrCode,
    cpp2::StatsItem* statisticItem) {
  cpp2::ReportTaskReq req;
  req.set_code(taskErrCode);
  req.set_job_id(jobId);
  req.set_task_id(taskId);
  if (statisticItem) {
    req.set_stats(*statisticItem);
  }
  folly::Promise<StatusOr<nebula::cpp2::ErrorCode>> pro;
  auto fut = pro.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_reportTaskFinish(request); },
      [](cpp2::ExecResp&& resp) -> nebula::cpp2::ErrorCode { return resp.get_code(); },
      std::move(pro),
      true);
  return fut;
}

folly::Future<StatusOr<bool>> MetaClient::signInFTService(
    cpp2::FTServiceType type, const std::vector<cpp2::FTClient>& clients) {
  cpp2::SignInFTServiceReq req;
  req.set_type(type);
  req.set_clients(clients);
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_signInFTService(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise),
      true);
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::signOutFTService() {
  cpp2::SignOutFTServiceReq req;
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_signOutFTService(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise),
      true);
  return future;
}

folly::Future<StatusOr<std::vector<cpp2::FTClient>>> MetaClient::listFTClients() {
  cpp2::ListFTClientsReq req;
  folly::Promise<StatusOr<std::vector<cpp2::FTClient>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listFTClients(request); },
      [](cpp2::ListFTClientsResp&& resp) -> decltype(auto) {
        return std::move(resp).get_clients();
      },
      std::move(promise));
  return future;
}

StatusOr<std::vector<cpp2::FTClient>> MetaClient::getFTClientsFromCache() {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  return fulltextClientList_;
}

folly::Future<StatusOr<bool>> MetaClient::createFTIndex(const std::string& name,
                                                        const cpp2::FTIndex& index) {
  cpp2::CreateFTIndexReq req;
  req.set_fulltext_index_name(name);
  req.set_index(index);
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_createFTIndex(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise),
      true);
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::dropFTIndex(GraphSpaceID spaceId,
                                                      const std::string& name) {
  cpp2::DropFTIndexReq req;
  req.set_fulltext_index_name(name);
  req.set_space_id(spaceId);
  folly::Promise<StatusOr<bool>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_dropFTIndex(request); },
      [](cpp2::ExecResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(promise),
      true);
  return future;
}

folly::Future<StatusOr<std::unordered_map<std::string, cpp2::FTIndex>>>
MetaClient::listFTIndexes() {
  cpp2::ListFTIndexesReq req;
  folly::Promise<StatusOr<std::unordered_map<std::string, cpp2::FTIndex>>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listFTIndexes(request); },
      [](cpp2::ListFTIndexesResp&& resp) -> decltype(auto) {
        return std::move(resp).get_indexes();
      },
      std::move(promise));
  return future;
}

StatusOr<std::unordered_map<std::string, cpp2::FTIndex>> MetaClient::getFTIndexesFromCache() {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  return fulltextIndexMap_;
}

StatusOr<std::unordered_map<std::string, cpp2::FTIndex>> MetaClient::getFTIndexBySpaceFromCache(
    GraphSpaceID spaceId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  std::unordered_map<std::string, cpp2::FTIndex> indexes;
  for (auto it = fulltextIndexMap_.begin(); it != fulltextIndexMap_.end(); ++it) {
    if (it->second.get_space_id() == spaceId) {
      indexes[it->first] = it->second;
    }
  }
  return indexes;
}

StatusOr<std::pair<std::string, cpp2::FTIndex>> MetaClient::getFTIndexBySpaceSchemaFromCache(
    GraphSpaceID spaceId, int32_t schemaId) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  for (auto it = fulltextIndexMap_.begin(); it != fulltextIndexMap_.end(); ++it) {
    auto id = it->second.get_depend_schema().getType() == nebula::cpp2::SchemaID::Type::edge_type
                  ? it->second.get_depend_schema().get_edge_type()
                  : it->second.get_depend_schema().get_tag_id();
    if (it->second.get_space_id() == spaceId && id == schemaId) {
      return std::make_pair(it->first, it->second);
    }
  }
  return Status::IndexNotFound();
}

StatusOr<cpp2::FTIndex> MetaClient::getFTIndexByNameFromCache(GraphSpaceID spaceId,
                                                              const std::string& name) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::RWSpinLock::ReadHolder holder(localCacheLock_);
  if (fulltextIndexMap_.find(name) != fulltextIndexMap_.end() &&
      fulltextIndexMap_[name].get_space_id() != spaceId) {
    return Status::IndexNotFound();
  }
  return fulltextIndexMap_[name];
}

folly::Future<StatusOr<cpp2::CreateSessionResp>> MetaClient::createSession(
    const std::string& userName, const HostAddr& graphAddr, const std::string& clientIp) {
  cpp2::CreateSessionReq req;
  req.set_user(userName);
  req.set_graph_addr(graphAddr);
  req.set_client_ip(clientIp);
  folly::Promise<StatusOr<cpp2::CreateSessionResp>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_createSession(request); },
      [](cpp2::CreateSessionResp&& resp) -> decltype(auto) { return std::move(resp); },
      std::move(promise),
      true);
  return future;
}

folly::Future<StatusOr<cpp2::UpdateSessionsResp>> MetaClient::updateSessions(
    const std::vector<cpp2::Session>& sessions) {
  cpp2::UpdateSessionsReq req;
  req.set_sessions(sessions);
  folly::Promise<StatusOr<cpp2::UpdateSessionsResp>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_updateSessions(request); },
      [](cpp2::UpdateSessionsResp&& resp) -> decltype(auto) { return std::move(resp); },
      std::move(promise),
      true);
  return future;
}

folly::Future<StatusOr<cpp2::ListSessionsResp>> MetaClient::listSessions() {
  cpp2::ListSessionsReq req;
  folly::Promise<StatusOr<cpp2::ListSessionsResp>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_listSessions(request); },
      [](cpp2::ListSessionsResp&& resp) -> decltype(auto) { return std::move(resp); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<cpp2::GetSessionResp>> MetaClient::getSession(SessionID sessionId) {
  cpp2::GetSessionReq req;
  req.set_session_id(sessionId);
  folly::Promise<StatusOr<cpp2::GetSessionResp>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_getSession(request); },
      [](cpp2::GetSessionResp&& resp) -> decltype(auto) { return std::move(resp); },
      std::move(promise));
  return future;
}

folly::Future<StatusOr<cpp2::ExecResp>> MetaClient::removeSession(SessionID sessionId) {
  cpp2::RemoveSessionReq req;
  req.set_session_id(sessionId);
  folly::Promise<StatusOr<cpp2::ExecResp>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_removeSession(request); },
      [](cpp2::ExecResp&& resp) -> decltype(auto) { return std::move(resp); },
      std::move(promise),
      true);
  return future;
}

folly::Future<StatusOr<cpp2::ExecResp>> MetaClient::killQuery(
    std::unordered_map<SessionID, std::unordered_set<ExecutionPlanID>> killQueries) {
  cpp2::KillQueryReq req;
  req.set_kill_queries(std::move(killQueries));
  folly::Promise<StatusOr<cpp2::ExecResp>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_killQuery(request); },
      [](cpp2::ExecResp&& resp) -> decltype(auto) { return std::move(resp); },
      std::move(promise),
      true);
  return future;
}

folly::Future<StatusOr<bool>> MetaClient::download(const std::string& hdfsHost,
                                                   int32_t hdfsPort,
                                                   const std::string& hdfsPath,
                                                   GraphSpaceID spaceId) {
  auto url = folly::stringPrintf("http://%s:%d/download-dispatch?host=%s&port=%d&path=%s&space=%d",
                                 leader_.host.c_str(),
                                 FLAGS_ws_meta_http_port,
                                 hdfsHost.c_str(),
                                 hdfsPort,
                                 hdfsPath.c_str(),
                                 spaceId);
  auto func = [url] {
    auto result = http::HttpClient::get(url);
    if (result.ok() && result.value() == "SSTFile dispatch successfully") {
      LOG(INFO) << "Download Successfully";
      return true;
    } else {
      LOG(ERROR) << "Download Failed: " << result.value();
      return false;
    }
  };
  return folly::async(func);
}

folly::Future<StatusOr<bool>> MetaClient::ingest(GraphSpaceID spaceId) {
  auto url = folly::stringPrintf("http://%s:%d/ingest-dispatch?space=%d",
                                 leader_.host.c_str(),
                                 FLAGS_ws_meta_http_port,
                                 spaceId);
  auto func = [url] {
    auto result = http::HttpClient::get(url);
    if (result.ok() && result.value() == "SSTFile ingest successfully") {
      LOG(INFO) << "Ingest Successfully";
      return true;
    } else {
      LOG(ERROR) << "Ingest Failed";
      return false;
    }
  };
  return folly::async(func);
}

bool MetaClient::loadSessions() {
  auto session_list = listSessions().get();
  if (!session_list.ok()) {
    LOG(ERROR) << "List sessions failed, status:" << session_list.status();
    return false;
  }
  SessionMap* oldSessionMap = sessionMap_.load();
  SessionMap* newSessionMap = new SessionMap(*oldSessionMap);
  auto oldKilledPlan = killedPlans_.load();
  auto newKilledPlan = new folly::F14FastSet<std::pair<SessionID, ExecutionPlanID>>(*oldKilledPlan);
  for (auto& session : session_list.value().get_sessions()) {
    (*newSessionMap)[session.get_session_id()] = session;
    for (auto& query : session.get_queries()) {
      if (query.second.get_status() == cpp2::QueryStatus::KILLING) {
        newKilledPlan->insert({session.get_session_id(), query.first});
      }
    }
  }
  sessionMap_.store(newSessionMap);
  killedPlans_.store(newKilledPlan);
  folly::rcu_retire(oldKilledPlan);
  folly::rcu_retire(oldSessionMap);
  return true;
}

StatusOr<cpp2::Session> MetaClient::getSessionFromCache(const nebula::SessionID& session_id) {
  if (!ready_) {
    return Status::Error("Not ready!");
  }
  folly::rcu_reader guard;
  auto session_map = sessionMap_.load();
  auto it = session_map->find(session_id);
  if (it != session_map->end()) {
    return it->second;
  }
  return Status::SessionNotFound();
}

bool MetaClient::checkIsPlanKilled(SessionID sessionId, ExecutionPlanID planId) {
  static thread_local int check_counter = 0;
  // Inaccurate in a multi-threaded environment, but it is not important
  check_counter = (check_counter + 1) & ((1 << FLAGS_check_plan_killed_frequency) - 1);
  if (check_counter != 0) {
    return false;
  }
  folly::rcu_reader guard;
  return killedPlans_.load()->count({sessionId, planId});
}

Status MetaClient::verifyVersion() {
  auto req = cpp2::VerifyClientVersionReq();
  folly::Promise<StatusOr<cpp2::VerifyClientVersionResp>> promise;
  auto future = promise.getFuture();
  getResponse(
      std::move(req),
      [](auto client, auto request) { return client->future_verifyClientVersion(request); },
      [](cpp2::VerifyClientVersionResp&& resp) { return std::move(resp); },
      std::move(promise));

  auto respStatus = std::move(future).get();
  if (!respStatus.ok()) {
    return respStatus.status();
  }
  auto resp = std::move(respStatus).value();
  if (resp.get_code() != nebula::cpp2::ErrorCode::SUCCEEDED) {
    return Status::Error("Client verified failed: %s", resp.get_error_msg()->c_str());
  }
  return Status::OK();
}
}  // namespace meta
}  // namespace nebula
