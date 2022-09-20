/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_META_METACLIENT_H_
#define CLIENTS_META_METACLIENT_H_

#include <folly/SharedMutex.h>
#include <folly/concurrency/ConcurrentHashMap.h>
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/synchronization/Rcu.h>
#include <gtest/gtest_prod.h>

#include <atomic>
#include <cstdint>

#include "common/base/Base.h"
#include "common/base/ObjectPool.h"
#include "common/base/StatusOr.h"
#include "common/meta/Common.h"
#include "common/meta/GflagsManager.h"
#include "common/meta/NebulaSchemaProvider.h"
#include "common/thread/GenericWorker.h"
#include "common/thrift/ThriftClientManager.h"
#include "interface/gen-cpp2/MetaServiceAsyncClient.h"
#include "interface/gen-cpp2/common_types.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/DiskManager.h"

DECLARE_int32(meta_client_retry_times);
DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace storage {
class MetaClientTestUpdater;
}  // namespace storage
}  // namespace nebula

namespace nebula {
namespace meta {

using PartsAlloc = std::unordered_map<PartitionID, std::vector<HostAddr>>;
using SpaceIdName = std::pair<GraphSpaceID, std::string>;
using HostStatus = std::pair<HostAddr, std::string>;

// struct for in cache
// the different version of tag schemas, from oldest to latest
using TagSchemas =
    std::unordered_map<TagID, std::vector<std::shared_ptr<const NebulaSchemaProvider>>>;

// Mapping of tagId and a *single* tag schema
using TagSchema = std::unordered_map<TagID, std::shared_ptr<const NebulaSchemaProvider>>;

// the different version of edge schema, from oldest to latest
using EdgeSchemas =
    std::unordered_map<EdgeType, std::vector<std::shared_ptr<const NebulaSchemaProvider>>>;

// Mapping of edgeType and  a *single* edge schema
using EdgeSchema = std::unordered_map<EdgeType, std::shared_ptr<const NebulaSchemaProvider>>;

// Space and index Name => IndexID
// Get IndexID via space ID and index name
using NameIndexMap = std::unordered_map<std::pair<GraphSpaceID, std::string>, IndexID>;

// Index ID => Index Item
// Get Index Structure by indexID
using Indexes = std::unordered_map<IndexID, std::shared_ptr<cpp2::IndexItem>>;

// Listeners is a map of ListenerHost => <PartId + type>, used to add/remove listener on local host
using Listeners =
    std::unordered_map<HostAddr, std::vector<std::pair<PartitionID, cpp2::ListenerType>>>;

// Get services
using ServiceClientsList =
    std::unordered_map<cpp2::ExternalServiceType, std::vector<cpp2::ServiceClient>>;

struct SpaceInfoCache {
  cpp2::SpaceDesc spaceDesc_;
  PartsAlloc partsAlloc_;
  std::unordered_map<HostAddr, std::vector<PartitionID>> partsOnHost_;
  std::vector<cpp2::TagItem> tagItemVec_;
  TagSchemas tagSchemas_;
  std::vector<cpp2::EdgeItem> edgeItemVec_;
  EdgeSchemas edgeSchemas_;
  std::vector<cpp2::IndexItem> tagIndexItemVec_;
  Indexes tagIndexes_;
  std::vector<cpp2::IndexItem> edgeIndexItemVec_;
  Indexes edgeIndexes_;
  Listeners listeners_;
  std::unordered_map<PartitionID, TermID> termOfPartition_;

  SpaceInfoCache() = default;
  SpaceInfoCache(const SpaceInfoCache& info)
      : spaceDesc_(info.spaceDesc_),
        partsAlloc_(info.partsAlloc_),
        partsOnHost_(info.partsOnHost_),
        tagItemVec_(info.tagItemVec_),
        tagSchemas_(info.tagSchemas_),
        edgeItemVec_(info.edgeItemVec_),
        edgeSchemas_(info.edgeSchemas_),
        tagIndexItemVec_(info.tagIndexItemVec_),
        tagIndexes_(info.tagIndexes_),
        edgeIndexItemVec_(info.edgeIndexItemVec_),
        edgeIndexes_(info.edgeIndexes_),
        listeners_(info.listeners_),
        termOfPartition_(info.termOfPartition_) {}

  ~SpaceInfoCache() = default;
};

using LocalCache = std::unordered_map<GraphSpaceID, std::shared_ptr<SpaceInfoCache>>;

using SpaceNameIdMap = std::unordered_map<std::string, GraphSpaceID>;
// get tagID via spaceId and tagName
using SpaceTagNameIdMap = std::unordered_map<std::pair<GraphSpaceID, std::string>, TagID>;
// get edgeType via spaceId and edgeName
using SpaceEdgeNameTypeMap = std::unordered_map<std::pair<GraphSpaceID, std::string>, EdgeType>;
// get tagName via spaceId and tagId
using SpaceTagIdNameMap = std::unordered_map<std::pair<GraphSpaceID, TagID>, std::string>;
// get latest tag version via spaceId and TagID
using SpaceNewestTagVerMap = std::unordered_map<std::pair<GraphSpaceID, TagID>, SchemaVer>;
// get latest edge version via spaceId and edgeType
using SpaceNewestEdgeVerMap = std::unordered_map<std::pair<GraphSpaceID, EdgeType>, SchemaVer>;
// get edgeName via spaceId and edgeType
using SpaceEdgeTypeNameMap = std::unordered_map<std::pair<GraphSpaceID, EdgeType>, std::string>;
// get all edgeType edgeName via spaceId
using SpaceAllEdgeMap = std::unordered_map<GraphSpaceID, std::vector<std::string>>;

struct LeaderInfo {
  // get leader host via spaceId and partId
  std::unordered_map<std::pair<GraphSpaceID, PartitionID>, HostAddr> leaderMap_;
  // index of picked host in all peers
  std::unordered_map<std::pair<GraphSpaceID, PartitionID>, size_t> pickedIndex_;
};

using IndexStatus = std::tuple<std::string, std::string, std::string>;

// get user roles by account
using UserRolesMap = std::unordered_map<std::string, std::vector<cpp2::RoleItem>>;
// get user password by account
using UserPasswordMap = std::unordered_map<std::string, std::string>;
// Mapping of user name and remaining wrong password attempts
using UserPasswordAttemptsRemain = folly::ConcurrentHashMap<std::string, uint32>;
// Mapping of user name and the timestamp when the account is locked
using UserLoginLockTime = folly::ConcurrentHashMap<std::string, uint32>;

// config cache, get config via module and name
using MetaConfigMap =
    std::unordered_map<std::pair<cpp2::ConfigModule, std::string>, cpp2::ConfigItem>;

using FTIndexMap = std::unordered_map<std::string, cpp2::FTIndex>;

using SessionMap = std::unordered_map<SessionID, cpp2::Session>;

class MetaChangedListener {
 public:
  virtual ~MetaChangedListener() = default;

  virtual void onSpaceAdded(GraphSpaceID spaceId, bool isListener = false) = 0;
  virtual void onSpaceRemoved(GraphSpaceID spaceId, bool isListener = false) = 0;
  virtual void onSpaceOptionUpdated(
      GraphSpaceID spaceId, const std::unordered_map<std::string, std::string>& options) = 0;
  virtual void onPartAdded(const PartHosts& partHosts) = 0;
  virtual void onPartRemoved(GraphSpaceID spaceId, PartitionID partId) = 0;
  virtual void onPartUpdated(const PartHosts& partHosts) = 0;
  virtual void fetchLeaderInfo(
      std::unordered_map<GraphSpaceID, std::vector<cpp2::LeaderInfo>>& leaders) = 0;
  virtual void fetchDiskParts(kvstore::SpaceDiskPartsMap& diskParts) = 0;
  virtual void onListenerAdded(GraphSpaceID spaceId,
                               PartitionID partId,
                               const ListenerHosts& listenerHosts) = 0;
  virtual void onListenerRemoved(GraphSpaceID spaceId,
                                 PartitionID partId,
                                 cpp2::ListenerType type) = 0;
  virtual void onCheckRemoteListeners(GraphSpaceID spaceId,
                                      PartitionID partId,
                                      const std::vector<HostAddr>& remoteListeners) = 0;
};

struct MetaClientOptions {
  MetaClientOptions() = default;
  MetaClientOptions(const MetaClientOptions& opt)
      : localHost_(opt.localHost_),
        clusterId_(opt.clusterId_.load()),
        serviceName_(opt.serviceName_),
        skipConfig_(opt.skipConfig_),
        role_(opt.role_),
        gitInfoSHA_(opt.gitInfoSHA_),
        dataPaths_(opt.dataPaths_),
        rootPath_(opt.rootPath_) {}

  // Current host address
  HostAddr localHost_{"", 0};
  // Current cluster Id, it is required by storaged only.
  std::atomic<ClusterID> clusterId_{0};
  // Current service name, used in StatsManager
  std::string serviceName_ = "";
  // Whether to skip the config manager
  bool skipConfig_ = false;
  // Host role(graph/meta/storage) using this client, and UNKNOWN role will not send heartbeat, used
  // for tools such as upgrader
  cpp2::HostRole role_ = cpp2::HostRole::UNKNOWN;
  // gitInfoSHA of Host using this client
  std::string gitInfoSHA_{""};
  // Data path list, used in storaged
  std::vector<std::string> dataPaths_;
  // Install path, used in metad/graphd/storaged
  std::string rootPath_;
};

class BaseMetaClient {
 public:
  virtual folly::Future<StatusOr<int64_t>> getSegmentId(int64_t length) = 0;

  virtual ~BaseMetaClient() = default;
};

class MetaClient : public BaseMetaClient {
  FRIEND_TEST(ConfigManTest, MetaConfigManTest);
  FRIEND_TEST(ConfigManTest, MockConfigTest);
  FRIEND_TEST(ConfigManTest, RocksdbOptionsTest);
  FRIEND_TEST(MetaClientTest, SimpleTest);
  FRIEND_TEST(MetaClientTest, RetryWithExceptionTest);
  FRIEND_TEST(MetaClientTest, RetryOnceTest);
  FRIEND_TEST(MetaClientTest, RetryUntilLimitTest);
  FRIEND_TEST(MetaClientTest, RocksdbOptionsTest);
  FRIEND_TEST(MetaClientTest, VerifyClientTest);
  FRIEND_TEST(ChainAddEdgesTest, AddEdgesLocalTest);
  friend class KillQueryMetaWrapper;
  friend class storage::MetaClientTestUpdater;

 public:
  MetaClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
             std::vector<HostAddr> addrs,
             const MetaClientOptions& options = MetaClientOptions());

  ~MetaClient() override;

#ifdef BUILD_STANDALONE
  StatusOr<bool> checkLocalMachineRegistered();
#endif

  bool isMetadReady();

  bool waitForMetadReady(int count = -1, int retryIntervalSecs = FLAGS_heartbeat_interval_secs);

  void notifyStop();

  void stop();

  void registerListener(MetaChangedListener* listener) {
    folly::SharedMutex::WriteHolder holder(listenerLock_);
    CHECK(listener_ == nullptr);
    listener_ = listener;
  }

  void unRegisterListener() {
    folly::SharedMutex::WriteHolder holder(listenerLock_);
    listener_ = nullptr;
  }

  folly::Future<StatusOr<cpp2::AdminJobResult>> submitJob(GraphSpaceID spaceId,
                                                          cpp2::JobOp op,
                                                          cpp2::JobType type,
                                                          std::vector<std::string> paras);

  // Operations for parts
  folly::Future<StatusOr<GraphSpaceID>> createSpace(meta::cpp2::SpaceDesc spaceDesc,
                                                    bool ifNotExists = false);

  folly::Future<StatusOr<GraphSpaceID>> createSpaceAs(const std::string& oldSpaceName,
                                                      const std::string& newSpaceName);

  folly::Future<StatusOr<std::vector<SpaceIdName>>> listSpaces();

  folly::Future<StatusOr<cpp2::SpaceItem>> getSpace(std::string name);

  folly::Future<StatusOr<bool>> dropSpace(std::string name, bool ifExists = false);

  // clear space data, but keep the space schema.
  folly::Future<StatusOr<bool>> clearSpace(std::string name, bool ifExists = false);

  folly::Future<StatusOr<std::vector<cpp2::HostItem>>> listHosts(
      cpp2::ListHostType type = cpp2::ListHostType::ALLOC);

  folly::Future<StatusOr<bool>> alterSpace(const std::string& spaceName,
                                           meta::cpp2::AlterSpaceOp op,
                                           const std::vector<std::string>& paras);

  folly::Future<StatusOr<std::vector<cpp2::PartItem>>> listParts(GraphSpaceID spaceId,
                                                                 std::vector<PartitionID> partIds);

  using PartTerms = std::unordered_map<PartitionID, TermID>;
  folly::Future<StatusOr<PartsAlloc>> getPartsAlloc(GraphSpaceID spaceId,
                                                    MetaClient::PartTerms* partTerms = nullptr);

  // Operations for schema
  folly::Future<StatusOr<TagID>> createTagSchema(GraphSpaceID spaceId,
                                                 std::string name,
                                                 cpp2::Schema schema,
                                                 bool ifNotExists = false);

  folly::Future<StatusOr<bool>> alterTagSchema(GraphSpaceID spaceId,
                                               std::string name,
                                               std::vector<cpp2::AlterSchemaItem> items,
                                               cpp2::SchemaProp schemaProp);

  folly::Future<StatusOr<std::vector<cpp2::TagItem>>> listTagSchemas(GraphSpaceID spaceId);

  folly::Future<StatusOr<bool>> dropTagSchema(GraphSpaceID spaceId,
                                              std::string name,
                                              bool ifExists = false);

  // Return the latest schema when ver = -1
  folly::Future<StatusOr<cpp2::Schema>> getTagSchema(GraphSpaceID spaceId,
                                                     std::string name,
                                                     SchemaVer version = -1);

  folly::Future<StatusOr<EdgeType>> createEdgeSchema(GraphSpaceID spaceId,
                                                     std::string name,
                                                     cpp2::Schema schema,
                                                     bool ifNotExists = false);

  folly::Future<StatusOr<bool>> alterEdgeSchema(GraphSpaceID spaceId,
                                                std::string name,
                                                std::vector<cpp2::AlterSchemaItem> items,
                                                cpp2::SchemaProp schemaProp);

  folly::Future<StatusOr<std::vector<cpp2::EdgeItem>>> listEdgeSchemas(GraphSpaceID spaceId);

  // Return the latest schema when ver = -1
  folly::Future<StatusOr<cpp2::Schema>> getEdgeSchema(GraphSpaceID spaceId,
                                                      std::string name,
                                                      SchemaVer version = -1);

  folly::Future<StatusOr<bool>> dropEdgeSchema(GraphSpaceID spaceId,
                                               std::string name,
                                               bool ifExists = false);

  // Operations for index
  folly::Future<StatusOr<IndexID>> createTagIndex(
      GraphSpaceID spaceID,
      std::string indexName,
      std::string tagName,
      std::vector<cpp2::IndexFieldDef> fields,
      bool ifNotExists = false,
      const meta::cpp2::IndexParams* indexParams = nullptr,
      const std::string* comment = nullptr);

  // Remove the define of tag index
  folly::Future<StatusOr<bool>> dropTagIndex(GraphSpaceID spaceId,
                                             std::string name,
                                             bool ifExists = false);

  folly::Future<StatusOr<cpp2::IndexItem>> getTagIndex(GraphSpaceID spaceId, std::string name);

  folly::Future<StatusOr<std::vector<cpp2::IndexItem>>> listTagIndexes(GraphSpaceID spaceId);

  folly::Future<StatusOr<bool>> rebuildTagIndex(GraphSpaceID spaceID, std::string name);

  folly::Future<StatusOr<std::vector<cpp2::IndexStatus>>> listTagIndexStatus(GraphSpaceID spaceId);

  folly::Future<StatusOr<IndexID>> createEdgeIndex(GraphSpaceID spaceID,
                                                   std::string indexName,
                                                   std::string edgeName,
                                                   std::vector<cpp2::IndexFieldDef> fields,
                                                   bool ifNotExists = false,
                                                   const cpp2::IndexParams* indexParams = nullptr,
                                                   const std::string* comment = nullptr);

  // Remove the definition of edge index
  folly::Future<StatusOr<bool>> dropEdgeIndex(GraphSpaceID spaceId,
                                              std::string name,
                                              bool ifExists = false);

  folly::Future<StatusOr<cpp2::IndexItem>> getEdgeIndex(GraphSpaceID spaceId, std::string name);

  folly::Future<StatusOr<std::vector<cpp2::IndexItem>>> listEdgeIndexes(GraphSpaceID spaceId);

  folly::Future<StatusOr<bool>> rebuildEdgeIndex(GraphSpaceID spaceId, std::string name);

  folly::Future<StatusOr<std::vector<cpp2::IndexStatus>>> listEdgeIndexStatus(GraphSpaceID spaceId);

  // Operations for users.
  folly::Future<StatusOr<bool>> createUser(std::string account,
                                           std::string password,
                                           bool ifNotExists);

  folly::Future<StatusOr<bool>> dropUser(std::string account, bool ifExists);

  folly::Future<StatusOr<bool>> alterUser(std::string account, std::string password);

  folly::Future<StatusOr<bool>> grantToUser(cpp2::RoleItem roleItem);

  folly::Future<StatusOr<bool>> revokeFromUser(cpp2::RoleItem roleItem);

  folly::Future<StatusOr<std::unordered_map<std::string, std::string>>> listUsers();

  folly::Future<StatusOr<std::vector<cpp2::RoleItem>>> listRoles(GraphSpaceID space);

  folly::Future<StatusOr<bool>> changePassword(std::string account,
                                               std::string newPwd,
                                               std::string oldPwd);

  folly::Future<StatusOr<std::vector<cpp2::RoleItem>>> getUserRoles(std::string account);

  // Operations for config
  folly::Future<StatusOr<bool>> regConfig(const std::vector<cpp2::ConfigItem>& items);

  folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>> getConfig(const cpp2::ConfigModule& module,
                                                                   const std::string& name);

  folly::Future<StatusOr<bool>> setConfig(const cpp2::ConfigModule& module,
                                          const std::string& name,
                                          const Value& value);

  folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>> listConfigs(
      const cpp2::ConfigModule& module);

  folly::Future<StatusOr<bool>> createSnapshot();

  folly::Future<StatusOr<bool>> dropSnapshot(const std::string& name);

  folly::Future<StatusOr<std::vector<cpp2::Snapshot>>> listSnapshots();

  // Operations for listener.

  folly::Future<StatusOr<bool>> addListener(GraphSpaceID spaceId,
                                            cpp2::ListenerType type,
                                            std::vector<HostAddr> hosts);

  folly::Future<StatusOr<bool>> removeListener(GraphSpaceID spaceId, cpp2::ListenerType type);

  folly::Future<StatusOr<std::vector<cpp2::ListenerInfo>>> listListener(GraphSpaceID spaceId);

  StatusOr<std::vector<std::pair<PartitionID, cpp2::ListenerType>>>
  getListenersBySpaceHostFromCache(GraphSpaceID spaceId, const HostAddr& host);

  StatusOr<ListenersMap> getListenersByHostFromCache(const HostAddr& host);

  StatusOr<HostAddr> getListenerHostsBySpacePartType(GraphSpaceID spaceId,
                                                     PartitionID partId,
                                                     cpp2::ListenerType type);

  StatusOr<std::vector<RemoteListenerInfo>> getListenerHostTypeBySpacePartType(GraphSpaceID spaceId,
                                                                               PartitionID partId);

  // Operations for services
  folly::Future<StatusOr<bool>> signInService(const cpp2::ExternalServiceType& type,
                                              const std::vector<cpp2::ServiceClient>& clients);

  folly::Future<StatusOr<bool>> signOutService(const cpp2::ExternalServiceType& type);

  folly::Future<StatusOr<ServiceClientsList>> listServiceClients(
      const cpp2::ExternalServiceType& type);

  StatusOr<std::vector<cpp2::ServiceClient>> getServiceClientsFromCache(
      const cpp2::ExternalServiceType& type);

  // Operations for fulltext index.

  folly::Future<StatusOr<bool>> createFTIndex(const std::string& name, const cpp2::FTIndex& index);

  folly::Future<StatusOr<bool>> dropFTIndex(GraphSpaceID spaceId, const std::string& name);

  folly::Future<StatusOr<std::unordered_map<std::string, cpp2::FTIndex>>> listFTIndexes();

  StatusOr<std::unordered_map<std::string, cpp2::FTIndex>> getFTIndexesFromCache();

  StatusOr<std::unordered_map<std::string, cpp2::FTIndex>> getFTIndexBySpaceFromCache(
      GraphSpaceID spaceId);

  StatusOr<std::pair<std::string, cpp2::FTIndex>> getFTIndexBySpaceSchemaFromCache(
      GraphSpaceID spaceId, int32_t schemaId);

  StatusOr<cpp2::FTIndex> getFTIndexByNameFromCache(GraphSpaceID spaceId, const std::string& name);

  // session
  folly::Future<StatusOr<cpp2::CreateSessionResp>> createSession(const std::string& userName,
                                                                 const HostAddr& graphAddr,
                                                                 const std::string& clientIp);

  folly::Future<StatusOr<cpp2::UpdateSessionsResp>> updateSessions(
      const std::vector<cpp2::Session>& sessions);

  folly::Future<StatusOr<cpp2::ListSessionsResp>> listSessions();

  folly::Future<StatusOr<cpp2::GetSessionResp>> getSession(SessionID sessionId);

  folly::Future<StatusOr<cpp2::ExecResp>> removeSession(SessionID sessionId);

  folly::Future<StatusOr<cpp2::ExecResp>> killQuery(
      std::unordered_map<SessionID, std::unordered_set<ExecutionPlanID>> killQueries);

  // Operations for cache.
  StatusOr<GraphSpaceID> getSpaceIdByNameFromCache(const std::string& name);

  StatusOr<std::string> getSpaceNameByIdFromCache(GraphSpaceID spaceId);

  StatusOr<int32_t> getSpaceVidLen(const GraphSpaceID& space);

  StatusOr<nebula::cpp2::PropertyType> getSpaceVidType(const GraphSpaceID& space);

  StatusOr<meta::cpp2::SpaceDesc> getSpaceDesc(const GraphSpaceID& space);

  StatusOr<meta::cpp2::IsolationLevel> getIsolationLevel(GraphSpaceID spaceId);

  StatusOr<TagID> getTagIDByNameFromCache(const GraphSpaceID& space, const std::string& name);

  StatusOr<std::string> getTagNameByIdFromCache(const GraphSpaceID& space, const TagID& tagId);

  StatusOr<SchemaVer> getLatestTagVersionFromCache(const GraphSpaceID& space, const TagID& tagId);

  StatusOr<SchemaVer> getLatestEdgeVersionFromCache(const GraphSpaceID& space,
                                                    const EdgeType& edgeType);

  StatusOr<EdgeType> getEdgeTypeByNameFromCache(const GraphSpaceID& space, const std::string& name);

  StatusOr<std::string> getEdgeNameByTypeFromCache(const GraphSpaceID& space,
                                                   const EdgeType edgeType);

  // get all latest version edge
  StatusOr<std::vector<std::string>> getAllEdgeFromCache(const GraphSpaceID& space);

  PartsMap getPartsMapFromCache(const HostAddr& host);

  StatusOr<PartHosts> getPartHostsFromCache(GraphSpaceID spaceId, PartitionID partId);

  Status checkPartExistInCache(const HostAddr& host, GraphSpaceID spaceId, PartitionID partId);

  Status checkSpaceExistInCache(const HostAddr& host, GraphSpaceID spaceId);

  StatusOr<int32_t> partsNum(GraphSpaceID spaceId);

  PartitionID partId(int32_t numParts, VertexID id) const;

  StatusOr<std::shared_ptr<const NebulaSchemaProvider>> getTagSchemaFromCache(GraphSpaceID spaceId,
                                                                              TagID tagID,
                                                                              SchemaVer ver = -1);

  StatusOr<std::shared_ptr<const NebulaSchemaProvider>> getEdgeSchemaFromCache(GraphSpaceID spaceId,
                                                                               EdgeType edgeType,
                                                                               SchemaVer ver = -1);

  StatusOr<TagSchemas> getAllVerTagSchema(GraphSpaceID spaceId);

  StatusOr<TagSchema> getAllLatestVerTagSchema(const GraphSpaceID& spaceId);

  StatusOr<EdgeSchemas> getAllVerEdgeSchema(GraphSpaceID spaceId);

  StatusOr<EdgeSchema> getAllLatestVerEdgeSchemaFromCache(const GraphSpaceID& spaceId);

  StatusOr<std::shared_ptr<cpp2::IndexItem>> getTagIndexByNameFromCache(const GraphSpaceID space,
                                                                        const std::string& name);

  StatusOr<std::shared_ptr<cpp2::IndexItem>> getEdgeIndexByNameFromCache(const GraphSpaceID space,
                                                                         const std::string& name);

  StatusOr<std::shared_ptr<cpp2::IndexItem>> getTagIndexFromCache(GraphSpaceID spaceId,
                                                                  IndexID indexID);

  StatusOr<TagID> getRelatedTagIDByIndexNameFromCache(const GraphSpaceID space,
                                                      const std::string& indexName);

  StatusOr<std::shared_ptr<cpp2::IndexItem>> getEdgeIndexFromCache(GraphSpaceID spaceId,
                                                                   IndexID indexID);

  StatusOr<EdgeType> getRelatedEdgeTypeByIndexNameFromCache(const GraphSpaceID space,
                                                            const std::string& indexName);

  StatusOr<std::vector<std::shared_ptr<cpp2::IndexItem>>> getTagIndexesFromCache(
      GraphSpaceID spaceId);

  StatusOr<std::vector<std::shared_ptr<cpp2::IndexItem>>> getEdgeIndexesFromCache(
      GraphSpaceID spaceId);

  Status checkTagIndexed(GraphSpaceID space, IndexID indexID);

  Status checkEdgeIndexed(GraphSpaceID space, IndexID indexID);

  const std::vector<HostAddr>& getAddresses();

  std::vector<cpp2::RoleItem> getRolesByUserFromCache(const std::string& user);

  Status authCheckFromCache(const std::string& account, const std::string& password);

  StatusOr<TermID> getTermFromCache(GraphSpaceID spaceId, PartitionID);

  bool checkShadowAccountFromCache(const std::string& account);

  StatusOr<std::vector<HostAddr>> getStorageHosts();

  StatusOr<cpp2::Session> getSessionFromCache(const nebula::SessionID& session_id);

  bool checkIsPlanKilled(SessionID session_id, ExecutionPlanID plan_id);

  StatusOr<HostAddr> getStorageLeaderFromCache(GraphSpaceID spaceId, PartitionID partId);

  void updateStorageLeader(GraphSpaceID spaceId, PartitionID partId, const HostAddr& leader);

  void invalidStorageLeader(GraphSpaceID spaceId, PartitionID partId);

  StatusOr<LeaderInfo> getLeaderInfo();

  folly::Future<StatusOr<bool>> addHosts(std::vector<HostAddr> hosts);

  folly::Future<StatusOr<bool>> dropHosts(std::vector<HostAddr> hosts);

  folly::Future<StatusOr<bool>> mergeZone(std::vector<std::string> zones, std::string zoneName);

  folly::Future<StatusOr<bool>> divideZone(
      std::string zoneName, std::unordered_map<std::string, std::vector<HostAddr>> zoneItems);

  folly::Future<StatusOr<bool>> renameZone(std::string originalZoneName, std::string zoneName);

  folly::Future<StatusOr<bool>> dropZone(std::string zoneName);

  folly::Future<StatusOr<bool>> addHostsIntoZone(std::vector<HostAddr> hosts,
                                                 std::string zoneName,
                                                 bool isNew);

  folly::Future<StatusOr<std::vector<HostAddr>>> getZone(std::string zoneName);

  folly::Future<StatusOr<std::vector<cpp2::Zone>>> listZones();

  Status refreshCache();

  folly::Future<StatusOr<cpp2::StatsItem>> getStats(GraphSpaceID spaceId);

  folly::Future<StatusOr<nebula::cpp2::ErrorCode>> reportTaskFinish(
      GraphSpaceID spaceId,
      int32_t jobId,
      int32_t taskId,
      nebula::cpp2::ErrorCode taskErrCode,
      cpp2::StatsItem* statisticItem);

  folly::Future<StatusOr<int64_t>> getWorkerId(std::string ipAddr);

  folly::Future<StatusOr<int64_t>> getSegmentId(int64_t length) override;

  HostAddr getMetaLeader() {
    return leader_;
  }

  int64_t HeartbeatTime() {
    return heartbeatTime_;
  }

  std::string getLocalIp() {
    return options_.localHost_.toString();
  }

 protected:
  // Return true if load succeeded.
  bool loadData();
  bool loadCfg();
  void heartBeatThreadFunc();

  bool registerCfg();
  void updateGflagsValue(const cpp2::ConfigItem& item);
  void updateNestedGflags(const std::unordered_map<std::string, Value>& nameValues);

  bool loadSchemas(GraphSpaceID spaceId,
                   std::shared_ptr<SpaceInfoCache> spaceInfoCache,
                   SpaceTagNameIdMap& tagNameIdMap,
                   SpaceTagIdNameMap& tagIdNameMap,
                   SpaceEdgeNameTypeMap& edgeNameTypeMap,
                   SpaceEdgeTypeNameMap& edgeTypeNamemap,
                   SpaceNewestTagVerMap& newestTagVerMap,
                   SpaceNewestEdgeVerMap& newestEdgeVerMap,
                   SpaceAllEdgeMap& allEdgemap);

  bool loadUsersAndRoles();

  bool loadIndexes(GraphSpaceID spaceId, std::shared_ptr<SpaceInfoCache> cache);

  bool loadListeners(GraphSpaceID spaceId, std::shared_ptr<SpaceInfoCache> cache);

  bool loadGlobalServiceClients();

  bool loadFulltextIndexes();

  bool loadSessions();

  void loadLeader(const std::vector<cpp2::HostItem>& hostItems,
                  const SpaceNameIdMap& spaceIndexByName);

  folly::Future<StatusOr<bool>> heartbeat();

  std::unordered_map<HostAddr, std::vector<PartitionID>> reverse(const PartsAlloc& parts);

  void updateActive() {
    folly::SharedMutex::WriteHolder holder(hostLock_);
    active_ = addrs_[folly::Random::rand64(addrs_.size())];
  }

  void updateLeader(HostAddr leader = {"", 0}) {
    folly::SharedMutex::WriteHolder holder(hostLock_);
    if (leader != HostAddr("", 0)) {
      leader_ = leader;
    } else {
      leader_ = addrs_[folly::Random::rand64(addrs_.size())];
    }
  }

  // part diff
  void diff(const LocalCache& oldCache, const LocalCache& newCache);

  void listenerDiff(const LocalCache& oldCache, const LocalCache& newCache);

  // add remote listener as part peers
  void loadRemoteListeners();

  template <typename RESP>
  Status handleResponse(const RESP& resp);

  template <class Request,
            class RemoteFunc,
            class RespGenerator,
            class RpcResponse = typename std::result_of<RemoteFunc(
                std::shared_ptr<meta::cpp2::MetaServiceAsyncClient>, Request)>::type::value_type,
            class Response = typename std::result_of<RespGenerator(RpcResponse)>::type>
  void getResponse(Request req,
                   RemoteFunc remoteFunc,
                   RespGenerator respGen,
                   folly::Promise<StatusOr<Response>> pro,
                   bool toLeader = true,
                   int32_t retry = 0,
                   int32_t retryLimit = FLAGS_meta_client_retry_times);

  std::vector<SpaceIdName> toSpaceIdName(const std::vector<cpp2::IdName>& tIdNames);

  PartsMap doGetPartsMap(const HostAddr& host, const LocalCache& localCache);

  ListenersMap doGetListenersMap(const HostAddr& host, const LocalCache& localCache);

  // Checks if the client version is compatible with the server version by checking the
  // whilelist in meta.
  Status verifyVersion();

  // Save the version of the graph service into meta so that it could be looked up.
  // This method should be only called in the internal client.
  Status saveVersionToMeta();

 private:
  std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
  std::shared_ptr<thrift::ThriftClientManager<cpp2::MetaServiceAsyncClient>> clientsMan_;

  // heartbeat is a single thread, maybe leaderIdsLock_ and diskPartsLock_ is useless?
  // leaderIdsLock_ is used to protect leaderIds_
  std::unordered_map<GraphSpaceID, std::vector<cpp2::LeaderInfo>> leaderIds_;
  folly::SharedMutex leaderIdsLock_;
  // diskPartsLock_ is used to protect diskParts_;
  kvstore::SpaceDiskPartsMap diskParts_;
  folly::SharedMutex diskPartsLock_;

  std::atomic<int64_t> localDataLastUpdateTime_{-1};
  std::atomic<int64_t> localCfgLastUpdateTime_{-1};
  std::atomic<int64_t> metadLastUpdateTime_{0};

  int64_t metaServerVersion_{-1};
  static constexpr int64_t EXPECT_META_VERSION = 3;

  // leadersLock_ is used to protect leadersInfo
  folly::SharedMutex leadersLock_;
  LeaderInfo leadersInfo_;

  LocalCache localCache_;
  std::vector<HostAddr> addrs_;
  // The lock used to protect active_ and leader_.
  folly::SharedMutex hostLock_;
  HostAddr active_;
  HostAddr leader_;
  HostAddr localHost_;

  // Only report dir info once when started
  bool dirInfoReported_ = false;

  struct MetaData {
    int64_t localLastUpdateTime_{-2};
    LocalCache localCache_;
    SpaceNameIdMap spaceIndexByName_;
    SpaceTagNameIdMap spaceTagIndexByName_;
    SpaceEdgeNameTypeMap spaceEdgeIndexByName_;
    SpaceEdgeTypeNameMap spaceEdgeIndexByType_;
    SpaceTagIdNameMap spaceTagIndexById_;
    SpaceNewestTagVerMap spaceNewestTagVerMap_;
    SpaceNewestEdgeVerMap spaceNewestEdgeVerMap_;
    SpaceAllEdgeMap spaceAllEdgeMap_;

    UserRolesMap userRolesMap_;
    std::vector<HostAddr> storageHosts_;
    FTIndexMap fulltextIndexMap_;
    UserPasswordMap userPasswordMap_;

    SessionMap sessionMap_;
    folly::F14FastSet<std::pair<SessionID, ExecutionPlanID>> killedPlans_;

    ServiceClientsList serviceClientList_;
  };

  void addSchemaField(NebulaSchemaProvider* schema, const cpp2::ColumnDef& col);

  TagSchemas buildTagSchemas(std::vector<cpp2::TagItem> tagItemVec);
  EdgeSchemas buildEdgeSchemas(std::vector<cpp2::EdgeItem> edgeItemVec);

  std::unique_ptr<thread::GenericWorker> bgThread_;
  SpaceNameIdMap spaceIndexByName_;
  SpaceTagNameIdMap spaceTagIndexByName_;
  SpaceEdgeNameTypeMap spaceEdgeIndexByName_;
  SpaceEdgeTypeNameMap spaceEdgeIndexByType_;
  SpaceTagIdNameMap spaceTagIndexById_;
  SpaceNewestTagVerMap spaceNewestTagVerMap_;
  SpaceNewestEdgeVerMap spaceNewestEdgeVerMap_;
  SpaceAllEdgeMap spaceAllEdgeMap_;

  UserRolesMap userRolesMap_;
  UserPasswordMap userPasswordMap_;
  UserPasswordAttemptsRemain userPasswordAttemptsRemain_;
  UserLoginLockTime userLoginLockTime_;

  NameIndexMap tagNameIndexMap_;
  NameIndexMap edgeNameIndexMap_;

  // Global service client
  ServiceClientsList serviceClientList_;
  FTIndexMap fulltextIndexMap_;

  // The listener_ is the NebulaStore
  MetaChangedListener* listener_{nullptr};
  // The lock used to protect listener_
  folly::SharedMutex listenerLock_;
  std::atomic<ClusterID> clusterId_{0};
  bool isRunning_{false};
  bool sendHeartBeat_{false};
  std::atomic_bool ready_{false};
  MetaConfigMap metaConfigMap_;
  folly::SharedMutex configCacheLock_;
  cpp2::ConfigModule gflagsModule_{cpp2::ConfigModule::UNKNOWN};
  std::atomic_bool configReady_{false};
  std::vector<cpp2::ConfigItem> gflagsDeclared_;
  bool skipConfig_ = false;
  MetaClientOptions options_;
  std::vector<HostAddr> storageHosts_;
  int64_t heartbeatTime_;
  SessionMap sessionMap_;
  folly::F14FastSet<std::pair<SessionID, ExecutionPlanID>> killedPlans_;
  std::atomic<MetaData*> metadata_;
};

}  // namespace meta
}  // namespace nebula
#endif  // CLIENTS_META_METACLIENT_H_
