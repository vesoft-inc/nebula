/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METACLIENT_H_
#define META_METACLIENT_H_

#include "base/Base.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/RWSpinLock.h>
#include <gtest/gtest_prod.h>
#include "gen-cpp2/MetaServiceAsyncClient.h"
#include "base/Status.h"
#include "base/StatusOr.h"
#include "thread/GenericWorker.h"
#include "thrift/ThriftClientManager.h"
#include "meta/SchemaProviderIf.h"
#include "meta/GflagsManager.h"
#include "stats/Stats.h"

DECLARE_int32(meta_client_retry_times);

namespace nebula {
namespace meta {

using PartsAlloc = std::unordered_map<PartitionID, std::vector<HostAddr>>;
using SpaceIdName = std::pair<GraphSpaceID, std::string>;
using HostStatus = std::pair<HostAddr, std::string>;

// struct for in cache
using TagSchemas = std::unordered_map<std::pair<TagID, SchemaVer>,
                                      std::shared_ptr<const SchemaProviderIf>>;
using EdgeSchemas = std::unordered_map<std::pair<EdgeType, SchemaVer>,
                                       std::shared_ptr<const SchemaProviderIf>>;


// Space and Schema Name => IndexID
// Get IndexID via space ID and index name
using NameIndexMap = std::unordered_map<std::pair<GraphSpaceID, std::string>, IndexID>;

// Index ID => Index Item
// Get Index Structure by indexID
using Indexes = std::unordered_map<IndexID, std::shared_ptr<nebula::cpp2::IndexItem>>;

struct SpaceInfoCache {
    std::string spaceName;
    PartsAlloc partsAlloc_;
    std::unordered_map<HostAddr, std::vector<PartitionID>> partsOnHost_;
    std::vector<cpp2::TagItem> tagItemVec_;
    TagSchemas  tagSchemas_;
    std::vector<cpp2::EdgeItem> edgeItemVec_;
    EdgeSchemas edgeSchemas_;
    std::vector<nebula::cpp2::IndexItem> tagIndexItemVec_;
    Indexes tagIndexes_;
    std::vector<nebula::cpp2::IndexItem> edgeIndexItemVec_;
    Indexes edgeIndexes_;
};

using LocalCache = std::unordered_map<GraphSpaceID, std::shared_ptr<SpaceInfoCache>>;

using SpaceNameIdMap = std::unordered_map<std::string, GraphSpaceID>;

// get tagID via spaceId and tagName
using SpaceTagNameIdMap = std::unordered_map<std::pair<GraphSpaceID, std::string>, TagID>;

// get edgeType via spaceId and edgeName
using SpaceEdgeNameTypeMap = std::unordered_map<std::pair<GraphSpaceID, std::string>, EdgeType>;

// get latest tag version via spaceId and TagID
using SpaceNewestTagVerMap = std::unordered_map<std::pair<GraphSpaceID, TagID>, SchemaVer>;

// get latest edge version via spaceId and edgeType
using SpaceNewestEdgeVerMap = std::unordered_map<std::pair<GraphSpaceID, EdgeType>, SchemaVer>;

// get edgeName via spaceId and edgeType
using SpaceEdgeTypeNameMap = std::unordered_map<std::pair<GraphSpaceID, EdgeType>, std::string>;

// get tagName via spaceId and tagId
using SpaceTagIdNameMap = std::unordered_map<std::pair<GraphSpaceID, TagID>, std::string>;

// get all edgeType edgeName via spaceId
using SpaceAllEdgeMap = std::unordered_map<GraphSpaceID, std::vector<std::string>>;

// get all tagId tagName via spaceId
using SpaceAllTagMap = std::unordered_map<GraphSpaceID, std::vector<std::string>>;
// get leader host via spaceId and partId
using LeaderMap = std::unordered_map<std::pair<GraphSpaceID, PartitionID>, HostAddr>;

using IndexStatus = std::tuple<std::string, std::string, std::string>;

// get user roles by account
using UserRolesMap = std::unordered_map<std::string, std::vector<nebula::cpp2::RoleItem>>;
// get user password by account
using UserPasswordMap = std::unordered_map<std::string, std::string>;

struct ConfigItem {
    ConfigItem() {}

    ConfigItem(const cpp2::ConfigModule& module, const std::string& name,
               const cpp2::ConfigType& type, const cpp2::ConfigMode& mode,
               const VariantType& value)
        : module_(module)
        , name_(name)
        , type_(type)
        , mode_(mode)
        , value_(value) {
    }

    cpp2::ConfigModule  module_;
    std::string         name_;
    cpp2::ConfigType    type_;
    cpp2::ConfigMode    mode_;
    VariantType         value_;
};


struct SpaceDesc {
    SpaceDesc() {}

    SpaceDesc(const std::string& spaceName, int32_t partNum,
              int32_t replicaFactor, const std::string& charsetName = "",
              const std::string& collationName = "")
        : spaceName_(spaceName)
        , partNum_(partNum)
        , replicaFactor_(replicaFactor)
        , charsetName_(charsetName)
        , collationName_(collationName) {
    }

    std::string  spaceName_;
    int32_t      partNum_{0};
    int32_t      replicaFactor_{0};
    std::string  charsetName_;
    std::string  collationName_;
};


// config cache, get config via module and name
using MetaConfigMap = std::unordered_map<std::pair<cpp2::ConfigModule, std::string>, ConfigItem>;

class MetaChangedListener {
public:
    virtual ~MetaChangedListener() = default;
    virtual void onSpaceAdded(GraphSpaceID spaceId) = 0;
    virtual void onSpaceRemoved(GraphSpaceID spaceId) = 0;
    virtual void onSpaceOptionUpdated(GraphSpaceID spaceId,
                                      const std::unordered_map<std::string, std::string>& options)
                                      = 0;
    virtual void onPartAdded(const PartMeta& partMeta) = 0;
    virtual void onPartRemoved(GraphSpaceID spaceId, PartitionID partId) = 0;
    virtual void onPartUpdated(const PartMeta& partMeta) = 0;
    virtual void fetchLeaderInfo(std::unordered_map<GraphSpaceID,
                                                    std::vector<PartitionID>>& leaderIds) = 0;
};

struct MetaClientOptions {
    MetaClientOptions() = default;
    MetaClientOptions(const MetaClientOptions& opt)
        : localHost_(opt.localHost_)
        , clusterId_(opt.clusterId_.load())
        , inStoraged_(opt.inStoraged_)
        , serviceName_(opt.serviceName_)
        , skipConfig_(opt.skipConfig_) {}

    // Current host address
    HostAddr localHost_{0, 0};
    // Current cluster Id, it is requried by storaged only.
    std::atomic<ClusterID> clusterId_{0};
    // If current client being used in storaged.
    bool inStoraged_ = false;
    // Current service name, used in StatsManager
    std::string serviceName_ = "";
    // Whether to skip the config manager
    bool skipConfig_ = false;
};

class MetaClient {
    FRIEND_TEST(ConfigManTest, MetaConfigManTest);
    FRIEND_TEST(ConfigManTest, MockConfigTest);
    FRIEND_TEST(ConfigManTest, RocksdbOptionsTest);
    FRIEND_TEST(MetaClientTest, SimpleTest);
    FRIEND_TEST(MetaClientTest, RetryWithExceptionTest);
    FRIEND_TEST(MetaClientTest, RetryOnceTest);
    FRIEND_TEST(MetaClientTest, RetryUntilLimitTest);
    FRIEND_TEST(MetaClientTest, RocksdbOptionsTest);

public:
    MetaClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
               std::vector<HostAddr> addrs,
               const MetaClientOptions& options = MetaClientOptions());

    virtual ~MetaClient();

    bool isMetadReady();

    bool waitForMetadReady(int count = -1, int retryIntervalSecs = 2);

    void stop();

    void registerListener(MetaChangedListener* listener) {
        folly::RWSpinLock::WriteHolder holder(listenerLock_);
        CHECK(listener_ == nullptr);
        listener_ = listener;
    }

    void unRegisterListener() {
        folly::RWSpinLock::WriteHolder holder(listenerLock_);
        listener_ = nullptr;
    }

    // Operations for parts
    folly::Future<StatusOr<GraphSpaceID>> createSpace(SpaceDesc spaceDesc,
                                                      bool ifNotExists = false);

    folly::Future<StatusOr<std::vector<SpaceIdName>>>
    listSpaces();

    folly::Future<StatusOr<cpp2::AdminJobResult>>
    submitJob(cpp2::AdminJobOp op, std::vector<std::string> paras);

    folly::Future<StatusOr<cpp2::SpaceItem>>
    getSpace(std::string name);

    folly::Future<StatusOr<bool>>
    dropSpace(std::string name, bool ifExists = false);

    folly::Future<StatusOr<std::vector<cpp2::HostItem>>>
    listHosts();

    folly::Future<StatusOr<std::vector<cpp2::PartItem>>>
    listParts(GraphSpaceID spaceId, std::vector<PartitionID> partIds);

    folly::Future<StatusOr<PartsAlloc>>
    getPartsAlloc(GraphSpaceID spaceId);

    // Operations for schema
    folly::Future<StatusOr<TagID>> createTagSchema(GraphSpaceID spaceId,
                                                   std::string name,
                                                   nebula::cpp2::Schema schema,
                                                   bool ifNotExists = false);

    folly::Future<StatusOr<TagID>>
    alterTagSchema(GraphSpaceID spaceId,
                   std::string name,
                   std::vector<cpp2::AlterSchemaItem> items,
                   nebula::cpp2::SchemaProp schemaProp);

    folly::Future<StatusOr<std::vector<cpp2::TagItem>>>
    listTagSchemas(GraphSpaceID spaceId);

    folly::Future<StatusOr<bool>>
    dropTagSchema(int32_t spaceId, std::string name, bool ifExists = false);

    // Return the latest schema when ver = -1
    folly::Future<StatusOr<nebula::cpp2::Schema>>
    getTagSchema(int32_t spaceId, std::string name, SchemaVer version = -1);

    folly::Future<StatusOr<EdgeType>> createEdgeSchema(GraphSpaceID spaceId,
                                                       std::string name,
                                                       nebula::cpp2::Schema schema,
                                                       bool ifNotExists = false);

    folly::Future<StatusOr<bool>>
    alterEdgeSchema(GraphSpaceID spaceId,
                    std::string name,
                    std::vector<cpp2::AlterSchemaItem> items,
                    nebula::cpp2::SchemaProp schemaProp);

    folly::Future<StatusOr<std::vector<cpp2::EdgeItem>>>
    listEdgeSchemas(GraphSpaceID spaceId);

    // Return the latest schema when ver = -1
    folly::Future<StatusOr<nebula::cpp2::Schema>>
    getEdgeSchema(GraphSpaceID spaceId, std::string name, SchemaVer version = -1);

    folly::Future<StatusOr<bool>>
    dropEdgeSchema(GraphSpaceID spaceId, std::string name, bool ifExists = false);

    // Operations for index
    folly::Future<StatusOr<IndexID>>
    createTagIndex(GraphSpaceID spaceID,
                   std::string indexName,
                   std::string tagName,
                   std::vector<std::string> fields,
                   bool ifNotExists = false);

    // Remove the define of tag index
    folly::Future<StatusOr<bool>>
    dropTagIndex(GraphSpaceID spaceId, std::string name, bool ifExists = false);

    folly::Future<StatusOr<nebula::cpp2::IndexItem>>
    getTagIndex(GraphSpaceID spaceId, std::string name);

    folly::Future<StatusOr<std::vector<nebula::cpp2::IndexItem>>>
    listTagIndexes(GraphSpaceID spaceId);

    folly::Future<StatusOr<bool>>
    rebuildTagIndex(GraphSpaceID spaceID, std::string name, bool isOffline);

    folly::Future<StatusOr<std::vector<cpp2::IndexStatus>>>
    listTagIndexStatus(GraphSpaceID spaceId);

    folly::Future<StatusOr<IndexID>>
    createEdgeIndex(GraphSpaceID spaceID,
                    std::string indexName,
                    std::string edgeName,
                    std::vector<std::string> fields,
                    bool ifNotExists = false);

    // Remove the define of edge index
    folly::Future<StatusOr<bool>>
    dropEdgeIndex(GraphSpaceID spaceId, std::string name, bool ifExists = false);

    folly::Future<StatusOr<nebula::cpp2::IndexItem>>
    getEdgeIndex(GraphSpaceID spaceId, std::string name);

    folly::Future<StatusOr<std::vector<nebula::cpp2::IndexItem>>>
    listEdgeIndexes(GraphSpaceID spaceId);

    folly::Future<StatusOr<bool>>
    rebuildEdgeIndex(GraphSpaceID spaceId, std::string name, bool isOffline);

    folly::Future<StatusOr<std::vector<cpp2::IndexStatus>>>
    listEdgeIndexStatus(GraphSpaceID spaceId);

    // Operations for custom kv
    folly::Future<StatusOr<bool>>
    multiPut(std::string segment,
             std::vector<std::pair<std::string, std::string>> pairs);

    folly::Future<StatusOr<std::string>>
    get(std::string segment, std::string key);

    folly::Future<StatusOr<std::vector<std::string>>>
    multiGet(std::string segment, std::vector<std::string> keys);

    folly::Future<StatusOr<std::vector<std::string>>>
    scan(std::string segment, std::string start, std::string end);

    folly::Future<StatusOr<bool>>
    remove(std::string segment, std::string key);

    folly::Future<StatusOr<bool>>
    removeRange(std::string segment, std::string start, std::string end);

    // Operations for users.
    folly::Future<StatusOr<bool>>
    createUser(std::string account, std::string password, bool ifNotExists);

    folly::Future<StatusOr<bool>>
    dropUser(std::string account, bool ifExists);

    folly::Future<StatusOr<bool>>
    alterUser(std::string account, std::string password);

    folly::Future<StatusOr<bool>>
    grantToUser(nebula::cpp2::RoleItem roleItem);

    folly::Future<StatusOr<bool>>
    revokeFromUser(nebula::cpp2::RoleItem roleItem);

    folly::Future<StatusOr<std::map<std::string, std::string>>>
    listUsers();

    folly::Future<StatusOr<std::vector<nebula::cpp2::RoleItem>>>
    listRoles(GraphSpaceID space);

    folly::Future<StatusOr<bool>>
    changePassword(std::string account, std::string newPwd, std::string oldPwd);

    folly::Future<StatusOr<std::vector<nebula::cpp2::RoleItem>>>
    getUserRoles(std::string account);

    // Operations for admin
    folly::Future<StatusOr<int64_t>>
    balance(std::vector<HostAddr> hostDel, bool isStop = false);

    folly::Future<StatusOr<std::vector<cpp2::BalanceTask>>>
    showBalance(int64_t balanceId);

    folly::Future<StatusOr<bool>> balanceLeader();

    // Operations for config
    folly::Future<StatusOr<bool>>
    regConfig(const std::vector<cpp2::ConfigItem>& items);

    folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
    getConfig(const cpp2::ConfigModule& module, const std::string& name);

    folly::Future<StatusOr<bool>>
    setConfig(const cpp2::ConfigModule& module, const std::string& name,
              const cpp2::ConfigType& type, const std::string& value);

    folly::Future<StatusOr<std::vector<cpp2::ConfigItem>>>
    listConfigs(const cpp2::ConfigModule& module);

    folly::Future<StatusOr<bool>> createSnapshot();

    folly::Future<StatusOr<bool>> dropSnapshot(const std::string& name);

    folly::Future<StatusOr<std::vector<cpp2::Snapshot>>> listSnapshots();

    // Operations for cache.
    StatusOr<GraphSpaceID>
    getSpaceIdByNameFromCache(const std::string& name);

    StatusOr<TagID>
    getTagIDByNameFromCache(const GraphSpaceID& space, const std::string& name);

    StatusOr<std::string>
    getTagNameByIdFromCache(const GraphSpaceID& space, const TagID& tagId);

    StatusOr<SchemaVer> getLatestTagVersionFromCache(const GraphSpaceID& space, const TagID& tagId);

    StatusOr<SchemaVer> getLatestEdgeVersionFromCache(const GraphSpaceID& space,
                                                      const EdgeType& edgeType);

    StatusOr<EdgeType>
    getEdgeTypeByNameFromCache(const GraphSpaceID& space, const std::string& name);

    StatusOr<std::string>
    getEdgeNameByTypeFromCache(const GraphSpaceID& space, const EdgeType edgeType);

    // get all lastest version edge
    StatusOr<std::vector<std::string>> getAllEdgeFromCache(const GraphSpaceID& space);

    // get all lastest version tag
    StatusOr<std::vector<std::string>> getAllTagFromCache(const GraphSpaceID& space);

    PartsMap getPartsMapFromCache(const HostAddr& host);

    StatusOr<PartMeta> getPartMetaFromCache(GraphSpaceID spaceId, PartitionID partId);

    Status checkPartExistInCache(const HostAddr& host,
                                 GraphSpaceID spaceId,
                                 PartitionID partId);

    Status checkSpaceExistInCache(const HostAddr& host,
                                  GraphSpaceID spaceId);

    StatusOr<int32_t> partsNum(GraphSpaceID spaceId);

    StatusOr<std::shared_ptr<const SchemaProviderIf>>
    getTagSchemaFromCache(GraphSpaceID spaceId, TagID tagID, SchemaVer ver = -1);

    StatusOr<std::shared_ptr<const SchemaProviderIf>>
    getEdgeSchemaFromCache(GraphSpaceID spaceId, EdgeType edgeType, SchemaVer ver = -1);

    StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
    getTagIndexByNameFromCache(const GraphSpaceID space, const std::string& name);

    StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
    getEdgeIndexByNameFromCache(const GraphSpaceID space, const std::string& name);

    StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
    getTagIndexFromCache(GraphSpaceID spaceId, IndexID indexID);

    StatusOr<TagID>
    getRelatedTagIDByIndexNameFromCache(const GraphSpaceID space,
                                        const std::string& indexName);

    StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>>
    getEdgeIndexFromCache(GraphSpaceID spaceId, IndexID indexID);

    StatusOr<EdgeType>
    getRelatedEdgeTypeByIndexNameFromCache(const GraphSpaceID space,
                                           const std::string& indexName);

    StatusOr<std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>>
    getTagIndexesFromCache(GraphSpaceID spaceId);

    StatusOr<std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>>
    getEdgeIndexesFromCache(GraphSpaceID spaceId);

    Status checkTagIndexed(GraphSpaceID space, TagID tagID);

    Status checkEdgeIndexed(GraphSpaceID space, EdgeType edgeType);

    const std::vector<HostAddr>& getAddresses();

    folly::Future<StatusOr<std::string>> getTagDefaultValue(GraphSpaceID spaceId,
                                                            TagID tagId,
                                                            const std::string& field);

    folly::Future<StatusOr<std::string>> getEdgeDefaultValue(GraphSpaceID spaceId,
                                                             EdgeType edgeType,
                                                             const std::string& field);

    std::vector<nebula::cpp2::RoleItem> getRolesByUserFromCache(const std::string& user);

    bool authCheckFromCache(const std::string& account, const std::string& password);

    bool checkShadowAccountFromCache(const std::string& account);

    Status refreshCache();

    StatusOr<LeaderMap> loadLeader();

protected:
    // Return true if load succeeded.
    bool loadData();
    bool loadCfg();
    void heartBeatThreadFunc();

    bool registerCfg();
    void updateGflagsValue(const ConfigItem& item);
    void updateNestedGflags(const std::string& name);

    bool loadSchemas(GraphSpaceID spaceId,
                     std::shared_ptr<SpaceInfoCache> spaceInfoCache,
                     SpaceTagNameIdMap &tagNameIdMap,
                     SpaceTagIdNameMap &tagIdNameMap,
                     SpaceEdgeNameTypeMap &edgeNameTypeMap,
                     SpaceEdgeTypeNameMap &edgeTypeNamemap,
                     SpaceNewestTagVerMap &newestTagVerMap,
                     SpaceNewestEdgeVerMap &newestEdgeVerMap,
                     SpaceAllEdgeMap &allEdgemap,
                     SpaceAllTagMap &allTagmap);

    bool loadUsersAndRoles();

    bool loadIndexes(GraphSpaceID spaceId,
                     std::shared_ptr<SpaceInfoCache> cache);

    folly::Future<StatusOr<bool>> heartbeat();

    std::unordered_map<HostAddr, std::vector<PartitionID>> reverse(const PartsAlloc& parts);

    void updateActive() {
        folly::RWSpinLock::WriteHolder holder(hostLock_);
        active_ = addrs_[folly::Random::rand64(addrs_.size())];
    }

    void updateLeader(HostAddr leader = {0, 0}) {
        if (leader != HostAddr(0, 0)) {
            folly::RWSpinLock::WriteHolder holder(hostLock_);
            leader_ = leader;
        } else {
            folly::RWSpinLock::WriteHolder holder(hostLock_);
            leader_ = addrs_[folly::Random::rand64(addrs_.size())];
        }
    }

    void diff(const LocalCache& oldCache, const LocalCache& newCache);

    template<typename RESP>
    Status handleResponse(const RESP& resp);

    template<class Request,
             class RemoteFunc,
             class RespGenerator,
             class RpcResponse =
                typename std::result_of<
                    RemoteFunc(std::shared_ptr<meta::cpp2::MetaServiceAsyncClient>, Request)
                >::type::value_type,
             class Response =
                typename std::result_of<RespGenerator(RpcResponse)>::type
    >
    void getResponse(Request req,
                     RemoteFunc remoteFunc,
                     RespGenerator respGen,
                     folly::Promise<StatusOr<Response>> pro,
                     bool toLeader = false,
                     int32_t retry = 0,
                     int32_t retryLimit = FLAGS_meta_client_retry_times);

    std::vector<HostAddr> to(const std::vector<nebula::cpp2::HostAddr>& hosts);

    std::vector<SpaceIdName> toSpaceIdName(const std::vector<cpp2::IdName>& tIdNames);

    ConfigItem toConfigItem(const cpp2::ConfigItem& item);

    PartsMap doGetPartsMap(const HostAddr& host,
                           const LocalCache& localCache);

private:
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
    std::shared_ptr<thrift::ThriftClientManager<meta::cpp2::MetaServiceAsyncClient>> clientsMan_;

    std::unordered_map<GraphSpaceID, std::vector<PartitionID>> leaderIds_;
    folly::RWSpinLock     leaderIdsLock_;
    std::atomic<int64_t>  localDataLastUpdateTime_{-1};
    std::atomic<int64_t>  localCfgLastUpdateTime_{-1};
    std::atomic<int64_t>  metadLastUpdateTime_{0};

    struct ThreadLocalInfo {
        int64_t               localLastUpdateTime_{-1};
        LocalCache            localCache_;
        SpaceNameIdMap        spaceIndexByName_;
        SpaceTagNameIdMap     spaceTagIndexByName_;
        SpaceEdgeNameTypeMap  spaceEdgeIndexByName_;
        SpaceEdgeTypeNameMap  spaceEdgeIndexByType_;
        SpaceTagIdNameMap     spaceTagIndexById_;
        SpaceNewestTagVerMap  spaceNewestTagVerMap_;
        SpaceNewestEdgeVerMap spaceNewestEdgeVerMap_;
        SpaceAllEdgeMap       spaceAllEdgeMap_;
        SpaceAllTagMap        spaceAllTagMap_;
    };

    const ThreadLocalInfo& getThreadLocalInfo();

    LocalCache localCache_;
    std::vector<HostAddr> addrs_;
    // The lock used to protect active_ and leader_.
    folly::RWSpinLock hostLock_;
    HostAddr active_;
    HostAddr leader_;
    HostAddr localHost_;

    std::unique_ptr<thread::GenericWorker> bgThread_;
    SpaceNameIdMap        spaceIndexByName_;
    SpaceTagNameIdMap     spaceTagIndexByName_;
    SpaceEdgeNameTypeMap  spaceEdgeIndexByName_;
    SpaceEdgeTypeNameMap  spaceEdgeIndexByType_;
    SpaceTagIdNameMap     spaceTagIndexById_;
    SpaceNewestTagVerMap  spaceNewestTagVerMap_;
    SpaceNewestEdgeVerMap spaceNewestEdgeVerMap_;
    SpaceAllEdgeMap       spaceAllEdgeMap_;
    SpaceAllTagMap        spaceAllTagMap_;

    UserRolesMap          userRolesMap_;
    UserPasswordMap       userPasswordMap_;

    NameIndexMap          tagNameIndexMap_;
    NameIndexMap          edgeNameIndexMap_;

    folly::RWSpinLock     localCacheLock_;
    MetaChangedListener*  listener_{nullptr};
    folly::RWSpinLock     listenerLock_;
    std::atomic<ClusterID> clusterId_{0};
    bool                  isRunning_{false};
    bool                  inStoraged_{true};
    std::atomic_bool      ready_{false};
    MetaConfigMap         metaConfigMap_;
    folly::RWSpinLock     configCacheLock_;
    cpp2::ConfigModule    gflagsModule_{cpp2::ConfigModule::UNKNOWN};
    std::atomic_bool      configReady_{false};
    std::vector<cpp2::ConfigItem> gflagsDeclared_;
    std::unique_ptr<stats::Stats> stats_;
    bool                  skipConfig_ = false;
    MetaClientOptions     options_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METACLIENT_H_
