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
using TagIDSchemas = std::unordered_map<std::pair<TagID, SchemaVer>,
                                        std::shared_ptr<const SchemaProviderIf>>;
using EdgeTypeSchemas = std::unordered_map<std::pair<EdgeType, SchemaVer>,
                                           std::shared_ptr<const SchemaProviderIf>>;

struct SpaceInfoCache {
    std::string spaceName;
    PartsAlloc partsAlloc_;
    std::unordered_map<HostAddr, std::vector<PartitionID>> partsOnHost_;
    TagIDSchemas tagSchemas_;
    EdgeTypeSchemas edgeSchemas_;
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
// get all edgeType edgeName via spaceId
using SpaceAllEdgeMap = std::unordered_map<GraphSpaceID, std::vector<std::string>>;

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

// config cahce, get config via module and name
using MetaConfigMap = std::unordered_map<std::pair<cpp2::ConfigModule, std::string>, ConfigItem>;

class MetaChangedListener {
public:
    virtual void onSpaceAdded(GraphSpaceID spaceId) = 0;
    virtual void onSpaceRemoved(GraphSpaceID spaceId) = 0;
    virtual void onSpaceOptionUpdated(GraphSpaceID spaceId,
                                      const std::unordered_map<std::string, std::string>& options)
                                      = 0;
    virtual void onPartAdded(const PartMeta& partMeta) = 0;
    virtual void onPartRemoved(GraphSpaceID spaceId, PartitionID partId) = 0;
    virtual void onPartUpdated(const PartMeta& partMeta) = 0;
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
    explicit MetaClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
                        std::vector<HostAddr> addrs,
                        HostAddr localHost = HostAddr(0, 0),
                        ClusterID clusterId = 0,
                        bool sendHeartBeat = false,
                        stats::Stats *stats = nullptr);


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
    /**
     * TODO(dangleptr): Use one struct to represent space description.
     * */
    folly::Future<StatusOr<GraphSpaceID>>
    createSpace(std::string name, int32_t partsNum, int32_t replicaFactor);

    folly::Future<StatusOr<std::vector<SpaceIdName>>>
    listSpaces();

    folly::Future<StatusOr<cpp2::SpaceItem>>
    getSpace(std::string name);

    folly::Future<StatusOr<bool>>
    dropSpace(std::string name);

    folly::Future<StatusOr<std::vector<cpp2::HostItem>>>
    listHosts();

    folly::Future<StatusOr<std::vector<cpp2::PartItem>>>
    listParts(GraphSpaceID spaceId);

    folly::Future<StatusOr<PartsAlloc>>
    getPartsAlloc(GraphSpaceID spaceId);

    // Operations for schema
    folly::Future<StatusOr<TagID>>
    createTagSchema(GraphSpaceID spaceId, std::string name, nebula::cpp2::Schema schema);

    folly::Future<StatusOr<TagID>>
    alterTagSchema(GraphSpaceID spaceId,
                   std::string name,
                   std::vector<cpp2::AlterSchemaItem> items,
                   nebula::cpp2::SchemaProp schemaProp);

    folly::Future<StatusOr<std::vector<cpp2::TagItem>>>
    listTagSchemas(GraphSpaceID spaceId);

    folly::Future<StatusOr<bool>>
    dropTagSchema(int32_t spaceId, std::string name);

    // Return the latest schema when ver = -1
    folly::Future<StatusOr<nebula::cpp2::Schema>>
    getTagSchema(int32_t spaceId, std::string name, SchemaVer version = -1);

    folly::Future<StatusOr<EdgeType>>
    createEdgeSchema(GraphSpaceID spaceId, std::string name, nebula::cpp2::Schema schema);

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
    dropEdgeSchema(GraphSpaceID spaceId, std::string name);

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

    // Opeartions for cache.
    StatusOr<GraphSpaceID> getSpaceIdByNameFromCache(const std::string& name);

    StatusOr<TagID> getTagIDByNameFromCache(const GraphSpaceID& space, const std::string& name);

    StatusOr<EdgeType> getEdgeTypeByNameFromCache(const GraphSpaceID& space,
                                                  const std::string& name);
    StatusOr<std::string> getEdgeNameByTypeFromCache(const GraphSpaceID& space,
                                                     const EdgeType edgeType);

    StatusOr<SchemaVer> getNewestTagVerFromCache(const GraphSpaceID& space, const TagID& tagId);

    StatusOr<SchemaVer> getNewestEdgeVerFromCache(const GraphSpaceID& space,
                                                  const EdgeType& edgeType);

    StatusOr<std::vector<std::string>> getAllEdgeFromCache(const GraphSpaceID& space);

    PartsMap getPartsMapFromCache(const HostAddr& host);

    StatusOr<PartMeta> getPartMetaFromCache(GraphSpaceID spaceId, PartitionID partId);

    bool checkPartExistInCache(const HostAddr& host,
                               GraphSpaceID spaceId,
                               PartitionID partId);

    bool checkSpaceExistInCache(const HostAddr& host,
                                GraphSpaceID spaceId);

    StatusOr<int32_t> partsNum(GraphSpaceID spaceId);

    StatusOr<std::shared_ptr<const SchemaProviderIf>>
    getTagSchemaFromCache(GraphSpaceID spaceId, TagID tagID, SchemaVer ver = -1);

    StatusOr<std::shared_ptr<const SchemaProviderIf>>
    getEdgeSchemaFromCache(GraphSpaceID spaceId, EdgeType edgeType, SchemaVer ver = -1);

    const std::vector<HostAddr>& getAddresses();

    folly::Future<StatusOr<std::string>> getTagDefaultValue(GraphSpaceID spaceId,
                                                            TagID tagId,
                                                            const std::string& field);

    folly::Future<StatusOr<std::string>> getEdgeDefaultValue(GraphSpaceID spaceId,
                                                             EdgeType edgeType,
                                                             const std::string& field);

    Status refreshCache();

protected:
    void loadDataThreadFunc();
    // Return true if load succeeded.
    bool loadData();

    void addLoadDataTask();

    void heartBeatThreadFunc();

    void loadCfgThreadFunc();
    void loadCfg();
    bool registerCfg();
    void addLoadCfgTask();
    void updateGflagsValue(const ConfigItem& item);
    void updateNestedGflags(const std::string& name);

    bool loadSchemas(GraphSpaceID spaceId,
                     std::shared_ptr<SpaceInfoCache> spaceInfoCache,
                     SpaceTagNameIdMap &tagNameIdMap,
                     SpaceEdgeNameTypeMap &edgeNameTypeMap,
                     SpaceEdgeTypeNameMap &edgeTypeNamemap,
                     SpaceNewestTagVerMap &newestTagVerMap,
                     SpaceNewestEdgeVerMap &newestEdgeVerMap,
                     SpaceAllEdgeMap &allEdgemap);

    folly::Future<StatusOr<bool>> heartbeat();

    std::unordered_map<HostAddr, std::vector<PartitionID>> reverse(const PartsAlloc& parts);

    void updateActive() {
        folly::RWSpinLock::WriteHolder holder(hostLock_);
        active_ = addrs_[folly::Random::rand64(addrs_.size())];
    }

    void updateLeader() {
        folly::RWSpinLock::WriteHolder holder(hostLock_);
        leader_ = addrs_[folly::Random::rand64(addrs_.size())];
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
    SpaceNewestTagVerMap  spaceNewestTagVerMap_;
    SpaceNewestEdgeVerMap spaceNewestEdgeVerMap_;
    SpaceAllEdgeMap      spaceAllEdgeMap_;
    folly::RWSpinLock     localCacheLock_;
    MetaChangedListener*  listener_{nullptr};
    folly::RWSpinLock     listenerLock_;
    std::atomic<ClusterID> clusterId_{0};
    bool                  isRunning_{false};
    bool                  sendHeartBeat_{false};
    std::atomic_bool      ready_{false};
    MetaConfigMap         metaConfigMap_;
    folly::RWSpinLock     configCacheLock_;
    cpp2::ConfigModule    gflagsModule_{cpp2::ConfigModule::UNKNOWN};
    std::atomic_bool      configReady_{false};
    std::vector<cpp2::ConfigItem> gflagsDeclared_;
    stats::Stats         *stats_{nullptr};
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METACLIENT_H_
