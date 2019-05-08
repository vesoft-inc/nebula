/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_METACLIENT_H_
#define META_METACLIENT_H_

#include "base/Base.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/RWSpinLock.h>
#include "gen-cpp2/MetaServiceAsyncClient.h"
#include "base/Status.h"
#include "base/StatusOr.h"
#include "thread/GenericWorker.h"
#include "thrift/ThriftClientManager.h"
#include "meta/SchemaProviderIf.h"

namespace nebula {
namespace meta {

using PartsAlloc = std::unordered_map<PartitionID, std::vector<HostAddr>>;
using SpaceIdName = std::pair<GraphSpaceID, std::string>;

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

using SpaceNameIdMap = std::unordered_map<std::string, GraphSpaceID>;
// get tagID via spaceId and tagName
using SpaceTagNameIdMap = std::unordered_map<std::pair<GraphSpaceID, std::string>, TagID>;
// get edgeType via spaceId and edgeName
using SpaceEdgeNameTypeMap = std::unordered_map<std::pair<GraphSpaceID, std::string>, EdgeType>;
// get newest tag ver via spaceId and TagID
using SpaceNewestTagVerMap = std::unordered_map<std::pair<GraphSpaceID, TagID>, SchemaVer>;
// get newest edge ver via spaceId and edgeType
using SpaceNewestEdgeVerMap = std::unordered_map<std::pair<GraphSpaceID, EdgeType>, SchemaVer>;

class MetaChangedListener {
public:
    virtual void onSpaceAdded(GraphSpaceID spaceId) = 0;
    virtual void onSpaceRemoved(GraphSpaceID spaceId) = 0;
    virtual void onPartAdded(const PartMeta& partMeta) = 0;
    virtual void onPartRemoved(GraphSpaceID spaceId, PartitionID partId) = 0;
    virtual void onPartUpdated(const PartMeta& partMeta) = 0;
    virtual HostAddr getLocalHost() = 0;
};

class MetaClient {
public:
    explicit MetaClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool = nullptr,
                        std::vector<HostAddr> addrs = {});

    virtual ~MetaClient();

    void init();

    void registerListener(MetaChangedListener* listener) {
        listener_ = listener;
    }

    /**
     * TODO(dangleptr): Use one struct to represent space description.
     * */
    folly::Future<StatusOr<GraphSpaceID>>
    createSpace(std::string name, int32_t partsNum, int32_t replicaFactor);

    folly::Future<StatusOr<std::vector<SpaceIdName>>>
    listSpaces();

    folly::Future<StatusOr<bool>>
    dropSpace(std::string name);

    folly::Future<StatusOr<bool>>
    addHosts(const std::vector<HostAddr>& hosts);

    folly::Future<StatusOr<std::vector<HostAddr>>>
    listHosts();

    folly::Future<StatusOr<bool>>
    removeHosts(const std::vector<HostAddr>& hosts);

    folly::Future<StatusOr<PartsAlloc>>
    getPartsAlloc(GraphSpaceID spaceId);

    // TODO(Laura) : We can actively update the cache once we add the schema
    folly::Future<StatusOr<TagID>> createTagSchema(GraphSpaceID spaceId,
                                                   std::string name,
                                                   nebula::cpp2::Schema schema);

    // TODO(boshengchen) need refresh tagNameIdMap and newestTagVerMap
    folly::Future<StatusOr<TagID>> alterTagSchema(GraphSpaceID spaceId,
                                                  std::string name,
                                                  std::vector<cpp2::AlterTagItem> tagItems);

    folly::Future<StatusOr<std::vector<cpp2::TagItem>>> listTagSchemas(GraphSpaceID spaceId);

    // TODO(Laura) : We can actively update the cache once we add the schema
    folly::Future<StatusOr<EdgeType>> createEdgeSchema(GraphSpaceID spaceId,
                                                       std::string name,
                                                       nebula::cpp2::Schema schema);

    folly::Future<StatusOr<std::vector<cpp2::EdgeItem>>> listEdgeSchemas(GraphSpaceID spaceId);

    // These are the interfaces about cache opeartions.
    StatusOr<GraphSpaceID> getSpaceIdByNameFromCache(const std::string& name);

    StatusOr<TagID> getTagIDByNameFromCache(const GraphSpaceID& space, const std::string& name);

    StatusOr<EdgeType> getEdgeTypeByNameFromCache(const GraphSpaceID& space,
                                                  const std::string& name);

    SchemaVer getNewestTagVerFromCache(const GraphSpaceID& space, const TagID& tagId);

    SchemaVer getNewestEdgeVerFromCache(const GraphSpaceID& space, const EdgeType& edgeType);

    PartsMap getPartsMapFromCache(const HostAddr& host);

    PartMeta getPartMetaFromCache(GraphSpaceID spaceId, PartitionID partId);

    bool checkPartExistInCache(const HostAddr& host,
                               GraphSpaceID spaceId,
                               PartitionID partId);

    bool checkSpaceExistInCache(const HostAddr& host,
                                GraphSpaceID spaceId);

    int32_t partsNum(GraphSpaceID spaceId);

    StatusOr<std::shared_ptr<const SchemaProviderIf>> getTagSchemeFromCache(GraphSpaceID spaceId,
                                                                            TagID tagID,
                                                                            SchemaVer ver = -1);

    StatusOr<std::shared_ptr<const SchemaProviderIf>> getEdgeSchemeFromCache(GraphSpaceID spaceId,
                                                                             EdgeType edgeType,
                                                                             SchemaVer ver = -1);

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

protected:
    void loadDataThreadFunc();

    bool loadSchemas(GraphSpaceID spaceId,
                     std::shared_ptr<SpaceInfoCache> spaceInfoCache,
                     SpaceTagNameIdMap &tagNameIdMap,
                     SpaceEdgeNameTypeMap &edgeNameTypeMap,
                     SpaceNewestTagVerMap &newestTagVerMap,
                     SpaceNewestEdgeVerMap &newestEdgeVerMap);

    std::unordered_map<HostAddr, std::vector<PartitionID>> reverse(const PartsAlloc& parts);

    void updateActiveHost() {
        active_ = addrs_[folly::Random::rand64(addrs_.size())];
    }

    void diff(const std::unordered_map<GraphSpaceID, std::shared_ptr<SpaceInfoCache>>& newCache);

    template<typename RESP>
    Status handleResponse(const RESP& resp);

    template<class Request,
             class RemoteFunc,
             class Response =
                typename std::result_of<
                    RemoteFunc(std::shared_ptr<meta::cpp2::MetaServiceAsyncClient>, Request)
                >::type::value_type
    >
    Response collectResponse(Request req, RemoteFunc remoteFunc);

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
    folly::Future<StatusOr<Response>> getResponse(Request req,
                                                  RemoteFunc remoteFunc,
                                                  RespGenerator respGen);

    std::vector<HostAddr> to(const std::vector<nebula::cpp2::HostAddr>& hosts);

    std::vector<SpaceIdName> toSpaceIdName(const std::vector<cpp2::IdName>& tIdNames);

    PartsMap doGetPartsMap(const HostAddr& host,
                           const std::unordered_map<
                                        GraphSpaceID,
                                        std::shared_ptr<SpaceInfoCache>>& localCache);

private:
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
    std::shared_ptr<thrift::ThriftClientManager<meta::cpp2::MetaServiceAsyncClient>> clientsMan_;
    std::vector<HostAddr> addrs_;
    HostAddr active_;
    thread::GenericWorker loadDataThread_;
    std::unordered_map<GraphSpaceID, std::shared_ptr<SpaceInfoCache>> localCache_;
    SpaceNameIdMap        spaceIndexByName_;
    SpaceTagNameIdMap     spaceTagIndexByName_;
    SpaceEdgeNameTypeMap  spaceEdgeIndexByName_;
    SpaceNewestTagVerMap  spaceNewestTagVerMap_;
    SpaceNewestEdgeVerMap spaceNewestEdgeVerMap_;
    folly::RWSpinLock localCacheLock_;
    MetaChangedListener* listener_{nullptr};
};
}  // namespace meta
}  // namespace nebula
#endif  // META_METACLIENT_H_
