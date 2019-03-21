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

namespace nebula {
namespace meta {

using PartsAlloc = std::unordered_map<PartitionID, std::vector<HostAddr>>;
using SpaceIdName = std::pair<GraphSpaceID, std::string>;

struct SpaceInfoCache {
    std::string spaceName;
    PartsAlloc partsAlloc_;
    std::unordered_map<HostAddr, std::vector<PartitionID>> partsOnHost_;
};

using SpaceNameIdMap = std::unordered_map<std::string, GraphSpaceID>;

class MetaClient {
public:
    MetaClient(std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
               std::vector<HostAddr> addrs)
        : ioThreadPool_(ioThreadPool)
        , addrs_(std::move(addrs)) {
        updateActiveHost();
     }

    virtual ~MetaClient() {
        loadDataThread_.stop();
        loadDataThread_.wait();
        VLOG(3) << "~MetaClient";
    }

    void init();

    /**
     * TODO(dangleptr): Use one struct to represent space description.
     * */
    StatusOr<GraphSpaceID> createSpace(std::string name, int32_t partsNum, int32_t replicaFator);

    StatusOr<std::vector<SpaceIdName>> listSpaces();

    Status addHosts(const std::vector<HostAddr>& hosts);

    StatusOr<std::vector<HostAddr>> listHosts();

    StatusOr<PartsAlloc> getPartsAlloc(GraphSpaceID spaceId);

    StatusOr<std::vector<PartitionID>>
    getPartsFromCache(GraphSpaceID spaceId, const HostAddr& host);

    StatusOr<std::vector<HostAddr>>
    getHostsFromCache(GraphSpaceID spaceId, PartitionID partId);

    StatusOr<GraphSpaceID> getSpaceIdByNameFromCache(const std::string& name);

protected:
    void loadDataThreadFunc();

    std::unordered_map<HostAddr, std::vector<PartitionID>> revert(const PartsAlloc& parts);

    void updateActiveHost() {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, addrs_.size() - 1);
        active_ = addrs_[dis(gen)];
    }

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

    std::vector<HostAddr> to(const std::vector<nebula::cpp2::HostAddr>& hosts);

    std::vector<SpaceIdName> toSpaceIdName(const std::vector<cpp2::IdName>& tIdNames);

private:
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool_;
    std::vector<HostAddr> addrs_;
    HostAddr active_;
    thread::GenericWorker loadDataThread_;
    std::unordered_map<GraphSpaceID, std::shared_ptr<SpaceInfoCache>> localCache_;
    SpaceNameIdMap  spaceIndexByName_;
    folly::RWSpinLock localCacheLock_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_METACLIENT_H_
