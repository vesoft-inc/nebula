/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#ifndef META_CLUSTERCHECKHANDLER_H
#define META_CLUSTERCHECKHANDLER_H

#include "base/Base.h"
#include <folly/executors/IOThreadPoolExecutor.h>
#include "base/Status.h"
#include "base/StatusOr.h"
#include "thread/GenericWorker.h"
#include "kvstore/KVStore.h"
#include "meta/processors/admin/AdminClient.h"
#include "meta/processors/Common.h"
#include "meta/MetaServiceUtils.h"
#include "ActiveHostsMan.h"

namespace nebula {
namespace meta {

class ClusterCheckHandler {
public:
    explicit ClusterCheckHandler(kvstore::KVStore* kv,
                                 AdminClient* adminClient);

    virtual ~ClusterCheckHandler();

    void stop();

    void addUnblockingTask();

    void addDropCheckpointTask(const std::string& name);

    bool isUnblockingRunning();

protected:
    bool unBlocking();
    bool dropCheckpoint(const std::string& name);
    void dropCheckpointThreadFunc(const std::string& name);
    bool getAllSpaces(std::vector<GraphSpaceID>& spaces);
    bool doSyncRemove(const std::string& key);

private:
    std::unique_ptr<thread::GenericWorker> bgThread_;
    kvstore::KVStore* kvstore_ = nullptr;
    ClusterID clusterId_{0};
    AdminClient* adminClient_;
    bool unblockingRunning_{false};
};
}  // namespace meta
}  // namespace nebula
#endif  // META_CLUSTERCHECKHANDLER_H

