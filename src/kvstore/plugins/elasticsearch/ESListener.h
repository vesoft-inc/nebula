/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_PLUGINS_ES_LISTENER_H_
#define KVSTORE_PLUGINS_ES_LISTENER_H_

#include "kvstore/Listener.h"

namespace nebula {
namespace kvstore {

class ESListener : public Listener {
public:
    ESListener(GraphSpaceID spaceId,
              PartitionID partId,
              HostAddr localAddr,
              const std::string& walPath,
              std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
              std::shared_ptr<thread::GenericThreadPool> workers,
              std::shared_ptr<folly::Executor> handlers,
              std::shared_ptr<raftex::SnapshotManager> snapshotMan,
              std::shared_ptr<RaftClient> clientMan,
              meta::SchemaManager* schemaMan)
        : Listener(spaceId, partId, std::move(localAddr), walPath,
                   ioPool, workers, handlers, snapshotMan, clientMan, schemaMan) {
    }

protected:
    void init() override {
    }

    bool apply(const std::vector<KV>&) override {
        return true;
    }

    bool persist(LogID, TermID, LogID) override {
        return true;
    }

    std::pair<LogID, TermID> lastCommittedLogId() override {
        return {0, 0};
    }

    LogID lastApplyLogId() override {
        return 0;
    }

    void cleanup() override {
    }
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_PLUGINS_ES_LISTENER_H_
