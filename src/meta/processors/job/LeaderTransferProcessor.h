//
// Created by fujie on 24-10-21.
//

#ifndef NEBULA_GRAPH_LEADERTRANSFERPROCESSOR_H
#define NEBULA_GRAPH_LEADERTRANSFERPROCESSOR_H


#include <memory>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {
/**
 * @brief Transfer leaders of specified space from specified host
 */
class LeaderTransferProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
  static LeaderTransferProcessor *instance(kvstore::KVStore *kvstore) {
    static std::unique_ptr<AdminClient> client(new AdminClient(kvstore));
    static std::unique_ptr<LeaderTransferProcessor> processor(new LeaderTransferProcessor(kvstore, client.get()));
    return new LeaderTransferProcessor(kvstore, client.get());
  }

  void process(const cpp2::LeaderTransferReq &req);

  nebula::cpp2::ErrorCode leaderTransfer(const meta::cpp2::LeaderTransferReq &req);

  nebula::cpp2::ErrorCode buildLeaderTransferPlan(
    const HostAddr &source,
    HostLeaderMap *hostLeaderMap,
    GraphSpaceID spaceId,
    std::vector<std::pair<PartitionID, HostAddr> > &plan) const;

private:
  LeaderTransferProcessor(kvstore::KVStore *kvstore, AdminClient *client) : BaseProcessor<cpp2::ExecResp>(kvstore),
                                                                            kv_(kvstore), client_(client) {
    executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(1);
  }

  std::atomic_bool running_{false};
  kvstore::KVStore *kv_{nullptr};
  AdminClient *client_{nullptr};
  std::unique_ptr<HostLeaderMap> hostLeaderMap_;
  std::atomic_bool inLeaderBalance_{false};
  std::unique_ptr<folly::Executor> executor_;
};
}
}

#endif  // NEBULA_GRAPH_LEADERTRANSFERPROCESSOR_H
