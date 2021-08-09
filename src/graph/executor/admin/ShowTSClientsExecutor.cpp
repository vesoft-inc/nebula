/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/interface/gen-cpp2/meta_types.h"

#include "context/QueryContext.h"
#include "executor/admin/ShowTSClientsExecutor.h"
#include "planner/plan/Admin.h"
#include "service/PermissionManager.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowTSClientsExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return showTSClients();
}

folly::Future<Status> ShowTSClientsExecutor::showTSClients() {
return qctx()
        ->getMetaClient()
        ->listFTClients()
        .via(runner())
        .thenValue([this](auto &&resp) {
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            auto value = std::move(resp).value();
            DataSet v({"Host", "Port"});
            for (const auto &client : value) {
                nebula::Row r({client.host.host, client.host.port});
                v.emplace_back(std::move(r));
            }
            return finish(std::move(v));
        });
}

}   // namespace graph
}   // namespace nebula
