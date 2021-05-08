/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "executor/admin/PartExecutor.h"
#include "planner/plan/Admin.h"
#include "context/QueryContext.h"
#include "util/ScopedTimer.h"

using nebula::network::NetworkUtils;

namespace nebula {
namespace graph {
folly::Future<Status> ShowPartsExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *spNode = asNode<ShowParts>(node());
    return qctx()->getMetaClient()->listParts(spNode->getSpaceId(), spNode->getPartIds())
            .via(runner())
            .thenValue([this](StatusOr<std::vector<meta::cpp2::PartItem>> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto partItems = std::move(resp).value();

                std::sort(partItems.begin(), partItems.end(),
                      [] (const auto& a, const auto& b) {
                          return a.get_part_id() < b.get_part_id();
                      });

                DataSet dataSet({"Partition ID", "Leader", "Peers", "Losts"});
                for (auto& item : partItems) {
                    Row row;
                    row.values.resize(4);
                    row.values[0].setInt(item.get_part_id());

                    if (item.leader_ref().has_value()) {
                        std::string leaderStr = NetworkUtils::toHostsStr({*item.leader_ref()});
                        row.values[1].setStr(std::move(leaderStr));
                    } else {
                        row.values[1].setStr("");
                    }

                    row.values[2].setStr(NetworkUtils::toHostsStr(item.get_peers()));
                    row.values[3].setStr(NetworkUtils::toHostsStr(item.get_losts()));
                    dataSet.emplace_back(std::move(row));
                }
                return finish(ResultBuilder()
                                  .value(Value(std::move(dataSet)))
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
            });
}
}   // namespace graph
}   // namespace nebula
