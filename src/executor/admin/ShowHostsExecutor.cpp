/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/ShowHostsExecutor.h"
#include "context/QueryContext.h"
#include "planner/Admin.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowHostsExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return showHosts();
}

folly::Future<Status> ShowHostsExecutor::showHosts() {
    static constexpr char kNoPartition[] = "No valid partition";
    static constexpr char kPartitionDelimeter[] = ", ";

    auto *shNode = asNode<ShowHosts>(node());
    auto makeTranditionalResult = [&](const std::vector<meta::cpp2::HostItem> &hostVec) -> DataSet {
        DataSet v({"Host",
                   "Port",
                   "Status",
                   "Leader count",
                   "Leader distribution",
                   "Partition distribution"});
        for (const auto &host : hostVec) {
            nebula::Row r({host.get_hostAddr().host,
                           host.get_hostAddr().port,
                           meta::cpp2::_HostStatus_VALUES_TO_NAMES.at(host.get_status())});
            int64_t leaderCount = 0;
            for (const auto &spaceEntry : host.get_leader_parts()) {
                leaderCount += spaceEntry.second.size();
            }
            std::stringstream leaders;
            std::stringstream parts;
            std::size_t i = 0;
            for (const auto &l : host.get_leader_parts()) {
                leaders << l.first << ":" << l.second.size();
                if (i < host.get_leader_parts().size() - 1) {
                    leaders << kPartitionDelimeter;
                }
                ++i;
            }
            if (host.get_leader_parts().empty()) {
                leaders << kNoPartition;
            }
            i = 0;
            for (const auto &p : host.get_all_parts()) {
                parts << p.first << ":" << p.second.size();
                if (i < host.get_all_parts().size() - 1) {
                    parts << kPartitionDelimeter;
                }
                ++i;
            }
            if (host.get_all_parts().empty()) {
                parts << kNoPartition;
            }
            r.emplace_back(leaderCount);
            r.emplace_back(leaders.str());
            r.emplace_back(parts.str());
            v.emplace_back(std::move(r));
        }   // row loop
        return v;
    };

    auto makeGitInfoResult = [&](const std::vector<meta::cpp2::HostItem> &hostVec) -> DataSet {
        DataSet v({"Host", "Port", "Status", "Role", "Git Info Sha"});
        for (const auto &host : hostVec) {
            nebula::Row r({host.get_hostAddr().host,
                           host.get_hostAddr().port,
                           meta::cpp2::_HostStatus_VALUES_TO_NAMES.at(host.get_status()),
                           meta::cpp2::_HostRole_VALUES_TO_NAMES.at(host.get_role()),
                           host.get_git_info_sha()});
            v.emplace_back(std::move(r));
        }   // row loop
        return v;
    };

    return qctx()
        ->getMetaClient()
        ->listHosts(shNode->getType())
        .via(runner())
        .then([=, type = shNode->getType()](auto &&resp) {
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            auto value = std::move(resp).value();
            if (type == meta::cpp2::ListHostType::ALLOC) {
                return finish(makeTranditionalResult(value));
            }
            return finish(makeGitInfoResult(value));
        });
}

}   // namespace graph
}   // namespace nebula
