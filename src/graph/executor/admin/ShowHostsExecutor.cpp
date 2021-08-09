/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "executor/admin/ShowHostsExecutor.h"
#include "context/QueryContext.h"
#include "planner/plan/Admin.h"
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
    auto makeTraditionalResult = [&](const std::vector<meta::cpp2::HostItem> &hostVec) -> DataSet {
        DataSet v({"Host",
                   "Port",
                   "Status",
                   "Leader count",
                   "Leader distribution",
                   "Partition distribution"});

        std::map<std::string, int64_t> leaderPartsCount;
        std::map<std::string, int64_t> allPartsCount;
        int64_t  totalLeader = 0;

        for (const auto &host : hostVec) {
            nebula::Row r({host.get_hostAddr().host,
                           host.get_hostAddr().port,
                           apache::thrift::util::enumNameSafe(host.get_status())});
            int64_t leaderCount = 0;
            for (const auto &spaceEntry : host.get_leader_parts()) {
                leaderCount += spaceEntry.second.size();
            }
            std::stringstream leaders;
            std::stringstream parts;
            std::size_t i = 0;

            auto& leaderParts = host.get_leader_parts();
            std::map<std::string, std::vector<PartitionID>> lPartsCount(leaderParts.begin(),
                                                                        leaderParts.end());

            for (const auto &l : lPartsCount) {
                leaderPartsCount[l.first] += l.second.size();
                leaders << l.first << ":" << l.second.size();
                if (i < lPartsCount.size() - 1) {
                    leaders << kPartitionDelimeter;
                }
                ++i;
            }
            if (lPartsCount.empty()) {
                leaders << kNoPartition;
            }

            i = 0;
            auto& allParts = host.get_all_parts();
            std::map<std::string, std::vector<PartitionID>> aPartsCount(allParts.begin(),
                                                                        allParts.end());
            for (const auto &p : aPartsCount) {
                allPartsCount[p.first] += p.second.size();
                parts << p.first << ":" << p.second.size();
                if (i < aPartsCount.size() - 1) {
                    parts << kPartitionDelimeter;
                }
                ++i;
            }
            if (host.get_all_parts().empty()) {
                parts << kNoPartition;
            }
            totalLeader += leaderCount;
            r.emplace_back(leaderCount);
            r.emplace_back(leaders.str());
            r.emplace_back(parts.str());
            v.emplace_back(std::move(r));
        }   // row loop
        {
            // set sum of leader/partitions of all hosts
            nebula::Row r({"Total",
                           Value(),
                           Value()});
            r.emplace_back(totalLeader);

            std::stringstream leaders;
            std::stringstream parts;
            std::size_t i = 0;
            for (const auto &spaceEntry : leaderPartsCount) {
                leaders << spaceEntry.first << ":" << spaceEntry.second;
                if (i < leaderPartsCount.size() - 1) {
                    leaders << kPartitionDelimeter;
                }
                ++i;
            }
            i = 0;
            for (const auto &spaceEntry : allPartsCount) {
                parts << spaceEntry.first << ":" << spaceEntry.second;
                if (i < allPartsCount.size() - 1) {
                    parts << kPartitionDelimeter;
                }
                ++i;
            }

            if (leaders.str().empty()) {
                r.emplace_back(Value());
            } else {
                r.emplace_back(leaders.str());
            }

            if (parts.str().empty()) {
                r.emplace_back(Value());
            } else {
                r.emplace_back(parts.str());
            }
            v.emplace_back(std::move(r));
        }
        return v;
    };

    auto makeGitInfoResult = [&](const std::vector<meta::cpp2::HostItem> &hostVec) -> DataSet {
        DataSet v({"Host", "Port", "Status", "Role", "Git Info Sha", "Version"});
        for (const auto &host : hostVec) {
            nebula::Row r({host.get_hostAddr().host,
                           host.get_hostAddr().port,
                           apache::thrift::util::enumNameSafe(host.get_status()),
                           apache::thrift::util::enumNameSafe(host.get_role()),
                           host.get_git_info_sha()});
            // empty for non-versioned
            r.emplace_back(host.version_ref().has_value() ? Value(*host.version_ref()) : Value());
            v.emplace_back(std::move(r));
        }   // row loop
        return v;
    };

    return qctx()
        ->getMetaClient()
        ->listHosts(shNode->getType())
        .via(runner())
        .thenValue([=, type = shNode->getType()](auto &&resp) {
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            auto value = std::move(resp).value();
            if (type == meta::cpp2::ListHostType::ALLOC) {
                return finish(makeTraditionalResult(value));
            }
            return finish(makeGitInfoResult(value));
        });
}

}   // namespace graph
}   // namespace nebula
