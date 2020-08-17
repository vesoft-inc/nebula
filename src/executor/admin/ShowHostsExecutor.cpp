/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/admin/ShowHostsExecutor.h"
#include "planner/Admin.h"
#include "context/QueryContext.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowHostsExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    return showHosts().ensure([this]() { UNUSED(this); });
}

folly::Future<Status> ShowHostsExecutor::showHosts() {
    static constexpr char kNoPartition[]        = "No valid partition";
    static constexpr char kPartitionDelimeter[] = ", ";
    return qctx()
        ->getMetaClient()
        ->listHosts()
        .via(runner())
        .then([this](auto &&resp) {
            if (!resp.ok()) {
                LOG(ERROR) << resp.status();
                return resp.status();
            }
            auto    value = std::move(resp).value();
            DataSet v({"Host",
                       "Port",
                       "Status",
                       "Leader count",
                       "Leader distribution",
                       "Partition distribution"});
            for (const auto &host : value) {
                nebula::Row r({host.get_hostAddr().host,
                               host.get_hostAddr().port,
                               meta::cpp2::_HostStatus_VALUES_TO_NAMES.at(host.get_status()),
                               static_cast<int64_t>(host.get_leader_parts().size())});
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
                r.emplace_back(leaders.str());
                r.emplace_back(parts.str());
                v.emplace_back(std::move(r));
            }  // row loop
            return finish(std::move(v));
        });
}

}  // namespace graph
}  // namespace nebula
