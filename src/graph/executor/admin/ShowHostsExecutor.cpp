/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/ShowHostsExecutor.h"

#include <folly/Try.h>                      // for Try::~Try<T>
#include <folly/futures/Future.h>           // for Future::Future<T>, Future...
#include <folly/futures/Promise.h>          // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>          // for PromiseException::Promise...
#include <folly/futures/Promise.h>          // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>          // for PromiseException::Promise...
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for optional_field_ref

#include <algorithm>      // for max
#include <cstddef>        // for size_t
#include <cstdint>        // for int64_t
#include <map>            // for map, operator!=, _Rb_tree...
#include <ostream>        // for operator<<, stringstream
#include <string>         // for string, basic_string, all...
#include <unordered_map>  // for unordered_map, operator!=
#include <utility>        // for pair, move, forward
#include <vector>         // for vector

#include "clients/meta/MetaClient.h"        // for MetaClient
#include "common/base/Logging.h"            // for LOG, LogMessage, _LOG_ERROR
#include "common/base/Status.h"             // for Status, operator<<
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/datatypes/DataSet.h"       // for Row, DataSet
#include "common/datatypes/HostAddr.h"      // for HostAddr
#include "common/datatypes/List.h"          // for List
#include "common/datatypes/Value.h"         // for Value
#include "common/thrift/ThriftTypes.h"      // for PartitionID
#include "common/time/ScopedTimer.h"        // for SCOPED_TIMER
#include "graph/context/QueryContext.h"     // for QueryContext
#include "graph/planner/plan/Admin.h"       // for ShowHosts
#include "interface/gen-cpp2/meta_types.h"  // for HostItem, ListHostType

namespace nebula {
namespace graph {

folly::Future<Status> ShowHostsExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return showHosts();
}

folly::Future<Status> ShowHostsExecutor::showHosts() {
  static constexpr char kNoPartition[] = "No valid partition";
  static constexpr char kPartitionDelimiter[] = ", ";

  auto *shNode = asNode<ShowHosts>(node());
  auto makeTraditionalResult = [&](const std::vector<meta::cpp2::HostItem> &hostVec) -> DataSet {
    DataSet v({"Host",
               "Port",
               "Status",
               "Leader count",
               "Leader distribution",
               "Partition distribution",
               "Version"});

    std::map<std::string, int64_t> leaderPartsCount;
    std::map<std::string, int64_t> allPartsCount;
    int64_t totalLeader = 0;

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

      auto &leaderParts = host.get_leader_parts();
      std::map<std::string, std::vector<PartitionID>> lPartsCount(leaderParts.begin(),
                                                                  leaderParts.end());

      for (const auto &l : lPartsCount) {
        leaderPartsCount[l.first] += l.second.size();
        leaders << l.first << ":" << l.second.size();
        if (i < lPartsCount.size() - 1) {
          leaders << kPartitionDelimiter;
        }
        ++i;
      }
      if (lPartsCount.empty()) {
        leaders << kNoPartition;
      }

      i = 0;
      auto &allParts = host.get_all_parts();
      std::map<std::string, std::vector<PartitionID>> aPartsCount(allParts.begin(), allParts.end());
      for (const auto &p : aPartsCount) {
        allPartsCount[p.first] += p.second.size();
        parts << p.first << ":" << p.second.size();
        if (i < aPartsCount.size() - 1) {
          parts << kPartitionDelimiter;
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
      r.emplace_back(host.version_ref().has_value() ? Value(*host.version_ref()) : Value());
      v.emplace_back(std::move(r));
    }  // row loop
    {
      // set sum of leader/partitions of all hosts
      nebula::Row r({"Total", Value(), Value()});
      r.emplace_back(totalLeader);

      std::stringstream leaders;
      std::stringstream parts;
      std::size_t i = 0;
      for (const auto &spaceEntry : leaderPartsCount) {
        leaders << spaceEntry.first << ":" << spaceEntry.second;
        if (i < leaderPartsCount.size() - 1) {
          leaders << kPartitionDelimiter;
        }
        ++i;
      }
      i = 0;
      for (const auto &spaceEntry : allPartsCount) {
        parts << spaceEntry.first << ":" << spaceEntry.second;
        if (i < allPartsCount.size() - 1) {
          parts << kPartitionDelimiter;
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
    }  // row loop
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
        auto value = std::forward<decltype(resp)>(resp).value();
        if (type == meta::cpp2::ListHostType::ALLOC) {
          return finish(makeTraditionalResult(value));
        }
        return finish(makeGitInfoResult(value));
      });
}

}  // namespace graph
}  // namespace nebula
