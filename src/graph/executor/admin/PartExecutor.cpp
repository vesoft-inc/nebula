/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/PartExecutor.h"

#include <folly/Try.h>                 // for Try::~Try<T>
#include <folly/futures/Future.h>      // for Future::Future<T>, Future...
#include <folly/futures/Promise.h>     // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>     // for PromiseException::Promise...
#include <folly/futures/Promise.h>     // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>     // for PromiseException::Promise...
#include <thrift/lib/cpp2/FieldRef.h>  // for optional_field_ref

#include <algorithm>    // for max, sort
#include <string>       // for string, basic_string
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move
#include <vector>       // for vector

#include "clients/meta/MetaClient.h"        // for MetaClient
#include "common/base/Logging.h"            // for LOG, LogMessage, _LOG_ERROR
#include "common/base/Status.h"             // for Status, operator<<
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/datatypes/DataSet.h"       // for Row, DataSet
#include "common/datatypes/HostAddr.h"      // for HostAddr
#include "common/datatypes/List.h"          // for List
#include "common/datatypes/Value.h"         // for Value
#include "common/network/NetworkUtils.h"    // for NetworkUtils
#include "common/thrift/ThriftTypes.h"      // for PartitionID
#include "common/time/ScopedTimer.h"        // for SCOPED_TIMER
#include "graph/context/Iterator.h"         // for Iterator, Iterator::Kind
#include "graph/context/QueryContext.h"     // for QueryContext
#include "graph/context/Result.h"           // for ResultBuilder
#include "graph/planner/plan/Admin.h"       // for ShowParts
#include "interface/gen-cpp2/meta_types.h"  // for PartItem, swap

using nebula::network::NetworkUtils;

namespace nebula {
namespace graph {
folly::Future<Status> ShowPartsExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto* spNode = asNode<ShowParts>(node());
  return qctx()
      ->getMetaClient()
      ->listParts(spNode->getSpaceId(), spNode->getPartIds())
      .via(runner())
      .thenValue([this](StatusOr<std::vector<meta::cpp2::PartItem>> resp) {
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }
        auto partItems = std::move(resp).value();

        std::sort(partItems.begin(), partItems.end(), [](const auto& a, const auto& b) {
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
                          .build());
      });
}
}  // namespace graph
}  // namespace nebula
