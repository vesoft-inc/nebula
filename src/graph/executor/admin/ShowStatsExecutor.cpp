/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/admin/ShowStatsExecutor.h"

#include <folly/Try.h>                 // for Try::~Try<T>
#include <folly/futures/Future.h>      // for Future::Future<T>, Future...
#include <folly/futures/Promise.h>     // for Promise::Promise<T>, Prom...
#include <folly/futures/Promise.h>     // for PromiseException::Promise...
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <algorithm>      // for max, sort
#include <cstdint>        // for int64_t
#include <ostream>        // for operator<<, basic_ostream...
#include <string>         // for string, basic_string, ope...
#include <type_traits>    // for remove_reference<>::type
#include <unordered_map>  // for _Node_const_iterator, ope...
#include <utility>        // for pair, move, make_pair
#include <vector>         // for vector

#include "clients/meta/MetaClient.h"        // for MetaClient
#include "common/base/Logging.h"            // for LOG, LogMessage, _LOG_ERROR
#include "common/base/Status.h"             // for Status, operator<<
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/datatypes/DataSet.h"       // for Row, DataSet
#include "common/datatypes/Value.h"         // for Value
#include "common/time/ScopedTimer.h"        // for SCOPED_TIMER
#include "graph/context/Iterator.h"         // for Iterator, Iterator::Kind
#include "graph/context/QueryContext.h"     // for QueryContext
#include "graph/context/Result.h"           // for ResultBuilder
#include "graph/service/RequestContext.h"   // for RequestContext
#include "graph/session/ClientSession.h"    // for ClientSession, SpaceInfo
#include "interface/gen-cpp2/meta_types.h"  // for StatsItem

namespace nebula {
namespace graph {

folly::Future<Status> ShowStatsExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()->getMetaClient()->getStats(spaceId).via(runner()).thenValue(
      [this, spaceId](StatusOr<meta::cpp2::StatsItem> resp) {
        if (!resp.ok()) {
          LOG(ERROR) << "SpaceId: " << spaceId << ", Show status failed: " << resp.status();
          return resp.status();
        }
        auto statsItem = std::move(resp).value();

        DataSet dataSet({"Type", "Name", "Count"});
        std::vector<std::pair<std::string, int64_t>> tagCount;
        std::vector<std::pair<std::string, int64_t>> edgeCount;

        for (auto& tag : statsItem.get_tag_vertices()) {
          tagCount.emplace_back(std::make_pair(tag.first, tag.second));
        }
        for (auto& edge : statsItem.get_edges()) {
          edgeCount.emplace_back(std::make_pair(edge.first, edge.second));
        }

        std::sort(tagCount.begin(), tagCount.end(), [](const auto& l, const auto& r) {
          return l.first < r.first;
        });
        std::sort(edgeCount.begin(), edgeCount.end(), [](const auto& l, const auto& r) {
          return l.first < r.first;
        });

        for (auto& tagElem : tagCount) {
          Row row;
          row.values.emplace_back("Tag");
          row.values.emplace_back(tagElem.first);
          row.values.emplace_back(tagElem.second);
          dataSet.rows.emplace_back(std::move(row));
        }

        for (auto& edgeElem : edgeCount) {
          Row row;
          row.values.emplace_back("Edge");
          row.values.emplace_back(edgeElem.first);
          row.values.emplace_back(edgeElem.second);
          dataSet.rows.emplace_back(std::move(row));
        }

        Row verticeRow;
        verticeRow.values.emplace_back("Space");
        verticeRow.values.emplace_back("vertices");
        verticeRow.values.emplace_back(*statsItem.space_vertices_ref());
        dataSet.rows.emplace_back(std::move(verticeRow));

        Row edgeRow;
        edgeRow.values.emplace_back("Space");
        edgeRow.values.emplace_back("edges");
        edgeRow.values.emplace_back(*statsItem.space_edges_ref());
        dataSet.rows.emplace_back(std::move(edgeRow));

        return finish(ResultBuilder()
                          .value(Value(std::move(dataSet)))
                          .iter(Iterator::Kind::kDefault)
                          .build());
      });
}

}  // namespace graph
}  // namespace nebula
