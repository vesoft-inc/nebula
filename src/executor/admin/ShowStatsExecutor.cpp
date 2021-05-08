/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "executor/admin/ShowStatsExecutor.h"
#include "context/QueryContext.h"
#include "service/PermissionManager.h"
#include "planner/plan/Admin.h"
#include "util/SchemaUtil.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> ShowStatsExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->getStatis(spaceId).via(runner()).thenValue(
        [this, spaceId](StatusOr<meta::cpp2::StatisItem> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId
                           << ", Show staus failed: " << resp.status();
                return resp.status();
            }
            auto statisItem = std::move(resp).value();

            DataSet dataSet({"Type", "Name", "Count"});
            std::vector<std::pair<std::string, int64_t>> tagCount;
            std::vector<std::pair<std::string, int64_t>> edgeCount;

            for (auto &tag : statisItem.get_tag_vertices()) {
                tagCount.emplace_back(std::make_pair(tag.first, tag.second));
            }
            for (auto &edge : statisItem.get_edges()) {
                edgeCount.emplace_back(std::make_pair(edge.first, edge.second));
            }

            std::sort(tagCount.begin(), tagCount.end(), [](const auto& l, const auto& r) {
                return l.first < r.first;
            });
            std::sort(edgeCount.begin(), edgeCount.end(), [](const auto& l, const auto& r) {
                return l.first < r.first;
            });

            for (auto &tagElem : tagCount) {
                Row row;
                row.values.emplace_back("Tag");
                row.values.emplace_back(tagElem.first);
                row.values.emplace_back(tagElem.second);
                dataSet.rows.emplace_back(std::move(row));
            }

            for (auto &edgeElem : edgeCount) {
                Row row;
                row.values.emplace_back("Edge");
                row.values.emplace_back(edgeElem.first);
                row.values.emplace_back(edgeElem.second);
                dataSet.rows.emplace_back(std::move(row));
            }

            Row verticeRow;
            verticeRow.values.emplace_back("Space");
            verticeRow.values.emplace_back("vertices");
            verticeRow.values.emplace_back(*statisItem.space_vertices_ref());
            dataSet.rows.emplace_back(std::move(verticeRow));

            Row edgeRow;
            edgeRow.values.emplace_back("Space");
            edgeRow.values.emplace_back("edges");
            edgeRow.values.emplace_back(*statisItem.space_edges_ref());
            dataSet.rows.emplace_back(std::move(edgeRow));

            return finish(ResultBuilder()
                              .value(Value(std::move(dataSet)))
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

}   // namespace graph
}   // namespace nebula
