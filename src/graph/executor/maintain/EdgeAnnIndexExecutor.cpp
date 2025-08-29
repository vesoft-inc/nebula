// Copyright (c) 2025 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/maintain/EdgeAnnIndexExecutor.h"

#include "graph/planner/plan/Maintain.h"
#include "graph/util/IndexUtil.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateEdgeAnnIndexExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *ctiNode = asNode<CreateEdgeAnnIndex>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->createEdgeAnnIndex(spaceId,
                           ctiNode->getIndexName(),
                           ctiNode->getSchemaNames(),
                           ctiNode->getField(),
                           ctiNode->getIfNotExists(),
                           ctiNode->getAnnIndexParam(),
                           ctiNode->getComment())
      .via(runner())
      .thenValue([ctiNode, spaceId](StatusOr<IndexID> resp) {
        memory::MemoryCheckGuard guard;
        if (!resp.ok()) {
          LOG(WARNING) << "SpaceId: " << spaceId << ", Create Ann index `"
                       << ctiNode->getIndexName() << "' at tagS: `"
                       << folly::join(",", ctiNode->getSchemaNames())
                       << "' failed: " << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> DropEdgeAnnIndexExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *deiNode = asNode<DropEdgeAnnIndex>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->dropEdgeAnnIndex(spaceId, deiNode->getIndexName(), deiNode->getIfExists())
      .via(runner())
      .thenValue([deiNode, spaceId](StatusOr<IndexID> resp) {
        memory::MemoryCheckGuard guard;
        if (!resp.ok()) {
          LOG(WARNING) << "SpaceId: " << spaceId << ", Drop edge ann index`"
                       << deiNode->getIndexName() << "' failed: " << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> DescEdgeAnnIndexExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *deiNode = asNode<DescEdgeAnnIndex>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->getEdgeAnnIndex(spaceId, deiNode->getIndexName())
      .via(runner())
      .thenValue([this, deiNode, spaceId](StatusOr<meta::cpp2::AnnIndexItem> resp) {
        memory::MemoryCheckGuard guard;
        if (!resp.ok()) {
          LOG(WARNING) << "SpaceId: " << spaceId << ", Desc ann edge index`"
                       << deiNode->getIndexName() << "' failed: " << resp.status();
          return resp.status();
        }

        auto ret = IndexUtil::toDescIndex(resp.value());
        if (!ret.ok()) {
          LOG(WARNING) << "SpaceId: " << spaceId << ", Desc ann edge index`"
                       << deiNode->getIndexName() << "' failed: " << resp.status();
          return ret.status();
        }
        return finish(
            ResultBuilder().value(std::move(ret).value()).iter(Iterator::Kind::kDefault).build());
      });
}

folly::Future<Status> ShowEdgeAnnIndexStatusExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()->getMetaClient()->listEdgeAnnIndexStatus(spaceId).via(runner()).thenValue(
      [this, spaceId](StatusOr<std::vector<meta::cpp2::IndexStatus>> resp) {
        memory::MemoryCheckGuard guard;
        if (!resp.ok()) {
          LOG(WARNING) << "SpaceId: " << spaceId
                       << ", Show edge ann index status failed: " << resp.status();
          return resp.status();
        }

        auto indexStatuses = std::move(resp).value();

        DataSet dataSet;
        dataSet.colNames = {"Name", "Index Status"};
        for (auto &indexStatus : indexStatuses) {
          Row row;
          row.values.emplace_back(std::move(indexStatus.get_name()));
          row.values.emplace_back(std::move(indexStatus.get_status()));
          dataSet.rows.emplace_back(std::move(row));
        }
        return finish(ResultBuilder()
                          .value(Value(std::move(dataSet)))
                          .iter(Iterator::Kind::kDefault)
                          .build());
      });
}

folly::Future<Status> ShowCreateEdgeAnnIndexExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  auto *sctiNode = asNode<ShowCreateEdgeAnnIndex>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->getEdgeAnnIndex(spaceId, sctiNode->getIndexName())
      .via(runner())
      .thenValue([this, sctiNode, spaceId](StatusOr<meta::cpp2::AnnIndexItem> resp) {
        memory::MemoryCheckGuard guard;
        if (!resp.ok()) {
          LOG(WARNING) << "SpaceId: " << spaceId << ", Show create edge ann index `"
                       << sctiNode->getIndexName() << "' failed: " << resp.status();
          return resp.status();
        }
        auto ret = IndexUtil::toShowCreateIndex(false, sctiNode->getIndexName(), resp.value());
        if (!ret.ok()) {
          LOG(WARNING) << "SpaceId: " << spaceId << ", Show create edge ann index `"
                       << sctiNode->getIndexName() << "' failed: " << resp.status();
          return ret.status();
        }
        return finish(
            ResultBuilder().value(std::move(ret).value()).iter(Iterator::Kind::kDefault).build());
      });
}

folly::Future<Status> ShowEdgeAnnIndexesExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *iNode = asNode<ShowEdgeAnnIndexes>(node());
  const auto &bySchema = iNode->name();
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()->getMetaClient()->listEdgeAnnIndexes(spaceId).via(runner()).thenValue(
      [this, spaceId, bySchema](StatusOr<std::vector<meta::cpp2::AnnIndexItem>> resp) {
        memory::MemoryCheckGuard guard;
        if (!resp.ok()) {
          LOG(WARNING) << "SpaceId: " << spaceId << ", Show edge ann indexes failed"
                       << resp.status();
          return resp.status();
        }

        auto edgeIndexItems = std::move(resp).value();

        DataSet dataSet;
        dataSet.colNames.emplace_back("Index Name");
        if (bySchema.empty()) {
          dataSet.colNames.emplace_back("By Edge");
        }
        dataSet.colNames.emplace_back("Columns");

        std::map<std::string, std::pair<std::vector<std::string>, std::vector<std::string>>> ids;
        for (auto &edgeIndex : edgeIndexItems) {
          const auto &schemas = edgeIndex.get_schema_names();
          const auto &prop = edgeIndex.get_prop_name();
          std::vector<std::string> colsName;
          std::vector<std::string> schemaNames;
          for (const auto &schema : schemas) {
            colsName.emplace_back(schema + "." + prop.c_str());
            schemaNames.emplace_back(schema);
          }
          // For now, just use the first schema name
          std::string sch = schemas.empty() ? "" : schemas[0];
          ids[edgeIndex.get_index_name()] =
              std::make_pair(std::move(schemaNames), std::move(colsName));
        }
        for (const auto &i : ids) {
          if (!bySchema.empty() &&
              std::any_of(i.second.first.begin(),
                          i.second.first.end(),
                          [&bySchema](const std::string &s) { return s != bySchema; })) {
            continue;
          }
          Row row;
          row.values.emplace_back(i.first);
          if (bySchema.empty()) {
            List schemaList;
            for (const auto &schema : i.second.first) {
              schemaList.values.emplace_back(schema);
            }
            row.values.emplace_back(std::move(schemaList));
          }
          List list;
          for (const auto &c : i.second.second) {
            list.values.emplace_back(c);
          }
          row.values.emplace_back(std::move(list));
          dataSet.rows.emplace_back(std::move(row));
        }
        return finish(ResultBuilder()
                          .value(Value(std::move(dataSet)))
                          .iter(Iterator::Kind::kDefault)
                          .build());
      });
}

}  // namespace graph
}  // namespace nebula
