/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/maintain/EdgeIndexExecutor.h"
#include "planner/plan/Maintain.h"
#include "util/IndexUtil.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateEdgeIndexExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *ceiNode = asNode<CreateEdgeIndex>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()
        ->getMetaClient()
        ->createEdgeIndex(spaceId,
                          ceiNode->getIndexName(),
                          ceiNode->getSchemaName(),
                          ceiNode->getFields(),
                          ceiNode->getIfNotExists(),
                          ceiNode->getComment())
        .via(runner())
        .thenValue([ceiNode, spaceId](StatusOr<IndexID> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId << ", Create index `"
                           << ceiNode->getIndexName() << "' at edge: `" << ceiNode->getSchemaName()
                           << "' failed: " << resp.status();
                return resp.status();
            }
            return Status::OK();
        });
}

folly::Future<Status> DropEdgeIndexExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *deiNode = asNode<DropEdgeIndex>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()
        ->getMetaClient()
        ->dropEdgeIndex(spaceId, deiNode->getIndexName(), deiNode->getIfExists())
        .via(runner())
        .thenValue([deiNode, spaceId](StatusOr<IndexID> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId << ", Drop edge index`"
                           << deiNode->getIndexName() << "' failed: " << resp.status();
                return resp.status();
            }
            return Status::OK();
        });
}

folly::Future<Status> DescEdgeIndexExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *deiNode = asNode<DescEdgeIndex>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()
        ->getMetaClient()
        ->getEdgeIndex(spaceId, deiNode->getIndexName())
        .via(runner())
        .thenValue([this, deiNode, spaceId](StatusOr<meta::cpp2::IndexItem> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId << ", Desc edge index`"
                           << deiNode->getIndexName() << "' failed: " << resp.status();
                return resp.status();
            }

            auto ret = IndexUtil::toDescIndex(resp.value());
            if (!ret.ok()) {
                LOG(ERROR) << ret.status();
                return ret.status();
            }
            return finish(ResultBuilder()
                              .value(std::move(ret).value())
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

folly::Future<Status> ShowCreateEdgeIndexExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *sceiNode = asNode<ShowCreateEdgeIndex>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()
        ->getMetaClient()
        ->getEdgeIndex(spaceId, sceiNode->getIndexName())
        .via(runner())
        .thenValue([this, sceiNode, spaceId](StatusOr<meta::cpp2::IndexItem> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId << ", Show create edge index `"
                           << sceiNode->getIndexName() << "' failed: " << resp.status();
                return resp.status();
            }
            auto ret = IndexUtil::toShowCreateIndex(false, sceiNode->getIndexName(), resp.value());
            if (!ret.ok()) {
                LOG(ERROR) << ret.status();
                return ret.status();
            }
            return finish(ResultBuilder()
                              .value(std::move(ret).value())
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

folly::Future<Status> ShowEdgeIndexesExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *iNode = asNode<ShowEdgeIndexes>(node());
    const auto& bySchema = iNode->name();
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->listEdgeIndexes(spaceId).via(runner()).thenValue(
        [this, spaceId, bySchema](StatusOr<std::vector<meta::cpp2::IndexItem>> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId << ", Show edge indexes failed"
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
            std::map<std::string, std::pair<std::string, std::vector<std::string>>> ids;
            for (auto &edgeIndex : edgeIndexItems) {
                const auto& sch = edgeIndex.get_schema_name();
                const auto& cols = edgeIndex.get_fields();
                std::vector<std::string> colsName;
                for (const auto& col : cols) {
                    colsName.emplace_back(col.get_name());
                }
                ids[edgeIndex.get_index_name()] = {sch, std::move(colsName)};
            }
            for (const auto& i : ids) {
                if (!bySchema.empty() && bySchema != i.second.first) {
                    continue;
                }
                Row row;
                row.values.emplace_back(i.first);
                if (bySchema.empty()) {
                    row.values.emplace_back(i.second.first);
                }
                List list;
                for (const auto& c : i.second.second) {
                    list.values.emplace_back(c);
                }
                row.values.emplace_back(std::move(list));
                dataSet.rows.emplace_back(std::move(row));
            }
            return finish(ResultBuilder()
                              .value(Value(std::move(dataSet)))
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

folly::Future<Status> ShowEdgeIndexStatusExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->listEdgeIndexStatus(spaceId).via(runner()).thenValue(
        [this, spaceId](StatusOr<std::vector<meta::cpp2::IndexStatus>> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId << ", Show edge index status failed"
                           << resp.status();
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
                              .finish());
        });
}

}   // namespace graph
}   // namespace nebula
