/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/maintain/EdgeExecutor.h"
#include "context/QueryContext.h"
#include "planner/plan/Maintain.h"
#include "util/SchemaUtil.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateEdgeExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *ceNode = asNode<CreateEdge>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->createEdgeSchema(spaceId,
            ceNode->getName(), ceNode->getSchema(), ceNode->getIfNotExists())
            .via(runner())
            .thenValue([ceNode, spaceId](StatusOr<EdgeType> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "SpaceId: " << spaceId
                               << ", Create edge `" << ceNode->getName()
                               << "' failed: " << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}


folly::Future<Status> DescEdgeExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *deNode = asNode<DescEdge>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->getEdgeSchema(spaceId, deNode->getName())
            .via(runner())
            .thenValue([this, deNode, spaceId](StatusOr<meta::cpp2::Schema> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << resp.status();
                    return resp.status();
                }
                auto ret = SchemaUtil::toDescSchema(resp.value());
                if (!ret.ok()) {
                    LOG(ERROR) << "SpaceId: " << spaceId
                               << ", Desc edge `" << deNode->getName()
                               << "' failed: " << resp.status();
                    return ret.status();
                }
                return finish(ResultBuilder()
                                  .value(Value(std::move(ret).value()))
                                  .iter(Iterator::Kind::kDefault)
                                  .finish());
            });
}


folly::Future<Status> DropEdgeExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *deNode = asNode<DropEdge>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->dropEdgeSchema(spaceId,
                                                   deNode->getName(),
                                                   deNode->getIfExists())
            .via(runner())
            .thenValue([deNode, spaceId](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "SpaceId: " << spaceId
                               << ", Drop edge `" << deNode->getName()
                               << "' failed: " << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> ShowEdgesExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->listEdgeSchemas(spaceId).via(runner()).thenValue(
        [this, spaceId](StatusOr<std::vector<meta::cpp2::EdgeItem>> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId
                           << ", Show edges failed: " << resp.status();
                return resp.status();
            }
            auto edgeItems = std::move(resp).value();

            DataSet dataSet;
            dataSet.colNames = {"Name"};
            std::set<std::string> orderEdgeNames;
            for (auto &edge : edgeItems) {
                orderEdgeNames.emplace(edge.get_edge_name());
            }
            for (auto &name : orderEdgeNames) {
                Row row;
                row.values.emplace_back(name);
                dataSet.rows.emplace_back(std::move(row));
            }
            return finish(ResultBuilder()
                              .value(Value(std::move(dataSet)))
                              .iter(Iterator::Kind::kDefault)
                              .finish());
        });
}

folly::Future<Status> ShowCreateEdgeExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *sceNode = asNode<ShowCreateEdge>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()
        ->getMetaClient()
        ->getEdgeSchema(spaceId, sceNode->getName())
        .via(runner())
        .thenValue([this, sceNode, spaceId](StatusOr<meta::cpp2::Schema> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId
                           << ", ShowCreate edge `" << sceNode->getName()
                           << "' failed: " << resp.status();
                return resp.status();
            }
            auto ret = SchemaUtil::toShowCreateSchema(false, sceNode->getName(), resp.value());
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

folly::Future<Status> AlterEdgeExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *aeNode = asNode<AlterEdge>(node());
    return qctx()->getMetaClient()->alterEdgeSchema(aeNode->space(),
                                                    aeNode->getName(),
                                                    aeNode->getSchemaItems(),
                                                    aeNode->getSchemaProp())
            .via(runner())
            .thenValue([this, aeNode](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "SpaceId: " << aeNode->space()
                               << ", Alter edge `" << aeNode->getName()
                               << "' failed: " << resp.status();
                    return resp.status();
                }
                return finish(
                    ResultBuilder().value(Value()).iter(Iterator::Kind::kDefault).finish());
            });
}
}   // namespace graph
}   // namespace nebula
