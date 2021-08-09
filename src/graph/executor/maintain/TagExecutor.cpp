/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/maintain/TagExecutor.h"
#include "context/QueryContext.h"
#include "planner/plan/Maintain.h"
#include "util/SchemaUtil.h"
#include "util/ScopedTimer.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateTagExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *ctNode = asNode<CreateTag>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->createTagSchema(spaceId,
            ctNode->getName(), ctNode->getSchema(), ctNode->getIfNotExists())
            .via(runner())
            .thenValue([ctNode, spaceId](StatusOr<TagID> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "SpaceId: " << spaceId
                               << ", Create tag `" << ctNode->getName()
                               << "' failed: " << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> DescTagExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *dtNode = asNode<DescTag>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()
        ->getMetaClient()
        ->getTagSchema(spaceId, dtNode->getName())
        .via(runner())
        .thenValue([this, dtNode, spaceId](StatusOr<meta::cpp2::Schema> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId
                           << ", Desc tag `" << dtNode->getName()
                           << "' failed: " << resp.status();
                return resp.status();
            }
            auto ret = SchemaUtil::toDescSchema(resp.value());
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

folly::Future<Status> DropTagExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *dtNode = asNode<DropTag>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->dropTagSchema(spaceId,
                                                  dtNode->getName(),
                                                  dtNode->getIfExists())
            .via(runner())
            .thenValue([dtNode, spaceId](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "SpaceId: " << spaceId
                               << ", Drop tag `" << dtNode->getName()
                               << "' failed: " << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}

folly::Future<Status> ShowTagsExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->listTagSchemas(spaceId).via(runner()).thenValue(
        [this, spaceId](StatusOr<std::vector<meta::cpp2::TagItem>> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId
                           << ", Show tags failed: " << resp.status();
                return resp.status();
            }
            auto tagItems = std::move(resp).value();

            DataSet dataSet;
            dataSet.colNames = {"Name"};
            std::set<std::string> orderTagNames;
            for (auto &tag : tagItems) {
                orderTagNames.emplace(tag.get_tag_name());
            }
            for (auto &name : orderTagNames) {
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

folly::Future<Status> ShowCreateTagExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *sctNode = asNode<ShowCreateTag>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->getTagSchema(spaceId, sctNode->getName())
            .via(runner())
            .thenValue([this, sctNode, spaceId](StatusOr<meta::cpp2::Schema> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "SpaceId: " << spaceId
                               << ", Show create tag `" << sctNode->getName()
                               << "' failed: " << resp.status();
                    return resp.status();
                }
                auto ret = SchemaUtil::toShowCreateSchema(true, sctNode->getName(), resp.value());
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

folly::Future<Status> AlterTagExecutor::execute() {
    SCOPED_TIMER(&execTime_);

    auto *aeNode = asNode<AlterTag>(node());
    return qctx()->getMetaClient()->alterTagSchema(aeNode->space(),
                                                   aeNode->getName(),
                                                   aeNode->getSchemaItems(),
                                                   aeNode->getSchemaProp())
            .via(runner())
            .thenValue([aeNode](StatusOr<bool> resp) {
                if (!resp.ok()) {
                    LOG(ERROR) << "SpaceId: " << aeNode->space()
                               << ", Alter tag `" << aeNode->getName()
                               << "' failed: " << resp.status();
                    return resp.status();
                }
                return Status::OK();
            });
}
}   // namespace graph
}   // namespace nebula
