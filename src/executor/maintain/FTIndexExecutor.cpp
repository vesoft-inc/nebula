/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/interface/gen-cpp2/meta_types.h"

#include "context/QueryContext.h"
#include "executor/maintain/FTIndexExecutor.h"
#include "planner/plan/Maintain.h"
#include "util/FTIndexUtils.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateFTIndexExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *inode = asNode<CreateFTIndex>(node());
    return qctx()
        ->getMetaClient()
        ->createFTIndex(inode->getIndexName(),
                        inode->getIndex())
        .via(runner())
        .thenValue([inode](StatusOr<IndexID> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "Create fulltext index `"
                           << inode->getIndexName() << "' "
                           << "failed: " << resp.status();
                return resp.status();
            }
            return Status::OK();
        });
}

folly::Future<Status> DropFTIndexExecutor::execute() {
    auto *inode = asNode<DropFTIndex>(node());
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()
        ->getMetaClient()
        ->dropFTIndex(spaceId, inode->getName())
        .via(runner())
        .thenValue([this, inode, spaceId](StatusOr<bool> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId << ", Drop fulltext index `"
                           << inode->getName() << "' failed: " << resp.status();
                return resp.status();
            }
            auto tsRet = FTIndexUtils::getTSClients(qctx()->getMetaClient());
            if (!tsRet.ok()) {
                LOG(WARNING) << "Get text search clients failed";
            }
            auto ftRet = FTIndexUtils::dropTSIndex(std::move(tsRet).value(), inode->getName());
            if (!ftRet.ok()) {
                LOG(WARNING) << "Drop fulltext index '"
                             << inode->getName() << "' failed: " << ftRet.status();
            }
            return Status::OK();
        });
}

folly::Future<Status> ShowFTIndexesExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto spaceId = qctx()->rctx()->session()->space().id;
    return qctx()->getMetaClient()->listFTIndexes().via(runner()).thenValue(
        [this, spaceId](StatusOr<std::unordered_map<std::string, meta::cpp2::FTIndex>> resp) {
            if (!resp.ok()) {
                LOG(ERROR) << "SpaceId: " << spaceId << ", Show fulltext indexes failed"
                           << resp.status();
                return resp.status();
            }

            auto indexes = std::move(resp).value();
            DataSet dataSet;
            dataSet.colNames = {"Name", "Schema Type", "Schema Name", "Fields"};
            for (auto& index : indexes) {
                if (index.second.get_space_id() != spaceId) {
                    continue;
                }
                auto shmId = index.second.get_depend_schema();
                auto isEdge = shmId.getType() == meta::cpp2::SchemaID::Type::edge_type;
                auto shmNameRet = isEdge
                    ? this->qctx_->schemaMng()->toEdgeName(spaceId, shmId.get_edge_type())
                    : this->qctx_->schemaMng()->toTagName(spaceId, shmId.get_tag_id());
                if (!shmNameRet.ok()) {
                    LOG(ERROR) << "SpaceId: " << spaceId << ", Get schema name failed";
                    return shmNameRet.status();
                }
                std::string fields;
                folly::join(", ", index.second.get_fields(), fields);
                Row row;
                row.values.emplace_back(index.first);
                row.values.emplace_back(isEdge ? "Edge" : "Tag");
                row.values.emplace_back(std::move(shmNameRet).value());
                row.values.emplace_back(std::move(fields));
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
