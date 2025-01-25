/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/executor/maintain/VectorIndexExecutor.h"

#include "graph/planner/plan/Maintain.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace graph {

folly::Future<Status> CreateVectorIndexExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* inode = asNode<CreateVectorIndex>(node());
  return qctx()
      ->getMetaClient()
      ->createVectorIndex(inode->getName(), inode->getIndex())
      .via(runner())
      .thenValue([inode](StatusOr<IndexID> resp) {
        memory::MemoryCheckGuard guard;
        if (!resp.ok()) {
          LOG(WARNING) << "Create vector index `" << inode->getName()
                       << "' failed: " << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> DropVectorIndexExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto* inode = asNode<DropVectorIndex>(node());
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()
      ->getMetaClient()
      ->dropVectorIndex(spaceId, inode->getName())
      .via(runner())
      .thenValue([inode](StatusOr<bool> resp) {
        if (!resp.ok()) {
          LOG(WARNING) << "Drop vector index `" << inode->getName()
                       << "' failed: " << resp.status();
          return resp.status();
        }
        return Status::OK();
      });
}

folly::Future<Status> ShowVectorIndexesExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto spaceId = qctx()->rctx()->session()->space().id;
  return qctx()->getMetaClient()->listVectorIndexes().via(runner()).thenValue(
      [this, spaceId](StatusOr<std::unordered_map<std::string, meta::cpp2::VectorIndex>> resp) {
        memory::MemoryCheckGuard guard;
        if (!resp.ok()) {
          LOG(WARNING) << "SpaceId: " << spaceId << ", Show vector indexes failed" << resp.status();
          return resp.status();
        }

        auto indexes = std::move(resp).value();
        DataSet dataSet;
        dataSet.colNames = {
            "Name", "Schema Type", "Schema Name", "Field", "Dimension", "Model Name"};
        for (auto& index : indexes) {
          if (index.second.get_space_id() != spaceId) {
            continue;
          }
          auto shmId = index.second.get_depend_schema();
          auto isEdge = shmId.getType() == nebula::cpp2::SchemaID::Type::edge_type;
          auto shmNameRet =
              isEdge ? this->qctx_->schemaMng()->toEdgeName(spaceId, shmId.get_edge_type())
                     : this->qctx_->schemaMng()->toTagName(spaceId, shmId.get_tag_id());
          if (!shmNameRet.ok()) {
            LOG(WARNING) << "SpaceId: " << spaceId
                         << ", Get schema name failed: " << shmNameRet.status();
            return shmNameRet.status();
          }
          Row row;
          row.values.emplace_back(index.first);                        // Name
          row.values.emplace_back(isEdge ? "Edge" : "Tag");            // Schema Type
          row.values.emplace_back(std::move(shmNameRet).value());      // Schema Name
          row.values.emplace_back(index.second.get_field());           // Field
          row.values.emplace_back(index.second.get_dimension());       // Dimension
          row.values.emplace_back(index.second.get_model_name());  // Model Name
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
