/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "UpdateExecutor.h"

#include "common/time/ScopedTimer.h"
#include "graph/context/QueryContext.h"
#include "graph/planner/plan/Mutate.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"

using nebula::storage::StorageClient;

namespace nebula {
namespace graph {

StatusOr<DataSet> UpdateBaseExecutor::handleResult(DataSet &&data) {
  if (data.colNames.size() <= 1) {
    if (yieldNames_.empty()) {
      return Status::OK();
    }
    LOG(ERROR) << "Empty return props";
    return Status::Error("Empty return props");
  }

  if (yieldNames_.size() != data.colNames.size() - 1) {
    LOG(ERROR) << "Expect colName size is " << yieldNames_.size() << ", return colName size is "
               << data.colNames.size() - 1;
    return Status::Error("Wrong return prop size");
  }
  DataSet result;
  result.colNames = std::move(yieldNames_);
  for (auto &row : data.rows) {
    std::vector<Value> columns;
    for (auto i = 1u; i < row.values.size(); i++) {
      columns.emplace_back(std::move(row.values[i]));
    }
    result.rows.emplace_back(std::move(columns));
  }
  return result;
}

folly::Future<Status> UpdateVertexExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *uvNode = asNode<UpdateVertex>(node());
  yieldNames_ = uvNode->getYieldNames();
  time::Duration updateVertTime;
  auto plan = qctx()->plan();
  auto sess = qctx()->rctx()->session();
  StorageClient::CommonRequestParam param(
      uvNode->getSpaceId(), sess->id(), plan->id(), plan->isProfileEnabled());
  return qctx()
      ->getStorageClient()
      ->updateVertex(param,
                     uvNode->getVId(),
                     uvNode->getTagId(),
                     uvNode->getUpdatedProps(),
                     uvNode->getInsertable(),
                     uvNode->getReturnProps(),
                     uvNode->getCondition())
      .via(runner())
      .ensure([updateVertTime]() {
        VLOG(1) << "Update vertice time: " << updateVertTime.elapsedInUSec() << "us";
      })
      .thenValue([this](StatusOr<storage::cpp2::UpdateResponse> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(ERROR) << resp.status();
          return resp.status();
        }
        auto value = std::move(resp).value();
        for (auto &code : value.get_result().get_failed_parts()) {
          NG_RETURN_IF_ERROR(handleErrorCode(code.get_code(), code.get_part_id()));
        }
        if (value.props_ref().has_value()) {
          auto status = handleResult(std::move(*value.props_ref()));
          if (!status.ok()) {
            return status.status();
          }
          return finish(ResultBuilder()
                            .value(std::move(status).value())
                            .iter(Iterator::Kind::kDefault)
                            .build());
        }
        return Status::OK();
      });
}

folly::Future<Status> UpdateEdgeExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *ueNode = asNode<UpdateEdge>(node());
  storage::cpp2::EdgeKey edgeKey;
  edgeKey.src_ref() = ueNode->getSrcId();
  edgeKey.ranking_ref() = ueNode->getRank();
  edgeKey.edge_type_ref() = ueNode->getEdgeType();
  edgeKey.dst_ref() = ueNode->getDstId();
  yieldNames_ = ueNode->getYieldNames();

  time::Duration updateEdgeTime;
  auto plan = qctx()->plan();
  StorageClient::CommonRequestParam param(
      ueNode->getSpaceId(), qctx()->rctx()->session()->id(), plan->id(), plan->isProfileEnabled());
  param.useExperimentalFeature = FLAGS_enable_experimental_feature;
  return qctx()
      ->getStorageClient()
      ->updateEdge(param,
                   edgeKey,
                   ueNode->getUpdatedProps(),
                   ueNode->getInsertable(),
                   ueNode->getReturnProps(),
                   ueNode->getCondition())
      .via(runner())
      .ensure([updateEdgeTime]() {
        VLOG(1) << "Update edge time: " << updateEdgeTime.elapsedInUSec() << "us";
      })
      .thenValue([this](StatusOr<storage::cpp2::UpdateResponse> resp) {
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(ERROR) << "Update edge failed: " << resp.status();
          return resp.status();
        }
        auto value = std::move(resp).value();
        for (auto &code : value.get_result().get_failed_parts()) {
          NG_RETURN_IF_ERROR(handleErrorCode(code.get_code(), code.get_part_id()));
        }
        if (value.props_ref().has_value()) {
          auto status = handleResult(std::move(*value.props_ref()));
          if (!status.ok()) {
            return status.status();
          }
          return finish(ResultBuilder()
                            .value(std::move(status).value())
                            .iter(Iterator::Kind::kDefault)
                            .build());
        }
        return Status::OK();
      });
}
}  // namespace graph
}  // namespace nebula
