/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "UpdateExecutor.h"

#include <folly/Try.h>                 // for Try::~Try<T>, Try::Try<T>
#include <folly/futures/Future.h>      // for Future::Future<T>, Fut...
#include <folly/futures/Promise.h>     // for Promise::Promise<T>
#include <folly/futures/Promise.h>     // for PromiseException::Prom...
#include <folly/futures/Promise.h>     // for Promise::Promise<T>
#include <folly/futures/Promise.h>     // for PromiseException::Prom...
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref, optional_fi...

#include <ostream>      // for operator<<, basic_ostream
#include <type_traits>  // for remove_reference<>::type
#include <utility>      // for move

#include "clients/storage/StorageClient.h"     // for StorageClient, Storage...
#include "common/base/Logging.h"               // for LogMessage, LOG, _LOG_...
#include "common/base/Status.h"                // for Status, operator<<
#include "common/datatypes/DataSet.h"          // for DataSet, Row
#include "common/datatypes/List.h"             // for List
#include "common/datatypes/Value.h"            // for Value
#include "common/time/Duration.h"              // for Duration
#include "common/time/ScopedTimer.h"           // for SCOPED_TIMER
#include "graph/context/Iterator.h"            // for Iterator, Iterator::Kind
#include "graph/context/QueryContext.h"        // for QueryContext
#include "graph/context/Result.h"              // for ResultBuilder
#include "graph/planner/plan/ExecutionPlan.h"  // for ExecutionPlan
#include "graph/planner/plan/Mutate.h"         // for UpdateEdge, UpdateVertex
#include "graph/service/GraphFlags.h"          // for FLAGS_enable_experimen...
#include "graph/service/RequestContext.h"      // for RequestContext
#include "graph/session/ClientSession.h"       // for ClientSession
#include "interface/gen-cpp2/storage_types.h"  // for UpdatedProp, UpdateRes...

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
