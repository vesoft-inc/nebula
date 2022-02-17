/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "DeleteExecutor.h"

#include <folly/Try.h>                 // for Try::~Try<T>, Try:...
#include <folly/futures/Future.h>      // for Future::Future<T>
#include <folly/futures/Promise.h>     // for Promise::Promise<T>
#include <folly/futures/Promise.h>     // for PromiseException::...
#include <folly/futures/Promise.h>     // for Promise::Promise<T>
#include <folly/futures/Promise.h>     // for PromiseException::...
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <algorithm>  // for max
#include <sstream>
#include <unordered_map>  // for operator!=
#include <utility>        // for move
#include <vector>         // for vector

#include "clients/storage/StorageClient.h"         // for StorageClient, Sto...
#include "clients/storage/StorageClientBase.h"     // for StorageRpcResponse
#include "common/base/Logging.h"                   // for COMPACT_GOOGLE_LOG...
#include "common/base/StatusOr.h"                  // for StatusOr
#include "common/datatypes/DataSet.h"              // for Row
#include "common/datatypes/Value.h"                // for Value, operator<<
#include "common/expression/Expression.h"          // for Expression
#include "common/time/Duration.h"                  // for Duration
#include "common/time/ScopedTimer.h"               // for SCOPED_TIMER
#include "graph/context/ExecutionContext.h"        // for ExecutionContext
#include "graph/context/Iterator.h"                // for Iterator
#include "graph/context/QueryContext.h"            // for QueryContext
#include "graph/context/QueryExpressionContext.h"  // for QueryExpressionCon...
#include "graph/context/Result.h"                  // for Result
#include "graph/executor/mutate/DeleteExecutor.h"  // for DeleteEdgesExecutor
#include "graph/planner/plan/ExecutionPlan.h"      // for ExecutionPlan
#include "graph/planner/plan/Mutate.h"             // for DeleteVertices
#include "graph/planner/plan/PlanNode.h"           // for SingleInputNode
#include "graph/service/GraphFlags.h"              // for FLAGS_enable_exper...
#include "graph/service/RequestContext.h"          // for RequestContext
#include "graph/session/ClientSession.h"           // for SpaceInfo, ClientS...
#include "graph/util/SchemaUtil.h"                 // for SchemaUtil
#include "interface/gen-cpp2/meta_types.h"         // for SpaceDesc
#include "interface/gen-cpp2/storage_types.h"      // for EdgeKey, DelTags
#include "parser/EdgeKey.h"                        // for EdgeKeyRef

using nebula::storage::StorageClient;

namespace nebula {
namespace graph {

folly::Future<Status> DeleteVerticesExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return deleteVertices();
}

folly::Future<Status> DeleteVerticesExecutor::deleteVertices() {
  auto* dvNode = asNode<DeleteVertices>(node());
  auto vidRef = dvNode->getVidRef();
  std::vector<Value> vertices;
  const auto& spaceInfo = qctx()->rctx()->session()->space();
  if (vidRef != nullptr) {
    auto inputVar = dvNode->inputVar();
    // empty inputVar means using pipe and need to get the GetNeighbors's
    // inputVar
    if (inputVar.empty()) {
      DCHECK(dvNode->dep() != nullptr);
      auto* gn = static_cast<const SingleInputNode*>(dvNode->dep())->dep();
      DCHECK(gn != nullptr);
      inputVar = static_cast<const SingleInputNode*>(gn)->inputVar();
    }
    DCHECK(!inputVar.empty());
    VLOG(2) << "inputVar: " << inputVar;
    auto& inputResult = ectx_->getResult(inputVar);
    auto iter = inputResult.iter();
    vertices.reserve(iter->size());
    QueryExpressionContext ctx(ectx_);
    for (; iter->valid(); iter->next()) {
      auto val = Expression::eval(vidRef, ctx(iter.get()));
      if (val.isNull() || val.empty()) {
        VLOG(3) << "NULL or EMPTY vid";
        continue;
      }
      if (!SchemaUtil::isValidVid(val, *spaceInfo.spaceDesc.vid_type_ref())) {
        std::stringstream ss;
        ss << "Wrong vid type `" << val.type() << "', value `" << val.toString() << "'";
        return Status::Error(ss.str());
      }
      vertices.emplace_back(std::move(val));
    }
  }

  if (vertices.empty()) {
    return Status::OK();
  }
  auto spaceId = spaceInfo.id;
  time::Duration deleteVertTime;
  auto plan = qctx()->plan();
  StorageClient::CommonRequestParam param(
      spaceId, qctx()->rctx()->session()->id(), plan->id(), plan->isProfileEnabled());
  return qctx()
      ->getStorageClient()
      ->deleteVertices(param, std::move(vertices))
      .via(runner())
      .ensure([deleteVertTime]() {
        VLOG(1) << "Delete vertices time: " << deleteVertTime.elapsedInUSec() << "us";
      })
      .thenValue([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(handleCompleteness(resp, false));
        return Status::OK();
      });
}

folly::Future<Status> DeleteTagsExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  return deleteTags();
}

folly::Future<Status> DeleteTagsExecutor::deleteTags() {
  auto* dtNode = asNode<DeleteTags>(node());
  const auto& spaceInfo = qctx()->rctx()->session()->space();
  auto vidRef = dtNode->getVidRef();
  DCHECK(vidRef != nullptr);
  auto inputVar = dtNode->inputVar();
  DCHECK(!inputVar.empty());
  auto& inputResult = ectx_->getResult(inputVar);
  auto iter = inputResult.iter();

  std::vector<storage::cpp2::DelTags> delTags;
  delTags.reserve(iter->size());

  QueryExpressionContext ctx(ectx_);
  for (; iter->valid(); iter->next()) {
    storage::cpp2::DelTags delTag;
    DCHECK(!iter->row()->empty());
    auto val = Expression::eval(vidRef, ctx(iter.get()));
    if (val.isNull() || val.empty()) {
      VLOG(3) << "NULL or EMPTY vid";
      continue;
    }
    if (!SchemaUtil::isValidVid(val, *spaceInfo.spaceDesc.vid_type_ref())) {
      std::stringstream ss;
      ss << "Wrong vid type `" << val.type() << "', value `" << val.toString() << "'";
      return Status::Error(ss.str());
    }
    delTag.id_ref() = val;
    delTag.tags_ref() = dtNode->tagIds();
    delTags.emplace_back(std::move(delTag));
  }

  auto spaceId = spaceInfo.id;
  time::Duration deleteTagTime;
  auto plan = qctx()->plan();
  StorageClient::CommonRequestParam param(
      spaceId, qctx()->rctx()->session()->id(), plan->id(), plan->isProfileEnabled());
  return qctx()
      ->getStorageClient()
      ->deleteTags(param, std::move(delTags))
      .via(runner())
      .ensure([deleteTagTime]() {
        VLOG(1) << "Delete vertices time: " << deleteTagTime.elapsedInUSec() << "us";
      })
      .thenValue([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(handleCompleteness(resp, false));
        return Status::OK();
      });
}

folly::Future<Status> DeleteEdgesExecutor::execute() {
  return deleteEdges();
}

folly::Future<Status> DeleteEdgesExecutor::deleteEdges() {
  SCOPED_TIMER(&execTime_);

  auto* deNode = asNode<DeleteEdges>(node());
  auto* edgeKeyRef = DCHECK_NOTNULL(deNode->edgeKeyRef());
  std::vector<storage::cpp2::EdgeKey> edgeKeys;
  const auto& spaceInfo = qctx()->rctx()->session()->space();
  auto inputVar = deNode->inputVar();
  DCHECK(!inputVar.empty());
  auto& inputResult = ectx_->getResult(inputVar);
  auto iter = inputResult.iter();
  edgeKeys.reserve(iter->size());
  QueryExpressionContext ctx(ectx_);
  for (; iter->valid(); iter->next()) {
    storage::cpp2::EdgeKey edgeKey;
    auto srcId = Expression::eval(edgeKeyRef->srcid(), ctx(iter.get()));
    if (srcId.isNull() || srcId.empty()) {
      VLOG(3) << "NULL or EMPTY vid";
      continue;
    }
    if (!SchemaUtil::isValidVid(srcId, *spaceInfo.spaceDesc.vid_type_ref())) {
      std::stringstream ss;
      ss << "Wrong srcId type `" << srcId.type() << "`, value `" << srcId.toString() << "'";
      return Status::Error(ss.str());
    }
    auto dstId = Expression::eval(edgeKeyRef->dstid(), ctx(iter.get()));
    if (!SchemaUtil::isValidVid(dstId, *spaceInfo.spaceDesc.vid_type_ref())) {
      std::stringstream ss;
      ss << "Wrong dstId type `" << dstId.type() << "', value `" << dstId.toString() << "'";
      return Status::Error(ss.str());
    }
    auto rank = Expression::eval(edgeKeyRef->rank(), ctx(iter.get()));
    if (!rank.isInt()) {
      std::stringstream ss;
      ss << "Wrong rank type `" << rank.type() << "', value `" << rank.toString() << "'";
      return Status::Error(ss.str());
    }
    DCHECK(edgeKeyRef->type());
    auto type = Expression::eval(edgeKeyRef->type(), ctx(iter.get()));
    if (!type.isInt()) {
      std::stringstream ss;
      ss << "Wrong edge type `" << type.type() << "', value `" << type.toString() << "'";
      return Status::Error(ss.str());
    }

    // out edge
    edgeKey.src_ref() = srcId;
    edgeKey.dst_ref() = dstId;
    edgeKey.ranking_ref() = rank.getInt();
    edgeKey.edge_type_ref() = type.getInt();
    edgeKeys.emplace_back(edgeKey);

    // in edge
    edgeKey.src_ref() = std::move(dstId);
    edgeKey.dst_ref() = std::move(srcId);
    edgeKey.edge_type_ref() = -type.getInt();
    edgeKeys.emplace_back(std::move(edgeKey));
  }

  if (edgeKeys.empty()) {
    VLOG(2) << "Empty edgeKeys";
    return Status::OK();
  }

  auto spaceId = spaceInfo.id;
  time::Duration deleteEdgeTime;
  auto plan = qctx()->plan();
  StorageClient::CommonRequestParam param(
      spaceId, qctx()->rctx()->session()->id(), plan->id(), plan->isProfileEnabled());
  param.useExperimentalFeature = FLAGS_enable_experimental_feature;
  return qctx()
      ->getStorageClient()
      ->deleteEdges(param, std::move(edgeKeys))
      .via(runner())
      .ensure([deleteEdgeTime]() {
        VLOG(1) << "Delete edge time: " << deleteEdgeTime.elapsedInUSec() << "us";
      })
      .thenValue([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(handleCompleteness(resp, false));
        return Status::OK();
      });
}
}  // namespace graph
}  // namespace nebula
