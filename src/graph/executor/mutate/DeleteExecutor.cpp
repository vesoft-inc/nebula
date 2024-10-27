// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/mutate/DeleteExecutor.h"

#include "common/memory/MemoryTracker.h"
#include "graph/planner/plan/Mutate.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"

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
    auto& inputResult = ectx_->getResult(inputVar);
    auto iter = inputResult.iter();
    vertices.reserve(iter->size());
    QueryExpressionContext ctx(ectx_);
    for (; iter->valid(); iter->next()) {
      auto val = Expression::eval(vidRef, ctx(iter.get()));
      if (val.isNull() || val.empty()) {
        continue;
      }
      if (!SchemaUtil::isValidVid(val, *spaceInfo.spaceDesc.vid_type_ref())) {
        auto errorMsg = fmt::format("Wrong vid type `{}`, value `{}`", val.type(), val.toString());
        return Status::Error(errorMsg);
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
        memory::MemoryCheckGuard guard;
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
      continue;
    }
    if (!SchemaUtil::isValidVid(val, *spaceInfo.spaceDesc.vid_type_ref())) {
      auto errorMsg = fmt::format("Wrong vid type `{}`, value `{}`", val.type(), val.toString());
      return Status::Error(errorMsg);
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
        memory::MemoryCheckGuard guard;
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
  edgeKeys.reserve(iter->size() * 2);
  QueryExpressionContext ctx(ectx_);
  for (; iter->valid(); iter->next()) {
    storage::cpp2::EdgeKey edgeKey;
    auto srcId = Expression::eval(edgeKeyRef->srcid(), ctx(iter.get()));
    if (srcId.isNull() || srcId.empty()) {
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
    return Status::OK();
  }

  auto spaceId = spaceInfo.id;
  time::Duration deleteEdgeTime;
  auto plan = qctx()->plan();
  StorageClient::CommonRequestParam param(
      spaceId, qctx()->rctx()->session()->id(), plan->id(), plan->isProfileEnabled());
  param.useExperimentalFeature = false;
  return qctx()
      ->getStorageClient()
      ->deleteEdges(param, std::move(edgeKeys))
      .via(runner())
      .ensure([deleteEdgeTime]() {
        VLOG(1) << "Delete edge time: " << deleteEdgeTime.elapsedInUSec() << "us";
      })
      .thenValue([this](storage::StorageRpcResponse<storage::cpp2::ExecResponse> resp) {
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        NG_RETURN_IF_ERROR(handleCompleteness(resp, false));
        return Status::OK();
      });
}
}  // namespace graph
}  // namespace nebula
