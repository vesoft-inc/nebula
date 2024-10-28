// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/mutate/UpdateExecutor.h"

#include "graph/planner/plan/Mutate.h"
#include "graph/service/GraphFlags.h"
#include "graph/util/SchemaUtil.h"

using nebula::storage::StorageClient;

namespace nebula {
namespace graph {

Status UpdateBaseExecutor::handleMultiResult(DataSet &result, DataSet &&data) {
  if (data.colNames.size() <= 1) {
    if (yieldNames_.empty()) {
      return Status::OK();
    }
    return Status::Error("Empty return props");
  }

  if (yieldNames_.size() != data.colNames.size() - 1) {
    LOG(WARNING) << "Expect colName size is " << yieldNames_.size() << ", return colName size is "
                 << data.colNames.size() - 1;
    return Status::Error("Wrong return prop size");
  }
  result.colNames = yieldNames_;
  for (auto &row : data.rows) {
    std::vector<Value> columns;
    for (auto i = 1u; i < row.values.size(); i++) {
      columns.emplace_back(std::move(row.values[i]));
    }
    result.rows.emplace_back(std::move(columns));
  }
  return Status::OK();
}

folly::Future<Status> UpdateVertexExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *urvNode = asNode<UpdateVertex>(node());

  auto vidRef = urvNode->getVidRef();
  std::vector<Value> vertices;
  const auto &spaceInfo = qctx()->rctx()->session()->space();

  if (vidRef != nullptr) {
    auto inputVar = urvNode->inputVar();
    DCHECK(!inputVar.empty());
    auto &inputResult = ectx_->getResult(inputVar);
    auto iter = inputResult.iter();
    vertices.reserve(iter->size());
    QueryExpressionContext ctx(ectx_);
    for (; iter->valid(); iter->next()) {
      auto val = Expression::eval(vidRef, ctx(iter.get()));
      if (val.isNull() || val.empty()) {
        continue;
      }
      if (!SchemaUtil::isValidVid(val, *spaceInfo.spaceDesc.vid_type_ref())) {
        auto errorMsg = fmt::format(
            "Wrong vid type `{}', value `{}'", Value::toString(val.type()), val.toString());
        return Status::Error(errorMsg);
      }
      vertices.emplace_back(std::move(val));
    }
  }

  if (vertices.empty()) {
    return Status::OK();
  }

  std::vector<folly::Future<StatusOr<storage::cpp2::UpdateResponse>>> futures;
  futures.reserve(vertices.size());

  yieldNames_ = urvNode->getYieldNames();
  time::Duration updateVertTime;
  auto plan = qctx()->plan();
  auto sess = qctx()->rctx()->session();
  StorageClient::CommonRequestParam param(
      urvNode->getSpaceId(), sess->id(), plan->id(), plan->isProfileEnabled());

  for (auto &vId : vertices) {
    futures.emplace_back(qctx()
                             ->getStorageClient()
                             ->updateVertex(param,
                                            vId,
                                            urvNode->getTagId(),
                                            urvNode->getUpdatedProps(),
                                            urvNode->getInsertable(),
                                            urvNode->getReturnProps(),
                                            urvNode->getCondition())
                             .via(runner()));
  }

  return folly::collectAll(futures)
      .via(runner())
      .ensure([updateVertTime]() {
        VLOG(1) << "updateVertTime: " << updateVertTime.elapsedInUSec() << "us";
      })
      .thenValue([this](std::vector<folly::Try<StatusOr<storage::cpp2::UpdateResponse>>> results) {
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        DataSet finalResult;
        for (auto &result : results) {
          if (result.hasException()) {
            LOG(WARNING) << "Update vertex request threw an exception.";
            return Status::Error("Exception occurred during update.");
          }

          if (!result.value().ok()) {
            LOG(WARNING) << "Update vertex failed: " << result.value().status();
            return result.value().status();
          }

          auto value = std::move(result.value()).value();
          for (auto &code : value.get_result().get_failed_parts()) {
            NG_RETURN_IF_ERROR(handleErrorCode(code.get_code(), code.get_part_id()));
          }

          if (value.props_ref().has_value()) {
            auto status = handleMultiResult(finalResult, std::move(*value.props_ref()));
            if (!status.ok()) {
              return status;
            }
          }
        }

        if (finalResult.colNames.empty()) {
          return Status::OK();
        } else {
          return finish(
              ResultBuilder().value(std::move(finalResult)).iter(Iterator::Kind::kDefault).build());
        }
      });
}

folly::Future<Status> UpdateEdgeExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *ureNode = asNode<UpdateEdge>(node());
  auto *edgeKeyRef = DCHECK_NOTNULL(ureNode->getEdgeKeyRef());
  auto edgeType = ureNode->getEdgeType();
  time::Duration updateEdgeTime;

  yieldNames_ = ureNode->getYieldNames();

  const auto &spaceInfo = qctx()->rctx()->session()->space();
  auto inputVar = ureNode->inputVar();
  DCHECK(!inputVar.empty());
  auto &inputResult = ectx_->getResult(inputVar);
  auto iter = inputResult.iter();

  std::vector<storage::cpp2::EdgeKey> edgeKeys;
  std::vector<storage::cpp2::EdgeKey> reverse_edgeKeys;
  edgeKeys.reserve(iter->size());
  reverse_edgeKeys.reserve(iter->size());

  QueryExpressionContext ctx(ectx_);
  for (; iter->valid(); iter->next()) {
    auto srcId = Expression::eval(edgeKeyRef->srcid(), ctx(iter.get()));
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
    storage::cpp2::EdgeKey edgeKey;
    edgeKey.src_ref() = srcId;
    edgeKey.dst_ref() = dstId;
    edgeKey.ranking_ref() = rank.getInt();
    edgeKey.edge_type_ref() = edgeType;

    storage::cpp2::EdgeKey reverse_edgeKey;
    reverse_edgeKey.src_ref() = std::move(dstId);
    reverse_edgeKey.dst_ref() = std::move(srcId);
    reverse_edgeKey.ranking_ref() = rank.getInt();
    reverse_edgeKey.edge_type_ref() = -edgeType;

    edgeKeys.emplace_back(std::move(edgeKey));
    reverse_edgeKeys.emplace_back(std::move(reverse_edgeKey));
  }

  if (edgeKeys.empty()) {
    return Status::OK();
  }

  auto plan = qctx()->plan();
  StorageClient::CommonRequestParam param(
      ureNode->getSpaceId(), qctx()->rctx()->session()->id(), plan->id(), plan->isProfileEnabled());
  param.useExperimentalFeature = false;

  std::vector<folly::Future<StatusOr<storage::cpp2::UpdateResponse>>> futures;
  futures.reserve(edgeKeys.size() + reverse_edgeKeys.size());

  for (auto &edgeKey : edgeKeys) {
    futures.emplace_back(qctx()
                             ->getStorageClient()
                             ->updateEdge(param,
                                          edgeKey,
                                          ureNode->getUpdatedProps(),
                                          ureNode->getInsertable(),
                                          {},
                                          ureNode->getCondition())
                             .via(runner()));
  }

  for (auto &edgeKey : reverse_edgeKeys) {
    futures.emplace_back(qctx()
                             ->getStorageClient()
                             ->updateEdge(param,
                                          edgeKey,
                                          ureNode->getUpdatedProps(),
                                          ureNode->getInsertable(),
                                          ureNode->getReturnProps(),
                                          ureNode->getCondition())
                             .via(runner()));
  }

  return folly::collectAll(futures)
      .via(runner())
      .ensure([updateEdgeTime]() {
        VLOG(1) << "updateEdgeTime: " << updateEdgeTime.elapsedInUSec() << "us";
      })
      .thenValue([this](std::vector<folly::Try<StatusOr<storage::cpp2::UpdateResponse>>> results) {
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        DataSet finalResult;
        for (auto &result : results) {
          if (result.hasException()) {
            LOG(WARNING) << "Update edge request threw an exception.";
            return Status::Error("Exception occurred during update.");
          }

          if (!result.value().ok()) {
            LOG(WARNING) << "Update edge failed: " << result.value().status();
            return result.value().status();
          }

          auto value = std::move(result.value()).value();
          for (auto &code : value.get_result().get_failed_parts()) {
            NG_RETURN_IF_ERROR(handleErrorCode(code.get_code(), code.get_part_id()));
          }

          if (value.props_ref().has_value() && value.props_ref()->colNames.size() > 1) {
            auto status = handleMultiResult(finalResult, std::move(*value.props_ref()));
            if (!status.ok()) {
              return status;
            }
          }
        }

        if (finalResult.colNames.empty()) {
          return Status::OK();
        } else {
          return finish(
              ResultBuilder().value(std::move(finalResult)).iter(Iterator::Kind::kDefault).build());
        }
      });
}

}  // namespace graph
}  // namespace nebula
