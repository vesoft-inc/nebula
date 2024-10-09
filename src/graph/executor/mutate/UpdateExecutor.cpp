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

StatusOr<DataSet> UpdateBaseExecutor::handleResult(DataSet &&data) {
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
  DataSet result;
  result.colNames = yieldNames_;
  // result.colNames = std::move(yieldNames_);
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
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(WARNING) << "Update vertices fail: " << resp.status();
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
    // result.colNames = std::move(yieldNames_);
    for (auto &row : data.rows) {
      std::vector<Value> columns;
      for (auto i = 1u; i < row.values.size(); i++) {
        columns.emplace_back(std::move(row.values[i]));
      }
      result.rows.emplace_back(std::move(columns));
    }
    return Status::OK();
}


folly::Future<Status> UpdateMultiVertexExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *umvNode = asNode<UpdateMultiVertex>(node());
    yieldNames_ = umvNode->getYieldNames();
    time::Duration updateMultiVertTime;
    auto plan = qctx()->plan();
    auto sess = qctx()->rctx()->session();
    StorageClient::CommonRequestParam param(
        umvNode->getSpaceId(), sess->id(), plan->id(), plan->isProfileEnabled());
    auto VIds = umvNode->getVIds();

    std::vector<folly::Future<StatusOr<storage::cpp2::UpdateResponse>>> futures;
    futures.reserve(VIds.size());

    for (auto &vId : VIds) {
      // update request for each vertex
      futures.emplace_back(
            qctx()
          ->getStorageClient()
          ->updateVertex(param,
                         vId,
                         umvNode->getTagId(),
                         umvNode->getUpdatedProps(),
                         umvNode->getInsertable(),
                         umvNode->getReturnProps(),
                         umvNode->getCondition())
          .via(runner()));
    }

    // collect all responses
    return folly::collectAll(futures)
        .via(runner())
        .ensure([updateMultiVertTime]() {
          VLOG(1) << "updateMultiVertTime: " << updateMultiVertTime.elapsedInUSec() << "us";
        })
        .thenValue([this](
                  std::vector<folly::Try<StatusOr<storage::cpp2::UpdateResponse>>> results) {
          memory::MemoryCheckGuard guard;
          SCOPED_TIMER(&execTime_);
          DataSet finalResult;
          bool propsExist = false;
          for (auto& result : results) {
            if (result.hasException()) {
              LOG(WARNING) << "Update vertex request threw an exception.";
              return Status::Error("Exception occurred during update.");
            }

            if (!result.value().ok()) {
              LOG(WARNING) << "Update vertex failed: " << result.value().status();
              return result.value().status();  // if fail, return the error
            }

            auto value = std::move(result.value()).value();
            for (auto& code : value.get_result().get_failed_parts()) {
              NG_RETURN_IF_ERROR(handleErrorCode(code.get_code(), code.get_part_id()));
            }

            if (value.props_ref().has_value()) {
              propsExist = true;
              auto status = handleMultiResult(finalResult, std::move(*value.props_ref()));
              if (!status.ok()) {
                return status;
              }
            }
          }

          // print the final result
          if (!propsExist) {
            return Status::OK();
          } else {
            return finish(ResultBuilder()
                .value(std::move(finalResult))
                .iter(Iterator::Kind::kDefault)
                .build());
          }
        });
}


folly::Future<Status> UpdateRefVertexExecutor::execute() {
    SCOPED_TIMER(&execTime_);
    auto *urvNode = asNode<UpdateRefVertex>(node());

    auto vidRef = urvNode->getVidRef();
    std::vector<Value> vertices;
    const auto& spaceInfo = qctx()->rctx()->session()->space();

    if (vidRef != nullptr) {
      auto inputVar = urvNode->inputVar();
      if (inputVar.empty()) {
        DCHECK(urvNode->dep() != nullptr);
        auto* gn = static_cast<const SingleInputNode*>(urvNode->dep())->dep();
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
        std::stringstream ss;
        ss << "Wrong vid type `" << val.type() << "', value `" << val.toString() << "'";
        return Status::Error(ss.str());
        }
        vertices.emplace_back(std::move(val));
      }
    }

    if (vertices.empty()) {
      LOG(INFO) << "No vertices to update";
      return Status::OK();
    }

    std::vector<folly::Future<StatusOr<storage::cpp2::UpdateResponse>>> futures;
    futures.reserve(vertices.size());

    yieldNames_ = urvNode->getYieldNames();
    time::Duration updateRefVertTime;
    auto plan = qctx()->plan();
    auto sess = qctx()->rctx()->session();
    StorageClient::CommonRequestParam param(
        urvNode->getSpaceId(), sess->id(), plan->id(), plan->isProfileEnabled());

    for (auto &vId : vertices) {
      // LOG(INFO) << "Update vertex id: " << vId.toString();
      futures.emplace_back(
            qctx()
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
        .ensure([updateRefVertTime]() {
          VLOG(1) << "updateMultiVertTime: " << updateRefVertTime.elapsedInUSec() << "us";
        })
        .thenValue([this](
                  std::vector<folly::Try<StatusOr<storage::cpp2::UpdateResponse>>> results) {
          memory::MemoryCheckGuard guard;
          SCOPED_TIMER(&execTime_);
          DataSet finalResult;
          bool propsExist = false;
          for (auto& result : results) {
            if (result.hasException()) {
              LOG(WARNING) << "Update vertex request threw an exception.";
              return Status::Error("Exception occurred during update.");
            }

            if (!result.value().ok()) {
              LOG(WARNING) << "Update vertex failed: " << result.value().status();
              return result.value().status();  // if fail, return the error
            }

            auto value = std::move(result.value()).value();
            for (auto& code : value.get_result().get_failed_parts()) {
              NG_RETURN_IF_ERROR(handleErrorCode(code.get_code(), code.get_part_id()));
            }

            if (value.props_ref().has_value()) {
              propsExist = true;
              auto status = handleMultiResult(finalResult, std::move(*value.props_ref()));
              if (!status.ok()) {
                return status;
              }
            }
          }

          // print the final result
          if (!propsExist) {
            return Status::OK();
          } else {
            return finish(ResultBuilder()
                .value(std::move(finalResult))
                .iter(Iterator::Kind::kDefault)
                .build());
          }
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

  LOG(INFO) << "Update edge: " << ueNode->getSrcId() << " -> " << ueNode->getDstId()
            << " @ " << ueNode->getRank() << " ON " << ueNode->getEdgeType();

  time::Duration updateEdgeTime;
  auto plan = qctx()->plan();
  StorageClient::CommonRequestParam param(
      ueNode->getSpaceId(), qctx()->rctx()->session()->id(), plan->id(), plan->isProfileEnabled());
  param.useExperimentalFeature = false;
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
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        if (!resp.ok()) {
          LOG(WARNING) << "Update edge failed: " << resp.status();
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

folly::Future<Status> UpdateMultiEdgeExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *umeNode = asNode<UpdateMultiEdge>(node());
  time::Duration updateMultiEdgeTime;
  auto plan = qctx()->plan();
  // TODO(zhijie): param 可以在多个请求间共用吗
  StorageClient::CommonRequestParam param(
      umeNode->getSpaceId(), qctx()->rctx()->session()->id(), plan->id(), plan->isProfileEnabled());
  param.useExperimentalFeature = false;

  yieldNames_ = umeNode->getYieldNames();

  bool isReverse = umeNode->isReverse();

  std::vector<storage::cpp2::EdgeKey> edgeKeys;
  if (!isReverse) {
      for (auto & edgeid : umeNode->getEdgeIds()) {
        storage::cpp2::EdgeKey edgeKey;
        edgeKey.src_ref() = edgeid.srcid();
        edgeKey.dst_ref() = edgeid.dstid();
        edgeKey.ranking_ref() = edgeid.rank();
        edgeKey.edge_type_ref() = umeNode->getEdgeType();
        edgeKeys.emplace_back(std::move(edgeKey));
      }
  } else {
      for (auto & edgeid : umeNode->getEdgeIds()) {
        storage::cpp2::EdgeKey edgeKey;
        edgeKey.src_ref() = edgeid.dstid();
        edgeKey.dst_ref() = edgeid.srcid();
        edgeKey.ranking_ref() = edgeid.rank();
        edgeKey.edge_type_ref() = umeNode->getEdgeType();
        edgeKeys.emplace_back(std::move(edgeKey));
      }
  }


  std::vector<folly::Future<StatusOr<storage::cpp2::UpdateResponse>>> futures;
  futures.reserve(edgeKeys.size());

  for (auto &edgeKey : edgeKeys) {
    futures.emplace_back(
          qctx()
        ->getStorageClient()
        ->updateEdge(param,
                     edgeKey,
                     umeNode->getUpdatedProps(),
                     umeNode->getInsertable(),
                     umeNode->getReturnProps(),
                     umeNode->getCondition())
        .via(runner()));
  }

  return folly::collectAll(futures)
      .via(runner())
      .ensure([updateMultiEdgeTime]() {
        VLOG(1) << "updateMultiEdgeTime: " << updateMultiEdgeTime.elapsedInUSec() << "us";
      })
      .thenValue([this](
                std::vector<folly::Try<StatusOr<storage::cpp2::UpdateResponse>>> results) {
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        DataSet finalResult;
        bool propsExist = false;
        for (auto& result : results) {
          if (result.hasException()) {
            LOG(WARNING) << "Update edge request threw an exception.";
            return Status::Error("Exception occurred during update.");
          }

          if (!result.value().ok()) {
            LOG(WARNING) << "Update edge failed: " << result.value().status();
            return result.value().status();  // if fail, return the error
          }

          auto value = std::move(result.value()).value();
          for (auto& code : value.get_result().get_failed_parts()) {
            NG_RETURN_IF_ERROR(handleErrorCode(code.get_code(), code.get_part_id()));
          }

          if (value.props_ref().has_value()) {
            propsExist = true;
            auto status = handleMultiResult(finalResult, std::move(*value.props_ref()));
            if (!status.ok()) {
              return status;
            }
          }
        }

        // print the final result
        if (!propsExist) {
          return Status::OK();
        } else {
          return finish(ResultBuilder()
              .value(std::move(finalResult))
              .iter(Iterator::Kind::kDefault)
              .build());
        }
      });
}

folly::Future<Status> UpdateRefEdgeExecutor::execute() {
  SCOPED_TIMER(&execTime_);
  auto *ureNode = asNode<UpdateRefEdge>(node());
  auto *edgeKeyRef = DCHECK_NOTNULL(ureNode->getEdgeKeyRef());
  auto edgeType = ureNode->getEdgeType();
  time::Duration updateRefEdgeTime;

  yieldNames_ = ureNode->getYieldNames();
  // print yield_name
  // for (auto &name : yieldNames_) {
  //   LOG(INFO) << "yield_name: " << name;
  // }

  const auto& spaceInfo = qctx()->rctx()->session()->space();
  auto inputVar = ureNode->inputVar();
  DCHECK(!inputVar.empty());
  auto& inputResult = ectx_->getResult(inputVar);
  auto iter = inputResult.iter();

  std::vector<storage::cpp2::EdgeKey> edgeKeys;
  std::vector<storage::cpp2::EdgeKey> reverse_edgeKeys;
  edgeKeys.reserve(iter->size());
  reverse_edgeKeys.reserve(iter->size());

  QueryExpressionContext ctx(ectx_);
  for (; iter->valid(); iter->next()) {
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
    futures.emplace_back(
          qctx()
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
    futures.emplace_back(
          qctx()
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
      .ensure([updateRefEdgeTime]() {
        VLOG(1) << "updateRefEdgeTime: " << updateRefEdgeTime.elapsedInUSec() << "us";
      })
      .thenValue([this](
                std::vector<folly::Try<StatusOr<storage::cpp2::UpdateResponse>>> results) {
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        DataSet finalResult;
        bool propsExist = false;
        for (auto& result : results) {
          // LOG(INFO) << "Update edge result";
          if (result.hasException()) {
            LOG(WARNING) << "Update edge request threw an exception.";
            return Status::Error("Exception occurred during update.");
          }

          if (!result.value().ok()) {
            LOG(WARNING) << "Update edge failed: " << result.value().status();
            return result.value().status();  // if fail, return the error
          }

          auto value = std::move(result.value()).value();
          for (auto& code : value.get_result().get_failed_parts()) {
            NG_RETURN_IF_ERROR(handleErrorCode(code.get_code(), code.get_part_id()));
          }

          // skip the edge result
          if (value.props_ref().has_value() && value.props_ref()->colNames.size() > 1) {
            propsExist = true;
            auto status = handleMultiResult(finalResult, std::move(*value.props_ref()));
            if (!status.ok()) {
              return status;
            }
          }
        }

        // print the final result
        if (!propsExist) {
          return Status::OK();
        } else {
          return finish(ResultBuilder()
              .value(std::move(finalResult))
              .iter(Iterator::Kind::kDefault)
              .build());
        }
      });
}

}  // namespace graph
}  // namespace nebula
