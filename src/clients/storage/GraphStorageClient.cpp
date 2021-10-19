/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "clients/storage/GraphStorageClient.h"

#include "common/base/Base.h"

using nebula::storage::cpp2::ExecResponse;
using nebula::storage::cpp2::GetNeighborsResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace storage {

GraphStorageClient::CommonRequestParam::CommonRequestParam(GraphSpaceID space_,
                                                           SessionID sess,
                                                           ExecutionPlanID plan_,
                                                           bool profile_,
                                                           bool experimental,
                                                           folly::EventBase* evb_)
    : space(space_),
      session(sess),
      plan(plan_),
      profile(profile_),
      useExperimentalFeature(experimental),
      evb(evb_) {}

cpp2::RequestCommon GraphStorageClient::CommonRequestParam::toReqCommon() const {
  cpp2::RequestCommon common;
  common.set_session_id(session);
  common.set_plan_id(plan);
  common.set_profile_detail(profile);
  return common;
}

StorageRpcRespFuture<cpp2::GetNeighborsResponse> GraphStorageClient::getNeighbors(
    const CommonRequestParam& param,
    std::vector<std::string> colNames,
    const std::vector<Row>& vertices,
    const std::vector<EdgeType>& edgeTypes,
    cpp2::EdgeDirection edgeDirection,
    const std::vector<cpp2::StatProp>* statProps,
    const std::vector<cpp2::VertexProp>* vertexProps,
    const std::vector<cpp2::EdgeProp>* edgeProps,
    const std::vector<cpp2::Expr>* expressions,
    bool dedup,
    bool random,
    const std::vector<cpp2::OrderBy>& orderBy,
    int64_t limit,
    const Expression* filter) {
  auto cbStatus = getIdFromRow(param.space, false);
  if (!cbStatus.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>(
        std::runtime_error(cbStatus.status().toString()));
  }

  auto status = clusterIdsToHosts(param.space, vertices, std::move(cbStatus).value());
  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>(
        std::runtime_error(status.status().toString()));
  }

  auto& clusters = status.value();
  auto common = param.toReqCommon();
  std::unordered_map<HostAddr, cpp2::GetNeighborsRequest> requests;
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.set_space_id(param.space);
    req.set_column_names(colNames);
    req.set_parts(std::move(c.second));
    req.set_common(common);
    cpp2::TraverseSpec spec;
    spec.set_edge_types(edgeTypes);
    spec.set_edge_direction(edgeDirection);
    spec.set_dedup(dedup);
    spec.set_random(random);
    if (statProps != nullptr) {
      spec.set_stat_props(*statProps);
    }
    if (vertexProps != nullptr) {
      spec.set_vertex_props(*vertexProps);
    }
    if (edgeProps != nullptr) {
      spec.set_edge_props(*edgeProps);
    }
    if (expressions != nullptr) {
      spec.set_expressions(*expressions);
    }
    if (!orderBy.empty()) {
      spec.set_order_by(orderBy);
    }
    spec.set_limit(limit);
    if (filter != nullptr) {
      spec.set_filter(filter->encode());
    }
    req.set_traverse_spec(std::move(spec));
  }

  return collectResponse(
      param.evb,
      std::move(requests),
      [](cpp2::GraphStorageServiceAsyncClient* client, const cpp2::GetNeighborsRequest& r) {
        return client->future_getNeighbors(r);
      });
}

StorageRpcRespFuture<cpp2::ExecResponse> GraphStorageClient::addVertices(
    const CommonRequestParam& param,
    std::vector<cpp2::NewVertex> vertices,
    std::unordered_map<TagID, std::vector<std::string>> propNames,
    bool ifNotExists) {
  auto cbStatus = getIdFromNewVertex(param.space);
  if (!cbStatus.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
        std::runtime_error(cbStatus.status().toString()));
  }

  auto status = clusterIdsToHosts(param.space, std::move(vertices), std::move(cbStatus).value());
  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
        std::runtime_error(status.status().toString()));
  }

  auto& clusters = status.value();
  std::unordered_map<HostAddr, cpp2::AddVerticesRequest> requests;
  auto common = param.toReqCommon();
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.set_space_id(param.space);
    req.set_if_not_exists(ifNotExists);
    req.set_parts(std::move(c.second));
    req.set_prop_names(propNames);
    req.set_common(common);
  }

  return collectResponse(
      param.evb,
      std::move(requests),
      [](cpp2::GraphStorageServiceAsyncClient* client, const cpp2::AddVerticesRequest& r) {
        return client->future_addVertices(r);
      });
}

StorageRpcRespFuture<cpp2::ExecResponse> GraphStorageClient::addEdges(
    const CommonRequestParam& param,
    std::vector<cpp2::NewEdge> edges,
    std::vector<std::string> propNames,
    bool ifNotExists) {
  auto cbStatus = getIdFromNewEdge(param.space);
  if (!cbStatus.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
        std::runtime_error(cbStatus.status().toString()));
  }

  auto status = clusterIdsToHosts(param.space, std::move(edges), std::move(cbStatus).value());
  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
        std::runtime_error(status.status().toString()));
  }

  auto& clusters = status.value();
  std::unordered_map<HostAddr, cpp2::AddEdgesRequest> requests;
  auto common = param.toReqCommon();
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.set_space_id(param.space);
    req.set_if_not_exists(ifNotExists);
    req.set_parts(std::move(c.second));
    req.set_prop_names(propNames);
    req.set_common(common);
  }
  return collectResponse(
      param.evb,
      std::move(requests),
      [useToss = param.useExperimentalFeature](cpp2::GraphStorageServiceAsyncClient* client,
                                               const cpp2::AddEdgesRequest& r) {
        return useToss ? client->future_chainAddEdges(r) : client->future_addEdges(r);
      });
}

StorageRpcRespFuture<cpp2::GetPropResponse> GraphStorageClient::getProps(
    const CommonRequestParam& param,
    const DataSet& input,
    const std::vector<cpp2::VertexProp>* vertexProps,
    const std::vector<cpp2::EdgeProp>* edgeProps,
    const std::vector<cpp2::Expr>* expressions,
    bool dedup,
    const std::vector<cpp2::OrderBy>& orderBy,
    int64_t limit,
    const Expression* filter) {
  auto cbStatus = getIdFromRow(param.space, edgeProps != nullptr);
  if (!cbStatus.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::GetPropResponse>>(
        std::runtime_error(cbStatus.status().toString()));
  }

  auto status = clusterIdsToHosts(param.space, input.rows, std::move(cbStatus).value());
  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::GetPropResponse>>(
        std::runtime_error(status.status().toString()));
  }

  auto& clusters = status.value();
  std::unordered_map<HostAddr, cpp2::GetPropRequest> requests;
  auto common = param.toReqCommon();
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.set_space_id(param.space);
    req.set_parts(std::move(c.second));
    req.set_dedup(dedup);
    if (vertexProps != nullptr) {
      req.set_vertex_props(*vertexProps);
    }
    if (edgeProps != nullptr) {
      req.set_edge_props(*edgeProps);
    }
    if (expressions != nullptr) {
      req.set_expressions(*expressions);
    }
    if (!orderBy.empty()) {
      req.set_order_by(orderBy);
    }
    req.set_limit(limit);
    if (filter != nullptr) {
      req.set_filter(filter->encode());
    }
    req.set_common(common);
  }

  return collectResponse(param.evb,
                         std::move(requests),
                         [](cpp2::GraphStorageServiceAsyncClient* client,
                            const cpp2::GetPropRequest& r) { return client->future_getProps(r); });
}

StorageRpcRespFuture<cpp2::ExecResponse> GraphStorageClient::deleteEdges(
    const CommonRequestParam& param, std::vector<cpp2::EdgeKey> edges) {
  auto cbStatus = getIdFromEdgeKey(param.space);
  if (!cbStatus.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
        std::runtime_error(cbStatus.status().toString()));
  }

  auto status = clusterIdsToHosts(param.space, std::move(edges), std::move(cbStatus).value());
  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
        std::runtime_error(status.status().toString()));
  }

  auto& clusters = status.value();
  std::unordered_map<HostAddr, cpp2::DeleteEdgesRequest> requests;
  auto common = param.toReqCommon();
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.set_space_id(param.space);
    req.set_parts(std::move(c.second));
    req.set_common(common);
  }

  return collectResponse(
      param.evb,
      std::move(requests),
      [](cpp2::GraphStorageServiceAsyncClient* client, const cpp2::DeleteEdgesRequest& r) {
        return client->future_deleteEdges(r);
      });
}

StorageRpcRespFuture<cpp2::ExecResponse> GraphStorageClient::deleteVertices(
    const CommonRequestParam& param, std::vector<Value> ids) {
  auto cbStatus = getIdFromValue(param.space);
  if (!cbStatus.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
        std::runtime_error(cbStatus.status().toString()));
  }

  auto status = clusterIdsToHosts(param.space, std::move(ids), std::move(cbStatus).value());
  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
        std::runtime_error(status.status().toString()));
  }

  auto& clusters = status.value();
  std::unordered_map<HostAddr, cpp2::DeleteVerticesRequest> requests;
  auto common = param.toReqCommon();
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.set_space_id(param.space);
    req.set_parts(std::move(c.second));
    req.set_common(common);
  }

  return collectResponse(
      param.evb,
      std::move(requests),
      [](cpp2::GraphStorageServiceAsyncClient* client, const cpp2::DeleteVerticesRequest& r) {
        return client->future_deleteVertices(r);
      });
}

StorageRpcRespFuture<cpp2::ExecResponse> GraphStorageClient::deleteTags(
    const CommonRequestParam& param, std::vector<cpp2::DelTags> delTags) {
  auto cbStatus = getIdFromDelTags(param.space);
  if (!cbStatus.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
        std::runtime_error(cbStatus.status().toString()));
  }

  auto status = clusterIdsToHosts(param.space, std::move(delTags), std::move(cbStatus).value());
  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
        std::runtime_error(status.status().toString()));
  }

  auto& clusters = status.value();
  std::unordered_map<HostAddr, cpp2::DeleteTagsRequest> requests;
  auto common = param.toReqCommon();
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.set_space_id(param.space);
    req.set_parts(std::move(c.second));
    req.set_common(common);
  }

  return collectResponse(
      param.evb,
      std::move(requests),
      [](cpp2::GraphStorageServiceAsyncClient* client, const cpp2::DeleteTagsRequest& r) {
        return client->future_deleteTags(r);
      });
}

folly::Future<StatusOr<storage::cpp2::UpdateResponse>> GraphStorageClient::updateVertex(
    const CommonRequestParam& param,
    Value vertexId,
    TagID tagId,
    std::vector<cpp2::UpdatedProp> updatedProps,
    bool insertable,
    std::vector<std::string> returnProps,
    std::string condition) {
  auto cbStatus = getIdFromValue(param.space);
  if (!cbStatus.ok()) {
    return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(cbStatus.status());
  }

  std::pair<HostAddr, cpp2::UpdateVertexRequest> request;

  DCHECK(!!metaClient_);
  auto status = metaClient_->partsNum(param.space);
  if (!status.ok()) {
    return Status::Error("Space not found, spaceid: %d", param.space);
  }
  auto numParts = status.value();
  status = metaClient_->partId(numParts, std::move(cbStatus).value()(vertexId));
  if (!status.ok()) {
    return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(status.status());
  }

  auto part = status.value();
  auto host = this->getLeader(param.space, part);
  if (!host.ok()) {
    return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(host.status());
  }
  request.first = std::move(host).value();
  cpp2::UpdateVertexRequest req;
  req.set_space_id(param.space);
  req.set_vertex_id(vertexId);
  req.set_tag_id(tagId);
  req.set_part_id(part);
  req.set_updated_props(std::move(updatedProps));
  req.set_return_props(std::move(returnProps));
  req.set_insertable(insertable);
  req.set_common(param.toReqCommon());
  if (condition.size() > 0) {
    req.set_condition(std::move(condition));
  }
  request.second = std::move(req);

  return getResponse(
      param.evb,
      std::move(request),
      [](cpp2::GraphStorageServiceAsyncClient* client, const cpp2::UpdateVertexRequest& r) {
        return client->future_updateVertex(r);
      });
}

folly::Future<StatusOr<storage::cpp2::UpdateResponse>> GraphStorageClient::updateEdge(
    const CommonRequestParam& param,
    storage::cpp2::EdgeKey edgeKey,
    std::vector<cpp2::UpdatedProp> updatedProps,
    bool insertable,
    std::vector<std::string> returnProps,
    std::string condition) {
  auto space = param.space;
  auto cbStatus = getIdFromEdgeKey(space);
  if (!cbStatus.ok()) {
    return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(cbStatus.status());
  }

  std::pair<HostAddr, cpp2::UpdateEdgeRequest> request;

  DCHECK(!!metaClient_);
  auto status = metaClient_->partsNum(space);
  if (!status.ok()) {
    return Status::Error("Space not found, spaceid: %d", space);
  }
  auto numParts = status.value();
  status = metaClient_->partId(numParts, std::move(cbStatus).value()(edgeKey));
  if (!status.ok()) {
    return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(status.status());
  }

  auto part = status.value();
  auto host = this->getLeader(space, part);
  if (!host.ok()) {
    return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(host.status());
  }
  request.first = std::move(host).value();
  cpp2::UpdateEdgeRequest req;
  req.set_space_id(space);
  req.set_edge_key(edgeKey);
  req.set_part_id(part);
  req.set_updated_props(std::move(updatedProps));
  req.set_return_props(std::move(returnProps));
  req.set_insertable(insertable);
  req.set_common(param.toReqCommon());
  if (condition.size() > 0) {
    req.set_condition(std::move(condition));
  }
  request.second = std::move(req);

  return getResponse(
      param.evb,
      std::move(request),
      [useExperimentalFeature = param.useExperimentalFeature](
          cpp2::GraphStorageServiceAsyncClient* client, const cpp2::UpdateEdgeRequest& r) {
        return useExperimentalFeature ? client->future_chainUpdateEdge(r)
                                      : client->future_updateEdge(r);
      });
}

folly::Future<StatusOr<cpp2::GetUUIDResp>> GraphStorageClient::getUUID(GraphSpaceID space,
                                                                       const std::string& name,
                                                                       folly::EventBase* evb) {
  std::pair<HostAddr, cpp2::GetUUIDReq> request;
  DCHECK(!!metaClient_);
  auto status = metaClient_->partsNum(space);
  if (!status.ok()) {
    return Status::Error("Space not found, spaceid: %d", space);
  }
  auto numParts = status.value();
  status = metaClient_->partId(numParts, name);
  if (!status.ok()) {
    return folly::makeFuture<StatusOr<cpp2::GetUUIDResp>>(status.status());
  }

  auto part = status.value();
  auto host = this->getLeader(space, part);
  if (!host.ok()) {
    return folly::makeFuture<StatusOr<storage::cpp2::GetUUIDResp>>(host.status());
  }
  request.first = std::move(host).value();
  cpp2::GetUUIDReq req;
  req.set_space_id(space);
  req.set_part_id(part);
  req.set_name(name);
  request.second = std::move(req);

  return getResponse(evb,
                     std::move(request),
                     [](cpp2::GraphStorageServiceAsyncClient* client, const cpp2::GetUUIDReq& r) {
                       return client->future_getUUID(r);
                     });
}

StorageRpcRespFuture<cpp2::LookupIndexResp> GraphStorageClient::lookupIndex(
    const CommonRequestParam& param,
    const std::vector<storage::cpp2::IndexQueryContext>& contexts,
    bool isEdge,
    int32_t tagOrEdge,
    const std::vector<std::string>& returnCols,
    int64_t limit) {
  // TODO(sky) : instead of isEdge and tagOrEdge to nebula::cpp2::SchemaID for graph layer.
  auto space = param.space;
  auto status = getHostParts(space);
  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::LookupIndexResp>>(
        std::runtime_error(status.status().toString()));
  }
  nebula::cpp2::SchemaID schemaId;
  if (isEdge) {
    schemaId.set_edge_type(tagOrEdge);
  } else {
    schemaId.set_tag_id(tagOrEdge);
  }

  auto& clusters = status.value();
  std::unordered_map<HostAddr, cpp2::LookupIndexRequest> requests;
  auto common = param.toReqCommon();
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.set_space_id(space);
    req.set_parts(std::move(c.second));
    req.set_return_columns(returnCols);

    cpp2::IndexSpec spec;
    spec.set_contexts(contexts);
    spec.set_schema_id(schemaId);
    req.set_indices(spec);
    req.set_common(common);
    req.set_limit(limit);
  }

  return collectResponse(
      param.evb,
      std::move(requests),
      [](cpp2::GraphStorageServiceAsyncClient* client, const cpp2::LookupIndexRequest& r) {
        return client->future_lookupIndex(r);
      });
}

StorageRpcRespFuture<cpp2::GetNeighborsResponse> GraphStorageClient::lookupAndTraverse(
    const CommonRequestParam& param, cpp2::IndexSpec indexSpec, cpp2::TraverseSpec traverseSpec) {
  auto space = param.space;
  auto status = getHostParts(space);
  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>(
        std::runtime_error(status.status().toString()));
  }

  auto& clusters = status.value();
  std::unordered_map<HostAddr, cpp2::LookupAndTraverseRequest> requests;
  auto common = param.toReqCommon();
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.set_space_id(space);
    req.set_parts(std::move(c.second));
    req.set_indices(indexSpec);
    req.set_traverse_spec(traverseSpec);
    req.set_common(common);
  }

  return collectResponse(
      param.evb,
      std::move(requests),
      [](cpp2::GraphStorageServiceAsyncClient* client, const cpp2::LookupAndTraverseRequest& r) {
        return client->future_lookupAndTraverse(r);
      });
}

folly::Future<StatusOr<cpp2::ScanEdgeResponse>> GraphStorageClient::scanEdge(
    cpp2::ScanEdgeRequest req, folly::EventBase* evb) {
  std::pair<HostAddr, cpp2::ScanEdgeRequest> request;
  auto host = this->getLeader(req.get_space_id(), req.get_part_id());
  if (!host.ok()) {
    return folly::makeFuture<StatusOr<cpp2::ScanEdgeResponse>>(host.status());
  }
  request.first = std::move(host).value();
  request.second = std::move(req);

  return getResponse(evb,
                     std::move(request),
                     [](cpp2::GraphStorageServiceAsyncClient* client,
                        const cpp2::ScanEdgeRequest& r) { return client->future_scanEdge(r); });
}

folly::Future<StatusOr<cpp2::ScanVertexResponse>> GraphStorageClient::scanVertex(
    cpp2::ScanVertexRequest req, folly::EventBase* evb) {
  std::pair<HostAddr, cpp2::ScanVertexRequest> request;
  auto host = this->getLeader(req.get_space_id(), req.get_part_id());
  if (!host.ok()) {
    return folly::makeFuture<StatusOr<cpp2::ScanVertexResponse>>(host.status());
  }
  request.first = std::move(host).value();
  request.second = std::move(req);

  return getResponse(evb,
                     std::move(request),
                     [](cpp2::GraphStorageServiceAsyncClient* client,
                        const cpp2::ScanVertexRequest& r) { return client->future_scanVertex(r); });
}

StatusOr<std::function<const VertexID&(const Row&)>> GraphStorageClient::getIdFromRow(
    GraphSpaceID space, bool isEdgeProps) const {
  auto vidTypeStatus = metaClient_->getSpaceVidType(space);
  if (!vidTypeStatus) {
    return vidTypeStatus.status();
  }
  auto vidType = std::move(vidTypeStatus).value();

  std::function<const VertexID&(const Row&)> cb;
  if (vidType == meta::cpp2::PropertyType::INT64) {
    if (isEdgeProps) {
      cb = [](const Row& r) -> const VertexID& {
        // The first column has to be the src, the thrid column has to be the
        // dst
        DCHECK_EQ(Value::Type::INT, r.values[0].type());
        DCHECK_EQ(Value::Type::INT, r.values[3].type());
        auto& mutableR = const_cast<Row&>(r);
        mutableR.values[0] =
            Value(std::string(reinterpret_cast<const char*>(&r.values[0].getInt()), 8));
        mutableR.values[3] =
            Value(std::string(reinterpret_cast<const char*>(&r.values[3].getInt()), 8));
        return mutableR.values[0].getStr();
      };
    } else {
      cb = [](const Row& r) -> const VertexID& {
        // The first column has to be the vid
        DCHECK_EQ(Value::Type::INT, r.values[0].type());
        auto& mutableR = const_cast<Row&>(r);
        mutableR.values[0] =
            Value(std::string(reinterpret_cast<const char*>(&r.values[0].getInt()), 8));
        return mutableR.values[0].getStr();
      };
    }
  } else if (vidType == meta::cpp2::PropertyType::FIXED_STRING) {
    cb = [](const Row& r) -> const VertexID& {
      // The first column has to be the vid
      DCHECK_EQ(Value::Type::STRING, r.values[0].type());
      return r.values[0].getStr();
    };
  } else {
    return Status::Error("Only support integer/string type vid.");
  }

  return cb;
}

StatusOr<std::function<const VertexID&(const cpp2::NewVertex&)>>
GraphStorageClient::getIdFromNewVertex(GraphSpaceID space) const {
  auto vidTypeStatus = metaClient_->getSpaceVidType(space);
  if (!vidTypeStatus) {
    return vidTypeStatus.status();
  }
  auto vidType = std::move(vidTypeStatus).value();

  std::function<const VertexID&(const cpp2::NewVertex&)> cb;
  if (vidType == meta::cpp2::PropertyType::INT64) {
    cb = [](const cpp2::NewVertex& v) -> const VertexID& {
      DCHECK_EQ(Value::Type::INT, v.get_id().type());
      auto& mutableV = const_cast<cpp2::NewVertex&>(v);
      mutableV.set_id(Value(std::string(reinterpret_cast<const char*>(&v.get_id().getInt()), 8)));
      return mutableV.get_id().getStr();
    };
  } else if (vidType == meta::cpp2::PropertyType::FIXED_STRING) {
    cb = [](const cpp2::NewVertex& v) -> const VertexID& {
      DCHECK_EQ(Value::Type::STRING, v.get_id().type());
      return v.get_id().getStr();
    };
  } else {
    return Status::Error("Only support integer/string type vid.");
  }
  return cb;
}

StatusOr<std::function<const VertexID&(const cpp2::NewEdge&)>> GraphStorageClient::getIdFromNewEdge(
    GraphSpaceID space) const {
  auto vidTypeStatus = metaClient_->getSpaceVidType(space);
  if (!vidTypeStatus) {
    return vidTypeStatus.status();
  }
  auto vidType = std::move(vidTypeStatus).value();

  std::function<const VertexID&(const cpp2::NewEdge&)> cb;
  if (vidType == meta::cpp2::PropertyType::INT64) {
    cb = [](const cpp2::NewEdge& e) -> const VertexID& {
      DCHECK_EQ(Value::Type::INT, e.get_key().get_src().type());
      DCHECK_EQ(Value::Type::INT, e.get_key().get_dst().type());
      auto& mutableE = const_cast<cpp2::NewEdge&>(e);
      (*mutableE.key_ref())
          .src_ref()
          .emplace(Value(
              std::string(reinterpret_cast<const char*>(&e.get_key().get_src().getInt()), 8)));
      (*mutableE.key_ref())
          .dst_ref()
          .emplace(Value(
              std::string(reinterpret_cast<const char*>(&e.get_key().get_dst().getInt()), 8)));
      return mutableE.get_key().get_src().getStr();
    };
  } else if (vidType == meta::cpp2::PropertyType::FIXED_STRING) {
    cb = [](const cpp2::NewEdge& e) -> const VertexID& {
      DCHECK_EQ(Value::Type::STRING, e.get_key().get_src().type());
      DCHECK_EQ(Value::Type::STRING, e.get_key().get_dst().type());
      return e.get_key().get_src().getStr();
    };
  } else {
    return Status::Error("Only support integer/string type vid.");
  }
  return cb;
}

StatusOr<std::function<const VertexID&(const cpp2::EdgeKey&)>> GraphStorageClient::getIdFromEdgeKey(
    GraphSpaceID space) const {
  auto vidTypeStatus = metaClient_->getSpaceVidType(space);
  if (!vidTypeStatus) {
    return vidTypeStatus.status();
  }
  auto vidType = std::move(vidTypeStatus).value();

  std::function<const VertexID&(const cpp2::EdgeKey&)> cb;
  if (vidType == meta::cpp2::PropertyType::INT64) {
    cb = [](const cpp2::EdgeKey& eKey) -> const VertexID& {
      DCHECK_EQ(Value::Type::INT, eKey.get_src().type());
      DCHECK_EQ(Value::Type::INT, eKey.get_dst().type());
      auto& mutableEK = const_cast<cpp2::EdgeKey&>(eKey);
      mutableEK.set_src(
          Value(std::string(reinterpret_cast<const char*>(&eKey.get_src().getInt()), 8)));
      mutableEK.set_dst(
          Value(std::string(reinterpret_cast<const char*>(&eKey.get_dst().getInt()), 8)));
      return mutableEK.get_src().getStr();
    };
  } else if (vidType == meta::cpp2::PropertyType::FIXED_STRING) {
    cb = [](const cpp2::EdgeKey& eKey) -> const VertexID& {
      DCHECK_EQ(Value::Type::STRING, eKey.get_src().type());
      DCHECK_EQ(Value::Type::STRING, eKey.get_dst().type());
      return eKey.get_src().getStr();
    };
  } else {
    return Status::Error("Only support integer/string type vid.");
  }
  return cb;
}

StatusOr<std::function<const VertexID&(const Value&)>> GraphStorageClient::getIdFromValue(
    GraphSpaceID space) const {
  auto vidTypeStatus = metaClient_->getSpaceVidType(space);
  if (!vidTypeStatus) {
    return vidTypeStatus.status();
  }
  auto vidType = std::move(vidTypeStatus).value();

  std::function<const VertexID&(const Value&)> cb;
  if (vidType == meta::cpp2::PropertyType::INT64) {
    cb = [](const Value& v) -> const VertexID& {
      DCHECK_EQ(Value::Type::INT, v.type());
      auto& mutableV = const_cast<Value&>(v);
      mutableV = Value(std::string(reinterpret_cast<const char*>(&v.getInt()), 8));
      return mutableV.getStr();
    };
  } else if (vidType == meta::cpp2::PropertyType::FIXED_STRING) {
    cb = [](const Value& v) -> const VertexID& {
      DCHECK_EQ(Value::Type::STRING, v.type());
      return v.getStr();
    };
  } else {
    return Status::Error("Only support integer/string type vid.");
  }
  return cb;
}

StatusOr<std::function<const VertexID&(const cpp2::DelTags&)>> GraphStorageClient::getIdFromDelTags(
    GraphSpaceID space) const {
  auto vidTypeStatus = metaClient_->getSpaceVidType(space);
  if (!vidTypeStatus) {
    return vidTypeStatus.status();
  }
  auto vidType = std::move(vidTypeStatus).value();

  std::function<const VertexID&(const cpp2::DelTags&)> cb;
  if (vidType == meta::cpp2::PropertyType::INT64) {
    cb = [](const cpp2::DelTags& delTags) -> const VertexID& {
      const auto& vId = delTags.get_id();
      DCHECK_EQ(Value::Type::INT, vId.type());
      auto& mutableV = const_cast<Value&>(vId);
      mutableV = Value(std::string(reinterpret_cast<const char*>(&vId.getInt()), 8));
      return mutableV.getStr();
    };
  } else if (vidType == meta::cpp2::PropertyType::FIXED_STRING) {
    cb = [](const cpp2::DelTags& delTags) -> const VertexID& {
      const auto& vId = delTags.get_id();
      DCHECK_EQ(Value::Type::STRING, vId.type());
      return vId.getStr();
    };
  } else {
    return Status::Error("Only support integer/string type vid.");
  }
  return cb;
}

}  // namespace storage
}  // namespace nebula
