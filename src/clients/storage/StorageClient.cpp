/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/storage/StorageClient.h"

#include "common/base/Base.h"

using nebula::cpp2::PropertyType;
using nebula::storage::cpp2::ExecResponse;
using nebula::storage::cpp2::GetDstBySrcResponse;
using nebula::storage::cpp2::GetNeighborsResponse;
using nebula::storage::cpp2::GetPropResponse;

namespace nebula {
namespace storage {

StorageClient::CommonRequestParam::CommonRequestParam(GraphSpaceID space_,
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

cpp2::RequestCommon StorageClient::CommonRequestParam::toReqCommon() const {
  cpp2::RequestCommon common;
  common.session_id_ref() = session;
  common.plan_id_ref() = plan;
  common.profile_detail_ref() = profile;
  return common;
}

StorageRpcRespFuture<cpp2::GetNeighborsResponse> StorageClient::getNeighbors(
    const CommonRequestParam& param,
    std::vector<std::string> colNames,
    const std::vector<Value>& vids,
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
    const Expression* filter,
    const Expression* tagFilter) {
  auto cbStatus = getIdFromValue(param.space);
  if (!cbStatus.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>(
        std::runtime_error(cbStatus.status().toString()));
  }
  auto status = clusterIdsToHosts(param.space, vids, std::move(cbStatus).value());
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
    req.space_id_ref() = param.space;
    req.column_names_ref() = colNames;
    req.parts_ref() = std::move(c.second);
    req.common_ref() = common;
    cpp2::TraverseSpec spec;
    spec.edge_types_ref() = edgeTypes;
    spec.edge_direction_ref() = edgeDirection;
    spec.dedup_ref() = dedup;
    spec.random_ref() = random;
    if (statProps != nullptr) {
      spec.stat_props_ref() = *statProps;
    }
    if (vertexProps != nullptr) {
      spec.vertex_props_ref() = *vertexProps;
    }
    if (edgeProps != nullptr) {
      spec.edge_props_ref() = *edgeProps;
    }
    if (expressions != nullptr) {
      spec.expressions_ref() = *expressions;
    }
    if (!orderBy.empty()) {
      spec.order_by_ref() = orderBy;
    }
    spec.limit_ref() = limit;
    if (filter != nullptr) {
      spec.filter_ref() = filter->encode();
    }
    if (tagFilter != nullptr) {
      spec.tag_filter_ref() = tagFilter->encode();
    }
    req.traverse_spec_ref() = std::move(spec);
  }

  return collectResponse(param.evb,
                         std::move(requests),
                         [](ThriftClientType* client, const cpp2::GetNeighborsRequest& r) {
                           return client->future_getNeighbors(r);
                         });
}

StorageRpcRespFuture<cpp2::GetDstBySrcResponse> StorageClient::getDstBySrc(
    const CommonRequestParam& param,
    const std::vector<Value>& vertices,
    const std::vector<EdgeType>& edgeTypes) {
  auto cbStatus = getIdFromValue(param.space);
  if (!cbStatus.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::GetDstBySrcResponse>>(
        std::runtime_error(cbStatus.status().toString()));
  }

  auto status = clusterIdsToHosts(param.space, vertices, std::move(cbStatus).value());
  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::GetDstBySrcResponse>>(
        std::runtime_error(status.status().toString()));
  }

  auto& clusters = status.value();
  auto common = param.toReqCommon();
  std::unordered_map<HostAddr, cpp2::GetDstBySrcRequest> requests;
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.space_id_ref() = param.space;
    req.parts_ref() = std::move(c.second);
    req.edge_types_ref() = edgeTypes;
    req.common_ref() = common;
  }

  return collectResponse(param.evb,
                         std::move(requests),
                         [](ThriftClientType* client, const cpp2::GetDstBySrcRequest& r) {
                           return client->future_getDstBySrc(r);
                         });
}

StorageRpcRespFuture<cpp2::ExecResponse> StorageClient::addVertices(
    const CommonRequestParam& param,
    std::vector<cpp2::NewVertex> vertices,
    std::unordered_map<TagID, std::vector<std::string>> propNames,
    bool ifNotExists,
    bool ignoreExistedIndex) {
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
    req.space_id_ref() = param.space;
    req.if_not_exists_ref() = ifNotExists;
    req.ignore_existed_index_ref() = ignoreExistedIndex;
    req.parts_ref() = std::move(c.second);
    req.prop_names_ref() = propNames;
    req.common_ref() = common;
  }

  return collectResponse(param.evb,
                         std::move(requests),
                         [](ThriftClientType* client, const cpp2::AddVerticesRequest& r) {
                           return client->future_addVertices(r);
                         });
}

StorageRpcRespFuture<cpp2::ExecResponse> StorageClient::addEdges(const CommonRequestParam& param,
                                                                 std::vector<cpp2::NewEdge> edges,
                                                                 std::vector<std::string> propNames,
                                                                 bool ifNotExists,
                                                                 bool ignoreExistedIndex) {
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
    req.space_id_ref() = param.space;
    req.if_not_exists_ref() = ifNotExists;
    req.ignore_existed_index_ref() = ignoreExistedIndex;
    req.parts_ref() = std::move(c.second);
    req.prop_names_ref() = propNames;
    req.common_ref() = common;
  }
  return collectResponse(param.evb,
                         std::move(requests),
                         [useToss = param.useExperimentalFeature](ThriftClientType* client,
                                                                  const cpp2::AddEdgesRequest& r) {
                           return useToss ? client->future_chainAddEdges(r)
                                          : client->future_addEdges(r);
                         });
}

StorageRpcRespFuture<cpp2::GetPropResponse> StorageClient::getProps(
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
    req.space_id_ref() = param.space;
    req.parts_ref() = std::move(c.second);
    req.dedup_ref() = dedup;
    if (vertexProps != nullptr) {
      req.vertex_props_ref() = *vertexProps;
    }
    if (edgeProps != nullptr) {
      req.edge_props_ref() = *edgeProps;
    }
    if (expressions != nullptr) {
      req.expressions_ref() = *expressions;
    }
    if (!orderBy.empty()) {
      req.order_by_ref() = orderBy;
    }
    req.limit_ref() = limit;
    if (filter != nullptr) {
      req.filter_ref() = filter->encode();
    }
    req.common_ref() = common;
  }

  return collectResponse(
      param.evb, std::move(requests), [](ThriftClientType* client, const cpp2::GetPropRequest& r) {
        return client->future_getProps(r);
      });
}

StorageRpcRespFuture<cpp2::ExecResponse> StorageClient::deleteEdges(
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
    req.space_id_ref() = param.space;
    req.parts_ref() = std::move(c.second);
    req.common_ref() = common;
  }

  return collectResponse(param.evb,
                         std::move(requests),
                         [useToss = param.useExperimentalFeature](
                             ThriftClientType* client, const cpp2::DeleteEdgesRequest& r) {
                           return useToss ? client->future_chainDeleteEdges(r)
                                          : client->future_deleteEdges(r);
                         });
}

StorageRpcRespFuture<cpp2::ExecResponse> StorageClient::deleteVertices(
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
    req.space_id_ref() = param.space;
    req.parts_ref() = std::move(c.second);
    req.common_ref() = common;
  }

  return collectResponse(param.evb,
                         std::move(requests),
                         [](ThriftClientType* client, const cpp2::DeleteVerticesRequest& r) {
                           return client->future_deleteVertices(r);
                         });
}

StorageRpcRespFuture<cpp2::ExecResponse> StorageClient::deleteTags(
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
    req.space_id_ref() = param.space;
    req.parts_ref() = std::move(c.second);
    req.common_ref() = common;
  }

  return collectResponse(param.evb,
                         std::move(requests),
                         [](ThriftClientType* client, const cpp2::DeleteTagsRequest& r) {
                           return client->future_deleteTags(r);
                         });
}

folly::Future<StatusOr<storage::cpp2::UpdateResponse>> StorageClient::updateVertex(
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
  cpp2::UpdateVertexRequest req;
  req.space_id_ref() = param.space;
  req.vertex_id_ref() = vertexId;
  req.tag_id_ref() = tagId;
  req.part_id_ref() = part;
  req.updated_props_ref() = std::move(updatedProps);
  req.return_props_ref() = std::move(returnProps);
  req.insertable_ref() = insertable;
  req.common_ref() = param.toReqCommon();
  if (condition.size() > 0) {
    req.condition_ref() = std::move(condition);
  }

  return getResponse(param.evb,
                     host.value(),
                     req,
                     [](ThriftClientType* client, const cpp2::UpdateVertexRequest& r) {
                       return client->future_updateVertex(r);
                     });
}

folly::Future<StatusOr<storage::cpp2::UpdateResponse>> StorageClient::updateEdge(
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
  cpp2::UpdateEdgeRequest req;
  req.space_id_ref() = space;
  req.edge_key_ref() = edgeKey;
  req.part_id_ref() = part;
  req.updated_props_ref() = std::move(updatedProps);
  req.return_props_ref() = std::move(returnProps);
  req.insertable_ref() = insertable;
  req.common_ref() = param.toReqCommon();
  if (condition.size() > 0) {
    req.condition_ref() = std::move(condition);
  }

  return getResponse(param.evb,
                     host.value(),
                     req,
                     [useExperimentalFeature = param.useExperimentalFeature](
                         ThriftClientType* client, const cpp2::UpdateEdgeRequest& r) {
                       return useExperimentalFeature ? client->future_chainUpdateEdge(r)
                                                     : client->future_updateEdge(r);
                     });
}

folly::Future<StatusOr<cpp2::GetUUIDResp>> StorageClient::getUUID(GraphSpaceID space,
                                                                  const std::string& name,
                                                                  folly::EventBase* evb) {
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
  cpp2::GetUUIDReq req;
  req.space_id_ref() = space;
  req.part_id_ref() = part;
  req.name_ref() = name;

  return getResponse(
      evb, host.value(), req, [](ThriftClientType* client, const cpp2::GetUUIDReq& r) {
        return client->future_getUUID(r);
      });
}

StorageRpcRespFuture<cpp2::LookupIndexResp> StorageClient::lookupIndex(
    const CommonRequestParam& param,
    const std::vector<storage::cpp2::IndexQueryContext>& contexts,
    bool isEdge,
    int32_t tagOrEdge,
    const std::vector<std::string>& returnCols,
    std::vector<storage::cpp2::OrderBy> orderBy,
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
    schemaId.edge_type_ref() = tagOrEdge;
  } else {
    schemaId.tag_id_ref() = tagOrEdge;
  }

  auto& clusters = status.value();
  std::unordered_map<HostAddr, cpp2::LookupIndexRequest> requests;
  auto common = param.toReqCommon();
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.space_id_ref() = space;
    req.parts_ref() = std::move(c.second);
    req.return_columns_ref() = returnCols;

    cpp2::IndexSpec spec;
    spec.contexts_ref() = contexts;
    spec.schema_id_ref() = schemaId;
    req.indices_ref() = spec;
    req.common_ref() = common;
    req.limit_ref() = limit;
    req.order_by_ref() = orderBy;
  }

  return collectResponse(param.evb,
                         std::move(requests),
                         [](ThriftClientType* client, const cpp2::LookupIndexRequest& r) {
                           return client->future_lookupIndex(r);
                         });
}

StorageRpcRespFuture<cpp2::GetNeighborsResponse> StorageClient::lookupAndTraverse(
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
    req.space_id_ref() = space;
    req.parts_ref() = std::move(c.second);
    req.indices_ref() = indexSpec;
    req.traverse_spec_ref() = traverseSpec;
    req.common_ref() = common;
  }

  return collectResponse(param.evb,
                         std::move(requests),
                         [](ThriftClientType* client, const cpp2::LookupAndTraverseRequest& r) {
                           return client->future_lookupAndTraverse(r);
                         });
}

StorageRpcRespFuture<cpp2::ScanResponse> StorageClient::scanEdge(
    const CommonRequestParam& param,
    const std::vector<cpp2::EdgeProp>& edgeProp,
    int64_t limit,
    const Expression* filter) {
  std::unordered_map<HostAddr, cpp2::ScanEdgeRequest> requests;
  auto status = getHostPartsWithCursor(param.space);
  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ScanResponse>>(
        std::runtime_error(status.status().toString()));
  }
  auto& clusters = status.value();
  for (const auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.space_id_ref() = param.space;
    req.parts_ref() = std::move(c.second);
    req.return_columns_ref() = edgeProp;
    req.limit_ref() = limit;
    if (filter != nullptr) {
      req.filter_ref() = filter->encode();
    }
    req.common_ref() = param.toReqCommon();
  }

  return collectResponse(
      param.evb, std::move(requests), [](ThriftClientType* client, const cpp2::ScanEdgeRequest& r) {
        return client->future_scanEdge(r);
      });
}

StorageRpcRespFuture<cpp2::ScanResponse> StorageClient::scanVertex(
    const CommonRequestParam& param,
    const std::vector<cpp2::VertexProp>& vertexProp,
    int64_t limit,
    const Expression* filter) {
  std::unordered_map<HostAddr, cpp2::ScanVertexRequest> requests;
  auto status = getHostPartsWithCursor(param.space);
  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ScanResponse>>(
        std::runtime_error(status.status().toString()));
  }
  auto& clusters = status.value();
  for (const auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.space_id_ref() = param.space;
    req.parts_ref() = std::move(c.second);
    req.return_columns_ref() = vertexProp;
    req.limit_ref() = limit;
    if (filter != nullptr) {
      req.filter_ref() = filter->encode();
    }
    req.common_ref() = param.toReqCommon();
  }

  return collectResponse(param.evb,
                         std::move(requests),
                         [](ThriftClientType* client, const cpp2::ScanVertexRequest& r) {
                           return client->future_scanVertex(r);
                         });
}

folly::SemiFuture<StorageRpcResponse<cpp2::KVGetResponse>> StorageClient::get(
    GraphSpaceID space, std::vector<std::string>&& keys, bool returnPartly, folly::EventBase* evb) {
  auto status = clusterIdsToHosts(
      space, std::move(keys), [](const std::string& v) -> const std::string& { return v; });

  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::KVGetResponse>>(
        std::runtime_error(status.status().toString()));
  }

  auto& clusters = status.value();
  std::unordered_map<HostAddr, cpp2::KVGetRequest> requests;
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.space_id_ref() = space;
    req.parts_ref() = std::move(c.second);
    req.return_partly_ref() = returnPartly;
  }

  return collectResponse(
      evb, std::move(requests), [](ThriftClientType* client, const cpp2::KVGetRequest& r) {
        return client->future_get(r);
      });
}

folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> StorageClient::put(
    GraphSpaceID space, std::vector<KeyValue> kvs, folly::EventBase* evb) {
  auto status = clusterIdsToHosts(
      space, std::move(kvs), [](const KeyValue& v) -> const std::string& { return v.key; });

  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
        std::runtime_error(status.status().toString()));
  }

  auto& clusters = status.value();
  std::unordered_map<HostAddr, cpp2::KVPutRequest> requests;
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.space_id_ref() = space;
    req.parts_ref() = std::move(c.second);
  }

  return collectResponse(
      evb, std::move(requests), [](ThriftClientType* client, const cpp2::KVPutRequest& r) {
        return client->future_put(r);
      });
}

folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> StorageClient::remove(
    GraphSpaceID space, std::vector<std::string> keys, folly::EventBase* evb) {
  auto status = clusterIdsToHosts(
      space, std::move(keys), [](const std::string& v) -> const std::string& { return v; });

  if (!status.ok()) {
    return folly::makeFuture<StorageRpcResponse<cpp2::ExecResponse>>(
        std::runtime_error(status.status().toString()));
  }

  auto& clusters = status.value();
  std::unordered_map<HostAddr, cpp2::KVRemoveRequest> requests;
  for (auto& c : clusters) {
    auto& host = c.first;
    auto& req = requests[host];
    req.space_id_ref() = space;
    req.parts_ref() = std::move(c.second);
  }

  return collectResponse(
      evb, std::move(requests), [](ThriftClientType* client, const cpp2::KVRemoveRequest& r) {
        return client->future_remove(r);
      });
}

StatusOr<std::function<const VertexID&(const Row&)>> StorageClient::getIdFromRow(
    GraphSpaceID space, bool isEdgeProps) const {
  auto vidTypeStatus = metaClient_->getSpaceVidType(space);
  if (!vidTypeStatus) {
    return vidTypeStatus.status();
  }
  auto vidType = std::move(vidTypeStatus).value();

  std::function<const VertexID&(const Row&)> cb;
  if (vidType == PropertyType::INT64) {
    if (isEdgeProps) {
      cb = [](const Row& r) -> const VertexID& {
        // The first column has to be the src, the third column has to be the
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
  } else if (vidType == PropertyType::FIXED_STRING) {
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

StatusOr<std::function<const VertexID&(const cpp2::NewVertex&)>> StorageClient::getIdFromNewVertex(
    GraphSpaceID space) const {
  auto vidTypeStatus = metaClient_->getSpaceVidType(space);
  if (!vidTypeStatus) {
    return vidTypeStatus.status();
  }
  auto vidType = std::move(vidTypeStatus).value();

  std::function<const VertexID&(const cpp2::NewVertex&)> cb;
  if (vidType == PropertyType::INT64) {
    cb = [](const cpp2::NewVertex& v) -> const VertexID& {
      DCHECK_EQ(Value::Type::INT, v.get_id().type());
      auto& mutableV = const_cast<cpp2::NewVertex&>(v);
      mutableV.id_ref() =
          Value(std::string(reinterpret_cast<const char*>(&v.get_id().getInt()), 8));
      return mutableV.get_id().getStr();
    };
  } else if (vidType == PropertyType::FIXED_STRING) {
    cb = [](const cpp2::NewVertex& v) -> const VertexID& {
      DCHECK_EQ(Value::Type::STRING, v.get_id().type());
      return v.get_id().getStr();
    };
  } else {
    return Status::Error("Only support integer/string type vid.");
  }
  return cb;
}

StatusOr<std::function<const VertexID&(const cpp2::NewEdge&)>> StorageClient::getIdFromNewEdge(
    GraphSpaceID space) const {
  auto vidTypeStatus = metaClient_->getSpaceVidType(space);
  if (!vidTypeStatus) {
    return vidTypeStatus.status();
  }
  auto vidType = std::move(vidTypeStatus).value();

  std::function<const VertexID&(const cpp2::NewEdge&)> cb;
  if (vidType == PropertyType::INT64) {
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
  } else if (vidType == PropertyType::FIXED_STRING) {
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

StatusOr<std::function<const VertexID&(const cpp2::EdgeKey&)>> StorageClient::getIdFromEdgeKey(
    GraphSpaceID space) const {
  auto vidTypeStatus = metaClient_->getSpaceVidType(space);
  if (!vidTypeStatus) {
    return vidTypeStatus.status();
  }
  auto vidType = std::move(vidTypeStatus).value();

  std::function<const VertexID&(const cpp2::EdgeKey&)> cb;
  if (vidType == PropertyType::INT64) {
    cb = [](const cpp2::EdgeKey& eKey) -> const VertexID& {
      DCHECK_EQ(Value::Type::INT, eKey.get_src().type());
      DCHECK_EQ(Value::Type::INT, eKey.get_dst().type());
      auto& mutableEK = const_cast<cpp2::EdgeKey&>(eKey);
      mutableEK.src_ref() =
          Value(std::string(reinterpret_cast<const char*>(&eKey.get_src().getInt()), 8));
      mutableEK.dst_ref() =
          Value(std::string(reinterpret_cast<const char*>(&eKey.get_dst().getInt()), 8));
      return mutableEK.get_src().getStr();
    };
  } else if (vidType == PropertyType::FIXED_STRING) {
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

StatusOr<std::function<const VertexID&(const Value&)>> StorageClient::getIdFromValue(
    GraphSpaceID space) const {
  auto vidTypeStatus = metaClient_->getSpaceVidType(space);
  if (!vidTypeStatus) {
    return vidTypeStatus.status();
  }
  auto vidType = std::move(vidTypeStatus).value();

  std::function<const VertexID&(const Value&)> cb;
  if (vidType == PropertyType::INT64) {
    cb = [](const Value& v) -> const VertexID& {
      DCHECK_EQ(Value::Type::INT, v.type());
      auto& mutableV = const_cast<Value&>(v);
      mutableV = Value(std::string(reinterpret_cast<const char*>(&v.getInt()), 8));
      return mutableV.getStr();
    };
  } else if (vidType == PropertyType::FIXED_STRING) {
    cb = [](const Value& v) -> const VertexID& {
      DCHECK_EQ(Value::Type::STRING, v.type());
      return v.getStr();
    };
  } else {
    return Status::Error("Only support integer/string type vid.");
  }
  return cb;
}

StatusOr<std::function<const VertexID&(const cpp2::DelTags&)>> StorageClient::getIdFromDelTags(
    GraphSpaceID space) const {
  auto vidTypeStatus = metaClient_->getSpaceVidType(space);
  if (!vidTypeStatus) {
    return vidTypeStatus.status();
  }
  auto vidType = std::move(vidTypeStatus).value();

  std::function<const VertexID&(const cpp2::DelTags&)> cb;
  if (vidType == PropertyType::INT64) {
    cb = [](const cpp2::DelTags& delTags) -> const VertexID& {
      const auto& vId = delTags.get_id();
      DCHECK_EQ(Value::Type::INT, vId.type());
      auto& mutableV = const_cast<Value&>(vId);
      mutableV = Value(std::string(reinterpret_cast<const char*>(&vId.getInt()), 8));
      return mutableV.getStr();
    };
  } else if (vidType == PropertyType::FIXED_STRING) {
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
