/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "common/base/Base.h"
#include "graph/cache/GraphCache.h"
#include "interface/gen-cpp2/storage_types.h"

namespace nebula {
namespace graph {
// Only the topological structure is stored, not the attribute of the vertices and edges
class StorageClientCache {
 public:
  StorageClientCache();

  template <class Response, class Request>
  StatusOr<Response> getCacheValue(const Request& req) {
    UNUSED(req);
    return Status::Error("No GetNeightbors");
  }

  template <class Response>
  void insertResultIntoCache(Response& resp) {
    UNUSED(resp);
  }

 private:
  Status buildEdgeContext(const nebula::storage::cpp2::TraverseSpec& req);

  std::string tagKey(const std::string& vId, TagID tagId);

  std::string edgeKey(const std::string& srcVid, EdgeType edgeType);

  Status checkCondition(const nebula::storage::cpp2::GetNeighborsRequest& req);

 private:
  GraphCache* cache_{nullptr};
  std::vector<EdgeType> edgeTypes_;
  nebula::DataSet resultDataSet_;
};

}  // namespace graph
}  // namespace nebula

#include "clients/storage/StorageClientCache-inl.h"
