/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_VALIDATOR_MOCKINDEXMANAGER_H_
#define GRAPH_VALIDATOR_MOCKINDEXMANAGER_H_

#include <memory>         // for shared_ptr, make_unique
#include <ostream>        // for operator<<
#include <string>         // for string
#include <unordered_map>  // for operator==, unordered_map
#include <utility>        // for pair
#include <vector>         // for vector

#include "common/base/Base.h"               // for UNUSED
#include "common/base/Logging.h"            // for LOG, LogMessageFatal, _LO...
#include "common/base/Status.h"             // for Status
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/meta/IndexManager.h"       // for IndexManager
#include "common/thrift/ThriftTypes.h"      // for GraphSpaceID, IndexID
#include "interface/gen-cpp2/meta_types.h"  // for IndexItem

namespace nebula {
namespace graph {

class MockIndexManager final : public ::nebula::meta::IndexManager {
 public:
  MockIndexManager() = default;
  ~MockIndexManager() = default;

  static std::unique_ptr<MockIndexManager> makeUnique() {
    auto instance = std::make_unique<MockIndexManager>();
    instance->init();
    return instance;
  }

  void init();

  using IndexItem = meta::cpp2::IndexItem;

  StatusOr<std::shared_ptr<IndexItem>> getTagIndex(GraphSpaceID space, IndexID index) override {
    UNUSED(space);
    UNUSED(index);
    LOG(FATAL) << "Unimplemented";
  }

  StatusOr<std::shared_ptr<IndexItem>> getEdgeIndex(GraphSpaceID space, IndexID index) override {
    UNUSED(space);
    UNUSED(index);
    LOG(FATAL) << "Unimplemented";
  }

  StatusOr<std::vector<std::shared_ptr<IndexItem>>> getTagIndexes(GraphSpaceID space) override {
    auto fd = tagIndexes_.find(space);
    if (fd == tagIndexes_.end()) {
      return Status::Error("No space for index");
    }
    return fd->second;
  }

  StatusOr<std::vector<std::shared_ptr<IndexItem>>> getEdgeIndexes(GraphSpaceID space) override {
    auto fd = edgeIndexes_.find(space);
    if (fd == edgeIndexes_.end()) {
      return Status::Error("No space for index");
    }
    return fd->second;
  }

  StatusOr<IndexID> toTagIndexID(GraphSpaceID space, std::string tagName) override {
    UNUSED(space);
    UNUSED(tagName);
    LOG(FATAL) << "Unimplemented";
  }

  StatusOr<IndexID> toEdgeIndexID(GraphSpaceID space, std::string edgeName) override {
    UNUSED(space);
    UNUSED(edgeName);
    LOG(FATAL) << "Unimplemented";
  }

  Status checkTagIndexed(GraphSpaceID space, IndexID index) override {
    UNUSED(space);
    UNUSED(index);
    LOG(FATAL) << "Unimplemented";
  }

  Status checkEdgeIndexed(GraphSpaceID space, IndexID index) override {
    UNUSED(space);
    UNUSED(index);
    LOG(FATAL) << "Unimplemented";
  }

 private:
  // index related
  std::unordered_map<GraphSpaceID, std::vector<std::shared_ptr<IndexItem>>> tagIndexes_;
  std::unordered_map<GraphSpaceID, std::vector<std::shared_ptr<IndexItem>>> edgeIndexes_;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_VALIDATOR_MOCKINDEXMANAGER_H_
