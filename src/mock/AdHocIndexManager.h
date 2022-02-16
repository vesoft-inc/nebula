/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef MOCK_ADHOCINDEXMANAGER_H_
#define MOCK_ADHOCINDEXMANAGER_H_

#include <folly/synchronization/RWSpinLock.h>  // for RWSpinLock

#include <memory>         // for shared_ptr
#include <string>         // for string
#include <unordered_map>  // for unordered_map
#include <vector>         // for vector

#include "common/base/Status.h"             // for Status
#include "common/base/StatusOr.h"           // for StatusOr
#include "common/meta/IndexManager.h"       // for IndexManager
#include "common/thrift/ThriftTypes.h"      // for GraphSpaceID, IndexID
#include "interface/gen-cpp2/meta_types.h"  // for ColumnDef (ptr only)

namespace nebula {
namespace meta {
class MetaClient;

class MetaClient;
}  // namespace meta

namespace mock {

using IndexItem = nebula::meta::cpp2::IndexItem;

class AdHocIndexManager final : public nebula::meta::IndexManager {
 public:
  AdHocIndexManager() = default;
  ~AdHocIndexManager() = default;

  void addTagIndex(GraphSpaceID space,
                   TagID tagID,
                   IndexID indexID,
                   std::vector<nebula::meta::cpp2::ColumnDef>&& fields);

  void addEdgeIndex(GraphSpaceID space,
                    EdgeType edgeType,
                    IndexID indexID,
                    std::vector<nebula::meta::cpp2::ColumnDef>&& fields);

  StatusOr<std::shared_ptr<IndexItem>> getTagIndex(GraphSpaceID space, IndexID index) override;

  StatusOr<std::shared_ptr<IndexItem>> getEdgeIndex(GraphSpaceID space, IndexID index) override;

  StatusOr<std::vector<std::shared_ptr<IndexItem>>> getTagIndexes(GraphSpaceID space) override;

  StatusOr<std::vector<std::shared_ptr<IndexItem>>> getEdgeIndexes(GraphSpaceID space) override;

  StatusOr<IndexID> toTagIndexID(GraphSpaceID space, std::string tagName) override;

  StatusOr<IndexID> toEdgeIndexID(GraphSpaceID space, std::string edgeName) override;

  Status checkTagIndexed(GraphSpaceID space, TagID tagID) override;

  Status checkEdgeIndexed(GraphSpaceID space, EdgeType edgeType) override;

  void init(nebula::meta::MetaClient*) {}

  void removeTagIndex(GraphSpaceID space, IndexID indexID);

 protected:
  folly::RWSpinLock tagIndexLock_;
  folly::RWSpinLock edgeIndexLock_;

  std::unordered_map<GraphSpaceID, std::vector<std::shared_ptr<IndexItem>>> tagIndexes_;
  std::unordered_map<GraphSpaceID, std::vector<std::shared_ptr<IndexItem>>> edgeIndexes_;
};

}  // namespace mock
}  // namespace nebula
#endif  // MOCK_ADHOCINDEXMANAGER_H_
