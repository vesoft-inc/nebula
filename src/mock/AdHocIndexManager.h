/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef MOCK_ADHOCINDEXMANAGER_H_
#define MOCK_ADHOCINDEXMANAGER_H_

#include "common/meta/IndexManager.h"
#include "interface/gen-cpp2/meta_types.h"

namespace nebula {
namespace mock {

using IndexItem = nebula::meta::cpp2::IndexItem;
using AnnIndexItem = nebula::meta::cpp2::AnnIndexItem;

class AdHocIndexManager final : public nebula::meta::IndexManager {
 public:
  AdHocIndexManager() = default;
  ~AdHocIndexManager() = default;

  void addEmptyIndex(GraphSpaceID space);

  void addTagIndex(GraphSpaceID space,
                   TagID tagID,
                   IndexID indexID,
                   std::vector<nebula::meta::cpp2::ColumnDef>&& fields);
  void addTagAnnIndex(GraphSpaceID space,
                      const std::vector<TagID>& tagID,
                      IndexID indexID,
                      nebula::meta::cpp2::ColumnDef&& field);

  void addEdgeIndex(GraphSpaceID space,
                    EdgeType edgeType,
                    IndexID indexID,
                    std::vector<nebula::meta::cpp2::ColumnDef>&& fields);

  void addEdgeAnnIndex(GraphSpaceID space,
                       const std::vector<EdgeType>& edgeTypes,
                       IndexID indexID,
                       nebula::meta::cpp2::ColumnDef&& field);

  StatusOr<std::shared_ptr<IndexItem>> getTagIndex(GraphSpaceID space, IndexID index) override;

  StatusOr<std::shared_ptr<IndexItem>> getEdgeIndex(GraphSpaceID space, IndexID index) override;

  StatusOr<std::vector<std::shared_ptr<IndexItem>>> getTagIndexes(GraphSpaceID space) override;

  StatusOr<std::vector<std::shared_ptr<IndexItem>>> getEdgeIndexes(GraphSpaceID space) override;

  StatusOr<IndexID> toTagIndexID(GraphSpaceID space, std::string tagName) override;

  StatusOr<IndexID> toEdgeIndexID(GraphSpaceID space, std::string edgeName) override;

  Status checkTagIndexed(GraphSpaceID space, TagID tagID) override;

  Status checkEdgeIndexed(GraphSpaceID space, EdgeType edgeType) override;

  StatusOr<std::shared_ptr<AnnIndexItem>> getTagAnnIndex(GraphSpaceID space,
                                                         IndexID index) override;

  StatusOr<std::shared_ptr<AnnIndexItem>> getEdgeAnnIndex(GraphSpaceID space,
                                                          IndexID index) override;

  void init(nebula::meta::MetaClient*) {}

  void removeTagIndex(GraphSpaceID space, IndexID indexID);

 protected:
  folly::RWSpinLock tagIndexLock_;
  folly::RWSpinLock edgeIndexLock_;

  std::unordered_map<GraphSpaceID, std::vector<std::shared_ptr<IndexItem>>> tagIndexes_;
  std::unordered_map<GraphSpaceID, std::vector<std::shared_ptr<IndexItem>>> edgeIndexes_;
  std::unordered_map<GraphSpaceID, std::vector<std::shared_ptr<AnnIndexItem>>> tagAnnIndexes_;
  std::unordered_map<GraphSpaceID, std::vector<std::shared_ptr<AnnIndexItem>>> edgeAnnIndexes_;
};

}  // namespace mock
}  // namespace nebula
#endif  // MOCK_ADHOCINDEXMANAGER_H_
