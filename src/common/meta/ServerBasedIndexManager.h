/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_META_SERVERBASEDINDEXMANAGER_H_
#define COMMON_META_SERVERBASEDINDEXMANAGER_H_

#include "common/meta/IndexManager.h"

namespace nebula {
namespace meta {

class ServerBasedIndexManager : public IndexManager {
public:
    ServerBasedIndexManager() = default;
    ~ServerBasedIndexManager();

    static std::unique_ptr<ServerBasedIndexManager> create(MetaClient *client);

    StatusOr<std::shared_ptr<IndexItem>>
    getTagIndex(GraphSpaceID space, IndexID index) override;

    StatusOr<std::shared_ptr<IndexItem>>
    getEdgeIndex(GraphSpaceID space, IndexID index) override;

    StatusOr<std::vector<std::shared_ptr<IndexItem>>>
    getTagIndexes(GraphSpaceID space) override;

    StatusOr<std::vector<std::shared_ptr<IndexItem>>>
    getEdgeIndexes(GraphSpaceID space) override;

    StatusOr<IndexID>
    toTagIndexID(GraphSpaceID space, std::string tagName) override;

    StatusOr<IndexID>
    toEdgeIndexID(GraphSpaceID space, std::string edgeName) override;

    Status checkTagIndexed(GraphSpaceID space, IndexID index) override;

    Status checkEdgeIndexed(GraphSpaceID space, IndexID index) override;

    void init(MetaClient *client);

private:
    MetaClient             *metaClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula
#endif  // COMMON_META_SERVERBASEDINDEXMANAGER_H_
