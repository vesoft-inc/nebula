/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_MOCKINDEXMANAGER_H_
#define VALIDATOR_MOCKINDEXMANAGER_H_

#include <vector>
#include "common/meta/IndexManager.h"

namespace nebula {
namespace graph {

class MockIndexManager final : public nebula::meta::IndexManager {
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

}   // namespace graph
}   // namespace nebula
#endif   // VALIDATOR_MOCKINDEXMANAGER_H_
