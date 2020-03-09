/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_STORAGEENVIRONMENT_H_
#define STORAGE_STORAGEENVIRONMENT_H_

#include "base/Base.h"

namespace nebula {
namespace storage {

class StorageEnvironment {
public:
    StorageEnvironment()
        : currentTagID_(-1)
        , currentEdgeType_(-1)
        , currentIndexID_(-1)
        , currentPartID_(-1) {}

    ~StorageEnvironment() = default;

    TagID getTagID() {
        return currentTagID_;
    }

    void setTagID(TagID tagID) {
        currentTagID_ = tagID;
    }

    EdgeType getEdgeType() {
        return currentEdgeType_;
    }

    void setEdgeType(EdgeType edgeType) {
        currentEdgeType_ = edgeType;
    }

    PartitionID getPartID() {
        return currentPartID_;
    }

    void setPartID(PartitionID partID) {
        currentPartID_ = partID;
    }

    IndexID getIndexID() {
        return currentIndexID_;
    }

    void setIndexID(IndexID indexID) {
        currentIndexID_ = indexID;
    }

    void cleanup() {
        currentTagID_    = -1;
        currentEdgeType_ = -1;
        currentIndexID_  = -1;
        currentPartID_   = -1;
    }

private:
    std::atomic<TagID>        currentTagID_;
    std::atomic<EdgeType>     currentEdgeType_;
    std::atomic<IndexID>      currentIndexID_;
    std::atomic<PartitionID>  currentPartID_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_STORAGEENVIRONMENT_H_
