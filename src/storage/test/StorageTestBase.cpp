/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/test/StorageTestBase.h"

namespace nebula {
namespace storage {

std::vector<cpp2::Vertex> setupVertices(
        const PartitionID partitionID,
        const int64_t vertexSize,
        const int32_t tagSize) {

    // partId => List<Vertex>
    // Vertex => {Id, List<VertexProp>}
    // VertexProp => {tagId, tags}
    std::vector<cpp2::Vertex> vertices;
    VertexID vertexID = 0;
    while (vertexID < vertexSize){
        TagID tagID = 0;
        std::vector<cpp2::Tag> tags;
        while (tagID < tagSize){
            tags.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                              tagID,
                              folly::stringPrintf("%d_%ld_%d", partitionID, vertexID, tagID++));
        }
        vertices.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                              vertexID++,
                              std::move(tags));
    }
    return std::move(vertices);
}

}  // namespace storage
}  // namespace nebula

