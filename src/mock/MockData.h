/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef MOCK_MOCKDATA_H_
#define MOCK_MOCKDATA_H_

#include "base/Base.h"

namespace nebula {
namespace mock {

struct VertexData {
    VertexID            vId_;
    TagID               tId_;
    std::vector<Value>  props_;
};

struct EdgeData {
    VertexID            srcId_;
    EdgeType            type_;
    EdgeRanking         rank_;
    VertexID            dstId_;
    std::vector<Value>  props_;
};

class MockData {
public:
    nebula::cpp2::Schema
    mockTagSchema();

    nebula::cpp2::Schema
    mockEdgeSchema();

    std::vector<VertexData>
    mockVertices();

    std::vector<EdgeData>
    mockEdges();
};

}  // namespace mock
}  // namespace nebula

#endif  // MOCK_MOCKDATA_H_
