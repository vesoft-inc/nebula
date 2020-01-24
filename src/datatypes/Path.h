/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef DATATYPES_PATH_H_
#define DATATYPES_PATH_H_

#include "base/Base.h"
#include "thrift/ThriftTypes.h"

namespace nebula {

struct Step {
    EdgeType type;
    EdgeRanking ranking;
    VertexID dst;

    bool operator==(const Step& rhs) const {
        return type == rhs.type &&
               ranking == rhs.ranking &&
               dst == rhs.dst;
    }
};


struct Path {
    VertexID src;
    std::vector<Step> steps;

    bool operator==(const Path& rhs) const {
        return src == rhs.src &&
               steps == rhs.steps;
    }
};

}  // namespace nebula
#endif  // DATATYPES_PATH_H_

