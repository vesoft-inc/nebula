/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/Algo.h"

namespace nebula {
namespace graph {

void ProduceSemiShortestPath::setStartsVid(std::vector<Value> starts) {
    starts_ = std::move(starts);
}

}  // namnspace graph
}  // namespace nebula
