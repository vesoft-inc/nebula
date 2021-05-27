/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_ANONCOLGENERATOR_H_
#define UTIL_ANONCOLGENERATOR_H_

#include "common/base/Base.h"

#include "util/IdGenerator.h"

namespace nebula {
namespace graph {

constexpr char kVertexStr[]   = "_vertex";
constexpr char kVerticesStr[] = "_vertices";
constexpr char kEdgeStr[]     = "_edge";
constexpr char kEdgesStr[]    = "_edges";
constexpr char kPathStr[]     = "_path";
constexpr char kCostStr[]     = "_cost";

/**
 * An utility to generate an anonymous column name.
 */
class AnonColGenerator final {
public:
    AnonColGenerator() {
        idGen_ = std::make_unique<IdGenerator>();
    }

    std::string getCol() const {
        return folly::stringPrintf("__COL_%ld", idGen_->id());
    }

private:
    std::unique_ptr<IdGenerator>    idGen_;
};
}  // namespace graph
}  // namespace nebula
#endif  // UTIL_ANONCOLGENERATOR_H_
