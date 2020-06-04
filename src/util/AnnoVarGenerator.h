/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef UTIL_ANNOVARGENERATOR_H_
#define UTIL_ANNOVARGENERATOR_H_

#include "common/base/Base.h"

#include "util/IdGenerator.h"

namespace nebula {
namespace graph {
/**
 * An utility to generate an anonymous variable.
 */
class AnnoVarGenerator final {
public:
    AnnoVarGenerator() {
        idGen_ = std::make_unique<IdGenerator>();
    }

    std::string getVar() const {
        return folly::stringPrintf("UNAMED_%ld", idGen_->id());
    }

private:
    std::unique_ptr<IdGenerator>    idGen_;
};
}  // namespace graph
}  // namespace nebula
#endif  // UTIL_ANNOVARGENERATOR_H_
