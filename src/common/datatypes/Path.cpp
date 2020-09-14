/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/datatypes/Path.h"

namespace nebula {
void Path::reverse() {
    if (steps.empty()) {
        return;
    }
    std::reverse(steps.begin(), steps.end());
    swap(src, steps.front().dst);
    for (size_t i = 0; i < steps.size() - 1; ++i) {
        swap(steps[i].dst, steps[i + 1].dst);
        steps[i].type = -steps[i].type;
    }
    steps.back().type = -steps.back().type;
}

bool Path::append(Path path) {
    if (src != path.src && steps.back().dst != path.src) {
        return false;
    }
    steps.reserve(steps.size() + path.steps.size());
    steps.insert(steps.end(),
                 std::make_move_iterator(path.steps.begin()),
                 std::make_move_iterator(path.steps.end()));
    return true;
}
}  // namespace nebula
