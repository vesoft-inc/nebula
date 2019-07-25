/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_DISTINCTER_H_
#define GRAPH_DISTINCTER_H_

#include "base/Base.h"
#include "dataman/RowWriter.h"
#include "dataman/RowSetWriter.h"

namespace nebula {
namespace graph {

class Distincter final {
public:
    explicit Distincter(bool distinct) : distinct_(distinct) {
        if (distinct) {
            uniq_ = std::make_unique<std::unordered_set<std::string>>();
        }
    }

    void doDistinct(RowWriter &writer, RowSetWriter &rsWriter);

private:
    bool                                                distinct_{false};
    std::unique_ptr<std::unordered_set<std::string>>    uniq_;
};
}  // namespace graph
}  // namespace nebula
#endif
