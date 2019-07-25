/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/Distincter.h"

namespace nebula {
namespace graph {

void Distincter::doDistinct(RowWriter &writer, RowSetWriter &rsWriter) {
    // TODO Consider float/double, and need to reduce mem copy.
    std::string encode = writer.encode();
    if (distinct_) {
        auto ret = uniq_->emplace(encode);
        if (ret.second) {
            rsWriter.addRow(writer);
        }
    } else {
        rsWriter.addRow(writer);
    }
}
}  // namespace graph
}  // namespace nebula
