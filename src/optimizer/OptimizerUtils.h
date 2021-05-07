/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_
#define NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_

#include "util/SchemaUtil.h"
#include <common/interface/gen-cpp2/meta_types.h>

namespace nebula {
namespace graph {

class OptimizerUtils {
public:
    enum class BoundValueOperator {
        GREATER_THAN = 0,
        LESS_THAN,
        MAX,
        MIN,
    };

public:
    OptimizerUtils() = delete;

    static Value boundValue(const meta::cpp2::ColumnDef& col,
                            BoundValueOperator op,
                            const Value& v = Value());

    static Value boundValueWithGT(const meta::cpp2::ColumnDef& col, const Value& v);

    static Value boundValueWithLT(const meta::cpp2::ColumnDef& col, const Value& v);

    static Value boundValueWithMax(const meta::cpp2::ColumnDef& col);

    static Value boundValueWithMin(const meta::cpp2::ColumnDef& col);

    static Value normalizeValue(const meta::cpp2::ColumnDef& col, const Value& v);
};

}  // namespace graph
}  // namespace nebula
#endif  // NEBULA_GRAPH_OPTIMIZER_OPTIMIZERUTILS_H_
