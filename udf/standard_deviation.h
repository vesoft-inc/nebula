/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef UDF_PROJECT_STANDARD_DEVIATION_H
#define UDF_PROJECT_STANDARD_DEVIATION_H

#include "../src/common/function/GraphFunction.h"

// Example of a UDF function that calculates the standard deviation of a set of numbers.
// > YIELD standard_deviation([1,2,3])
// +-----------------------------+
// | standard_deviation([1,2,3]) |
// +-----------------------------+
// | 0.816496580927726           |
// +-----------------------------+

// > YIELD standard_deviation([1,1,1])
// +-----------------------------+
// | standard_deviation([1,1,1]) |
// +-----------------------------+
// | 0.0                         |
// +-----------------------------+

// > GO 1 TO 2 STEPS FROM "player100" OVER follow
//     YIELD properties(edge).degree AS d | YIELD collect($-.d)
// +--------------------------+
// | collect($-.d)            |
// +--------------------------+
// | [95, 95, 95, 90, 95, 90] |
// +--------------------------+

// > GO 1 TO 2 STEPS FROM "player100" OVER follow
//     YIELD properties(edge).degree AS d | YIELD collect($-.d) AS d |
//     YIELD standard_deviation($-.d)
// +--------------------------+
// | standard_deviation($-.d) |
// +--------------------------+
// | 2.357022603955158        |
// +--------------------------+


class standard_deviation : public GraphFunction {
 public:
  char *name() override;

  std::vector<std::vector<nebula::Value::Type>> inputType() override;

  nebula::Value::Type returnType() override;

  size_t minArity() override;

  size_t maxArity() override;

  bool isPure() override;

  nebula::Value body(const std::vector<std::reference_wrapper<const nebula::Value>> &args) override;
};

#endif  // UDF_PROJECT_STANDARD_DEVIATION_H
