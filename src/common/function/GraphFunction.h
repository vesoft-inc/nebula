/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_FUNCTION_GRAPHFUNCTION_H
#define COMMON_FUNCTION_GRAPHFUNCTION_H

#include <vector>

#include "common/datatypes/Value.h"

class GraphFunction;

extern "C" GraphFunction *create();
extern "C" void destroy(GraphFunction *function);

class GraphFunction {
 public:
  virtual ~GraphFunction() = default;

  virtual char *name() = 0;

  virtual std::vector<std::vector<nebula::Value::Type>> inputType() = 0;

  virtual nebula::Value::Type returnType() = 0;

  virtual size_t minArity() = 0;

  virtual size_t maxArity() = 0;

  virtual bool isPure() = 0;

  virtual nebula::Value body(
      const std::vector<std::reference_wrapper<const nebula::Value>> &args) = 0;
};

#endif  // COMMON_FUNCTION_GRAPHFUNCTION_H
