/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "standard_deviation.h"

#include <cmath>
#include <vector>

#include "../src/common/datatypes/List.h"

extern "C" GraphFunction *create() {
  return new standard_deviation;
}
extern "C" void destroy(GraphFunction *function) {
  delete function;
}

char *standard_deviation::name() {
  const char *name = "standard_deviation";
  return const_cast<char *>(name);
}

std::vector<std::vector<nebula::Value::Type>> standard_deviation::inputType() {
  std::vector<nebula::Value::Type> vtp = {nebula::Value::Type::LIST};
  std::vector<std::vector<nebula::Value::Type>> vvtp = {vtp};
  return vvtp;
}

nebula::Value::Type standard_deviation::returnType() {
  return nebula::Value::Type::FLOAT;
}

size_t standard_deviation::minArity() {
  return 1;
}

size_t standard_deviation::maxArity() {
  return 1;
}

bool standard_deviation::isPure() {
  return true;
}

double caculate_standard_deviation(const std::vector<double> &numbers) {
  double sum = 0;
  for (double number : numbers) {
    sum += number;
  }
  double average = sum / numbers.size();

  double variance = 0;
  for (double number : numbers) {
    double difference = number - average;
    variance += difference * difference;
  }
  variance /= numbers.size();

  return sqrt(variance);
}

nebula::Value standard_deviation::body(
    const std::vector<std::reference_wrapper<const nebula::Value>> &args) {
  switch (args[0].get().type()) {
    case nebula::Value::Type::NULLVALUE: {
      return nebula::Value::kNullValue;
    }
    case nebula::Value::Type::LIST: {
      std::vector<double> numbers;
      auto list = args[0].get().getList();
      auto size = list.size();

      for (int i = 0; i < size; i++) {
        auto &value = list[i];
        if (value.isInt()) {
          numbers.push_back(value.getInt());
        } else if (value.isFloat()) {
          numbers.push_back(value.getFloat());
        } else {
          return nebula::Value::kNullValue;
        }
      }
      return nebula::Value(caculate_standard_deviation(numbers));
    }
    default: {
      return nebula::Value::kNullValue;
    }
  }
}
