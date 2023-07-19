/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_FUNCTION_FUNCTIONUDFMANAGER_H_
#define COMMON_FUNCTION_FUNCTIONUDFMANAGER_H_

#include "FunctionManager.h"
#include "GraphFunction.h"

namespace nebula {

class FunctionManager;

class FunctionUdfManager {
 public:
  typedef GraphFunction *(create_f)();
  typedef void(destroy_f)(GraphFunction *);

  static StatusOr<Value::Type> getUdfReturnType(const std::string functionName,
                                                const std::vector<Value::Type> &argsType);

  static StatusOr<const FunctionManager::FunctionAttributes> loadUdfFunction(
      std::string functionName, size_t arity);

  static FunctionUdfManager &instance();

  FunctionUdfManager();

 private:
  static create_f *getGraphFunctionClass(void *func_handle);
  static destroy_f *deleteGraphFunctionClass(void *func_handle);

  void addSoUdfFunction(char *funName, const char *soPath, size_t i, size_t i1, bool b);
  void initAndLoadSoFunction();
};

}  // namespace nebula
#endif  // COMMON_FUNCTION_FUNCTIONUDFMANAGER_H_
