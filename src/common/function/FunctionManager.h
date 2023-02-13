/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_FUNCTION_FUNCTIONMANAGER_H_
#define COMMON_FUNCTION_FUNCTIONMANAGER_H_

#include <folly/futures/Future.h>

#include "common/base/Status.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/Value.h"

/**
 * FunctionManager is for managing builtin and dynamic-loaded functions,
 * which users could use as function call expressions.
 *
 * TODO(dutor) To implement dynamic loading.
 */

namespace nebula {

struct TypeSignature {
  TypeSignature() = default;
  TypeSignature(std::vector<Value::Type> argsType, Value::Type returnType)
      : argsType_(std::move(argsType)), returnType_(returnType) {}
  std::vector<Value::Type> argsType_;
  Value::Type returnType_;
};

class FunctionManager final {
 public:
  using ArgType = std::reference_wrapper<const Value>;
  using Function = std::function<Value(const std::vector<ArgType> &)>;

  /**
   * To obtain a function named `func', with the actual arity.
   */
  static StatusOr<Function> get(const std::string &func, size_t arity);

  /**
   * To Check the validity of the function named `func', with the actual arity.
   * Only used for parser check.
   */
  static Status find(const std::string &func, const size_t arity);

  static StatusOr<bool> getIsPure(const std::string &func, size_t arity);

  /**
   * To load a set of functions from a shared object dynamically.
   */
  static Status load(const std::string &soname, const std::vector<std::string> &funcs);

  /**
   * To unload a shared object.
   */
  static Status unload(const std::string &soname, const std::vector<std::string> &funcs);

  /**
   * To obtain the return value type according to the parameter type
   */
  static StatusOr<Value::Type> getReturnType(const std::string &funcName,
                                             const std::vector<Value::Type> &argsType);

  // The attributes of the function call
  struct FunctionAttributes final {
    size_t minArity_{0};
    size_t maxArity_{0};
    // Pure means if the function takes the same input, it returns the same result
    // The key here is the number of arity, and the key represents whether the function is pure for
    // that number of arity.
    // For example, timestamp() is non-pure because it returns the current timestamp, while
    // timestamp(1) is pure because the output is always 1.
    std::unordered_map<size_t, bool> isPure_;
    // The flag indicates whether the function is always pure.
    // Some functions allow using int64_max as the maxArity_, so add isAlwaysPure_ to avoid
    // allocating huge amount of memory for these functions.
    bool isAlwaysPure_{false};
    Function body_;
  };

  // Sets the function to NON-pure for all numbers of arity it take, which means the function is
  // always NON-pure.
  void setCompleteNonPure(FunctionAttributes &attr) {
    for (auto i = attr.minArity_; i <= attr.maxArity_; ++i) {
      attr.isPure_[i] = false;
    }
  }

 private:
  /**
   * FunctionManager functions as a singleton, since the dynamic loading is
   * process-wide.
   */
  FunctionManager();

  static FunctionManager &instance();

  StatusOr<const FunctionAttributes> getInternal(std::string func, size_t arity) const;

  Status loadInternal(const std::string &soname, const std::vector<std::string> &funcs);

  Status unloadInternal(const std::string &soname, const std::vector<std::string> &funcs);

  static std::unordered_map<std::string, std::vector<TypeSignature>> typeSignature_;

  // the return type of a variadic function
  static std::unordered_map<std::string, Value::Type> variadicFunReturnType_;

  std::unordered_map<std::string, FunctionAttributes> functions_;
};

}  // namespace nebula

#endif  // COMMON_FUNCTION_FUNCTIONMANAGER_H_
