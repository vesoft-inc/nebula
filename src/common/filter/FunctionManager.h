/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_FILTER_FUNCTIONMANAGER_H_
#define COMMON_FILTER_FUNCTIONMANAGER_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "base/Status.h"
#include <folly/futures/Future.h>

/**
 * FunctionManager is for managing builtin and dynamic-loaded functions,
 * which users could use as function call expressions.
 *
 * TODO(dutor) To implement dynamic loading.
 */

namespace nebula {

class FunctionManager final {
public:
    using Function = std::function<StatusOr<VariantType>(const std::vector<VariantType>&)>;

    /**
     * To obtain a function named `func', with the actual arity.
     */
    static StatusOr<Function> get(const std::string &func, size_t arity);

    /**
     * To load a set of functions from a shared object dynamically.
     */
    static Status load(const std::string &soname, const std::vector<std::string> &funcs);

    /**
     * To unload a shared object.
     */
    static Status unload(const std::string &soname, const std::vector<std::string> &funcs);

private:
    /**
     * FunctionManager functions as a singleton, since the dynamic loading is process-wide.
     */
    FunctionManager();

    static FunctionManager& instance();

    StatusOr<Function> getInternal(const std::string &func, size_t arity) const;

    Status loadInternal(const std::string &soname, const std::vector<std::string> &funcs);

    Status unloadInternal(const std::string &soname, const std::vector<std::string> &funcs);

    struct FunctionAttributes final {
        size_t                  minArity_{0};
        size_t                  maxArity_{0};
        Function                body_;
    };

    mutable folly::RWSpinLock                           lock_;
    std::unordered_map<std::string, FunctionAttributes> functions_;
};

}   // namespace nebula

#endif  // COMMON_FILTER_FUNCTIONMANAGER_H_
