/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_FUNCTION_AGGFUNCTIONMANAGER_H_
#define COMMON_FUNCTION_AGGFUNCTIONMANAGER_H_

#include "common/base/Base.h"
#include "common/base/StatusOr.h"
#include "common/base/Status.h"
#include "common/datatypes/Value.h"
/**
 * AggFunctionManager is for managing builtin and dynamic-loaded aggregate functions,
 * which users could use as AggregateExpression.
 *
 * TODO(dutor) To implement dynamic loading.
 */

namespace nebula {

class AggData final {
public:
    explicit AggData(Set* uniques = nullptr)
        : cnt_(0),
          sum_(0.0),
          avg_(0.0),
          deviation_(0.0),
          result_(Value::kNullValue) {
        if (uniques == nullptr) {
            uniques_ = std::make_unique<Set>();
        } else {
            uniques_.reset(uniques);
        }
    }

    const Value& cnt() const {
        return cnt_;
    }

    Value& cnt() {
        return cnt_;
    }

    void setCnt(Value&& cnt) {
        cnt_ = std::move(cnt);
    }

    const Value& sum() const {
        return sum_;
    }

    Value& sum() {
        return sum_;
    }

    void setSum(Value&& sum) {
        sum_ = std::move(sum);
    }

    const Value& avg() const {
        return avg_;
    }

    Value& avg() {
        return avg_;
    }

    void setAvg(Value&& avg) {
        avg_ = std::move(avg);
    }

    const Value& deviation() const {
        return deviation_;
    }

    Value& deviation() {
        return deviation_;
    }

    void setDeviation(Value&& deviation) {
        deviation_ = std::move(deviation);
    }

    const Value& result() const {
        return result_;
    }

    Value& result() {
        return result_;
    }

    void setResult(Value&& res) {
        result_ = std::move(res);
    }

    void setResult(const Value& res) {
        result_ = res;
    }

    const Set* uniques() const {
        return uniques_.get();
    }

    Set* uniques() {
        return uniques_.get();
    }

    void setUniques(Set* uniques) {
        uniques_.reset(uniques);
    }

private:
    Value cnt_;
    Value sum_;
    Value avg_;
    Value deviation_;
    Value result_;
    std::unique_ptr<Set>   uniques_;
};

class AggFunctionManager final {
public:
    using AggFunction = std::function<void(AggData*, const Value&)>;

    /**
     * To obtain a aggregate function named `func'
     */
    static StatusOr<AggFunction> get(const std::string &func);

    /**
     * To Check the validity of the function named `func'
     * Only used for parser check.
     */
    static Status find(const std::string &func);

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
     * AggFunctionManager functions as a singleton, since the dynamic loading is process-wide.
     */
    AggFunctionManager();

    static AggFunctionManager &instance();

    StatusOr<AggFunction> getInternal(std::string func) const;

    Status loadInternal(const std::string &soname, const std::vector<std::string> &funcs);

    Status unloadInternal(const std::string &soname, const std::vector<std::string> &funcs);

    std::unordered_map<std::string, AggFunction> functions_;
};

}   // namespace nebula

#endif  // COMMON_FUNCTION_AGGFUNCTIONMANAGER_H_
