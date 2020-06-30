/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_EXECUTIONCONTEXT_H_
#define CONTEXT_EXECUTIONCONTEXT_H_

#include "common/base/Base.h"
#include "common/datatypes/Value.h"
#include "context/Iterator.h"

namespace nebula {
namespace graph {
/***************************************************************************
 *
 * The context for each query request
 *
 * The context is NOT thread-safe. The execution plan has to guarantee
 * all accesses to context are safe
 *
 * The life span of the context is same as the request. That means a new
 * context object will be created as soon as the query engine receives the
 * query request. The context object will be visible to the parser, the
 * planner, the optimizer, and the executor.
 *
 **************************************************************************/
class State final {
public:
    enum class Stat : uint8_t {
        kUnExecuted,
        kPartialSuccess,
        kSuccess
    };

    State() = default;
    State(Stat stat, std::string msg) {
        stat_ = stat;
        msg_ = std::move(msg);
    }

    const Stat& stat() const {
        return stat_;
    }

private:
    Stat            stat_{Stat::kUnExecuted};
    std::string     msg_;
};


// An executor will produce a result.
class ExecResult final {
public:
    static const ExecResult kEmptyResult;
    static const std::vector<ExecResult> kEmptyResultList;

    static ExecResult buildDefault(Value&& val) {
        return ExecResult(std::move(val));
    }

    static ExecResult buildGetNeighbors(Value&& val, State&& stat) {
        ExecResult result(std::move(val), std::move(stat));
        auto iter = std::make_unique<GetNeighborsIter>(result.valuePtr());
        result.setIter(std::move(iter));
        return result;
    }

    static ExecResult buildSequential(Value&& val, State&& stat) {
        ExecResult result(std::move(val), std::move(stat));
        auto iter = std::make_unique<SequentialIter>(result.valuePtr());
        result.setIter(std::move(iter));
        return result;
    }

    void setIter(std::unique_ptr<Iterator> iter) {
        iter_ = std::move(iter);
    }

    std::shared_ptr<Value> valuePtr() const {
        return value_;
    }

    const Value& value() const {
        return *value_;
    }

    Value&& moveValue() {
        return std::move(*value_);
    }

    const State& state() const {
        return state_;
    }

    std::unique_ptr<Iterator> iter() const {
        return iter_->copy();
    }

private:
    ExecResult()
        : value_(std::make_shared<Value>()),
          state_(State::Stat::kUnExecuted, ""),
          iter_(std::make_unique<DefaultIter>(value_)) {}

    explicit ExecResult(Value&& val)
        : value_(std::make_shared<Value>(std::move(val))),
          state_(State::Stat::kSuccess, ""),
          iter_(std::make_unique<DefaultIter>(value_)) {}

    ExecResult(Value&& val, State stat)
        : value_(std::make_shared<Value>(std::move(val))), state_(stat) {}

private:
    std::shared_ptr<Value>          value_;
    State                           state_;
    std::unique_ptr<Iterator>       iter_;
};

class ExecutionContext {
public:
    ExecutionContext() = default;

    virtual ~ExecutionContext() = default;

    void initVar(const std::string& name) {
        valueMap_[name];
    }

    // Get the latest version of the value
    const Value& getValue(const std::string& name) const;

    Value moveValue(const std::string& name);

    const ExecResult& getResult(const std::string& name) const;

    size_t numVersions(const std::string& name) const;

    // Return all existing history of the value. The front is the latest value
    // and the back is the oldest value
    const std::vector<ExecResult>& getHistory(const std::string& name) const;

    void setValue(const std::string& name, Value&& val);

    void setResult(const std::string& name, ExecResult&& result);

    void deleteValue(const std::string& name);

    // Only keep the last several versoins of the Value
    void truncHistory(const std::string& name, size_t numVersionsToKeep);

    bool exist(const std::string& name) const {
        return valueMap_.find(name) != valueMap_.end();
    }

private:
    // name -> Value with multiple versions
    std::unordered_map<std::string, std::vector<ExecResult>>     valueMap_;
};
}  // namespace graph
}  // namespace nebula
#endif  // CONTEXT_EXECUTIONCONTEXT_H_
