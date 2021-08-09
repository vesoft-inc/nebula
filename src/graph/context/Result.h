/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_RESULT_H_
#define CONTEXT_RESULT_H_

#include <vector>

#include "context/Iterator.h"

namespace nebula {
namespace graph {

class ExecutionContext;
class ResultBuilder;

// An executor will produce a result.
class Result final {
public:
    enum class State : uint8_t {
        kUnExecuted,
        kPartialSuccess,
        kSuccess,
    };

    static const Result& EmptyResult();
    static const std::vector<Result>& EmptyResultList();

    std::shared_ptr<Value> valuePtr() const {
        return core_.value;
    }

    const Value& value() const {
        return *core_.value;
    }

    State state() const {
        return core_.state;
    }

    size_t size() const {
        return core_.iter->size();
    }

    std::unique_ptr<Iterator> iter() const & {
        return core_.iter->copy();
    }

    std::unique_ptr<Iterator> iter() && {
        return std::move(core_.iter);
    }

    Iterator* iterRef() {
        return core_.iter.get();
    }

private:
    friend class ResultBuilder;
    friend class ExecutionContext;

    Value&& moveValue() {
        return std::move(*core_.value);
    }

    struct Core {
        Core() = default;
        Core(Core &&) = default;
        Core& operator=(Core &&) = default;
        Core(const Core &c) {
            *this = c;
        }
        Core& operator=(const Core &c) {
            if (&c != this) {
                state = c.state;
                msg = c.msg;
                value = c.value;
                iter = c.iter->copy();
            }
            return *this;
        }

        State state;
        std::string msg;
        std::shared_ptr<Value> value;
        std::unique_ptr<Iterator> iter;
    };

    explicit Result(Core&& core) : core_(std::move(core)) {}

    Core core_;
};

// Default iterator type is sequential.
class ResultBuilder final {
public:
    ResultBuilder() {
        core_.state = Result::State::kSuccess;
    }

    Result finish() {
        if (!core_.iter) iter(Iterator::Kind::kSequential);
        if (!core_.value && core_.iter) value(core_.iter->valuePtr());
        return Result(std::move(core_));
    }

    ResultBuilder& value(Value&& value) {
        core_.value = std::make_shared<Value>(std::move(value));
        return *this;
    }

    ResultBuilder& value(std::shared_ptr<Value> value) {
        core_.value = value;
        return *this;
    }

    ResultBuilder& iter(std::unique_ptr<Iterator> iter) {
        core_.iter = std::move(iter);
        return *this;
    }

    ResultBuilder& iter(Iterator::Kind kind);

    ResultBuilder& state(Result::State state) {
        core_.state = state;
        return *this;
    }

    ResultBuilder& msg(std::string&& msg) {
        core_.msg = std::move(msg);
        return *this;
    }

private:
    Result::Core core_;
};

}   // namespace graph
}   // namespace nebula

#endif   // CONTEXT_RESULT_H_
