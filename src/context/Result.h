/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CONTEXT_RESULT_H_
#define CONTEXT_RESULT_H_

#include <memory>
#include <vector>

#include "context/Iterator.h"

namespace nebula {
namespace graph {

class ResultBuilder;

// An executor will produce a result.
class Result final {
public:
    static const Result kEmptyResult;
    static const std::vector<Result> kEmptyResultList;

    enum class State : uint8_t {
        kUnExecuted,
        kPartialSuccess,
        kSuccess,
    };

    std::shared_ptr<Value> valuePtr() const {
        return core_.value;
    }

    const Value& value() const {
        return *core_.value;
    }

    Value&& moveValue() {
        return std::move(*core_.value);
    }

    State state() const {
        return core_.state;
    }

    std::unique_ptr<Iterator> iter() const {
        return core_.iter->copy();
    }

private:
    friend class ResultBuilder;

    struct Core {
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

    ResultBuilder& iter(Iterator::Kind kind) {
        DCHECK(kind == Iterator::Kind::kDefault || core_.value)
            << "Must set value when creating non-default iterator";
        switch (kind) {
            case Iterator::Kind::kDefault:
                return iter(std::make_unique<DefaultIter>(core_.value));
            case Iterator::Kind::kSequential:
                return iter(std::make_unique<SequentialIter>(core_.value));
            case Iterator::Kind::kGetNeighbors:
                return iter(std::make_unique<GetNeighborsIter>(core_.value));
            default:
                LOG(FATAL) << "Invalid Iterator kind" << static_cast<uint8_t>(kind);
        }
    }

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
