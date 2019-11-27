/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_EXECUTOR_H_
#define GRAPH_EXECUTOR_H_

#include "base/Base.h"
#include "base/Status.h"
#include "cpp/helpers.h"
#include "graph/ExecutionContext.h"
#include "gen-cpp2/common_types.h"
#include "gen-cpp2/storage_types.h"
#include "dataman/RowWriter.h"
#include "meta/SchemaManager.h"


/**
 * Executor is the interface of kinds of specific executors that do the actual execution.
 */

namespace nebula {
namespace graph {

class Executor : public cpp::NonCopyable, public cpp::NonMovable {
public:
    explicit Executor(ExecutionContext *ectx) {
        ectx_ = ectx;
    }

    virtual ~Executor() {}

    /**
     * Do some preparatory works, such as sanitize checking, dependency setup, etc.
     *
     * `prepare' succeeds only if all its sub-executors are prepared.
     * `prepare' works in a synchronous way, once the executor is prepared, it will
     * be executed.
     */
    virtual Status MUST_USE_RESULT prepare() = 0;

    virtual void execute() = 0;

    virtual const char* name() const = 0;

    enum ProcessControl : uint8_t {
        kNext = 0,
        kReturn,
    };

    /**
     * Set callback to be invoked when this executor is finished(normally).
     */
    void setOnFinish(std::function<void(ProcessControl)> onFinish) {
        onFinish_ = onFinish;
    }
    /**
     * When some error happens during an executor's execution, it should invoke its
     * `onError_' with a Status that indicates the reason.
     *
     * An executor terminates its execution via invoking either `onFinish_' or `onError_',
     * but should never call them both.
     */
    void setOnError(std::function<void(Status)> onError) {
        onError_ = onError;
    }
    /**
     * Upon finished successfully, `setupResponse' would be invoked on the last executor.
     * Any Executor implementation, which wants to send its meaningful result to the client,
     * should override this method.
     */
    virtual void setupResponse(cpp2::ExecutionResponse &resp) {
        resp.set_error_code(cpp2::ErrorCode::SUCCEEDED);
    }

    ExecutionContext* ectx() const {
        return ectx_;
    }

protected:
    std::unique_ptr<Executor> makeExecutor(Sentence *sentence);

    std::string valueTypeToString(nebula::cpp2::ValueType type);

    void writeVariantType(RowWriter &writer, const VariantType &value);

    bool checkValueType(const nebula::cpp2::ValueType &type, const VariantType &value);

    StatusOr<int64_t> toTimestamp(const VariantType &value);

    StatusOr<cpp2::ColumnValue> toColumnValue(const VariantType& value,
                                              cpp2::ColumnValue::Type type) const;

    OptVariantType toVariantType(const cpp2::ColumnValue& value) const;

    Status checkIfGraphSpaceChosen() const {
        if (ectx()->rctx()->session()->space() == -1) {
            return Status::Error("Please choose a graph space with `USE spaceName' firstly");
        }
        return Status::OK();
    }

    StatusOr<VariantType> transformDefaultValue(nebula::cpp2::SupportedType type,
                                                std::string& originalValue);

protected:
    ExecutionContext                           *ectx_;
    std::function<void(ProcessControl)>         onFinish_;
    std::function<void(Status)>                 onError_;
};

}   // namespace graph
}   // namespace nebula

#endif  // GRAPH_EXECUTOR_H_
